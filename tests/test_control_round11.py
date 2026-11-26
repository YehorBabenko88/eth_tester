"""Isolated G checkpoint regression suite: paper only, no PG/Bybit/network."""
import asyncio
import os
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import pytest

from eth_tester import service_health as health, supervisor, telegram_views as view


class FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []
    def execute(self, query, params=()):
        self.queries.append((query, params))
        result = self.rows.pop(0)
        return SimpleNamespace(fetchone=lambda: result)


def test_status_write_full_disk_does_not_kill_supervisor_task(tmp_path, monkeypatch):
    sup = supervisor.Supervisor(path=tmp_path)
    monkeypatch.setattr(Path, 'write_text', lambda *a, **k: (_ for _ in ()).throw(OSError('ENOSPC')))
    sup._record('engine', 'running', pid=123)
    assert sup.status['engine']['pid'] == 123


def test_analytics_fast_fresh_slow_stale_restarts_only_owned_analytics():
    class Sup:
        def __init__(self):
            self.children={'engine':SimpleNamespace(returncode=None), 'analyse':SimpleNamespace(returncode=None)}
            self.process_starts={'engine':0, 'analyse':0}
            self.terminated=[]
        def _record(self,*a,**kw): pass
        async def _terminate(self,name,proc):
            self.terminated.append(name)
            proc.returncode=1
    sup=Sup()
    check=health.HealthWatchdog(clock=lambda:501,checker=lambda _: {'paper_engine':1, 'live_paper_window':2, 'analytics':999})
    asyncio.run(check.check_once(sup,None))
    asyncio.run(check.check_once(sup,None))
    assert sup.terminated==['analyse']


def test_redaction_hides_credentials_in_key_names_too():
    text = view._json({'secret=TOPSECRET-CODE': {'password': 'PASSPHRASE-ABC'},
                      'diagnostic': 'Bearer AUTH-123'}, limit=3000)
    for word in ['TOPSECRET-CODE','PASSPHRASE-ABC','AUTH-123']:
        assert word not in text
    assert 'diagnostic' in text


def test_legacy_string_message_redaction_catches_dsn():
    val=view.sanitize_message('err postgresql://user:pw@host/db token=XYZ Bearer SECRET')
    for secret in ['user:pw','XYZ','SECRET']:
        assert secret not in val


def test_existing_chart_readonly_uses_persisted_filename_and_png_magic(tmp_path,monkeypatch):
    root=tmp_path/'owned'/'charts'
    root.mkdir(parents=True)
    fname=root/'paper_trade_7_exit.png'
    fname.write_bytes(b'\x89PNG\r\n\x1a\n' + b'PNG_ORIGINAL_CONTENT')
    mtime=fname.stat().st_mtime_ns
    database=FakeConn([{'id':7, 'closed_at': '2026-11-25T11:00', 'entry_chart':None,
                        'exit_chart':str(fname)}])
    @contextmanager
    def fake(_):
        yield database
    monkeypatch.setattr(view,'connection',fake)
    if os.name == 'nt':
        # Gate-off must refuse both the native file and the database lookup.
        with pytest.raises(FileNotFoundError, match='not QA approved'):
            view.existing_trade_chart(SimpleNamespace(symbol='ETHUSDT'),7,root=root)
        assert database.queries == []
    else:
        with view.existing_trade_chart(SimpleNamespace(symbol='ETHUSDT'),7,root=root) as chart:
            assert chart.read().endswith(b'PNG_ORIGINAL_CONTENT')
    assert fname.read_bytes().endswith(b'PNG_ORIGINAL_CONTENT')
    assert fname.stat().st_mtime_ns==mtime
    if os.name != 'nt':
        assert len(database.queries)==1 and database.queries[0][1]==(7,'ETHUSDT')
        assert 'UPDATE ' not in database.queries[0][0]


def test_forged_chart_database_path_is_rejected(tmp_path,monkeypatch):
    root=tmp_path/'owned'
    root.mkdir()
    path=root/'paper_trade_7_entry.png'
    path.write_bytes(b'\x89PNG\r\n\x1a\nvalid')
    conn=FakeConn([{'id':7,'closed_at':None,'entry_chart':str(tmp_path/'foreign.png'), 'exit_chart':None}])
    @contextmanager
    def connect(_):yield conn
    monkeypatch.setattr(view,'connection',connect)
    with pytest.raises(FileNotFoundError):
        view.existing_trade_chart(SimpleNamespace(symbol='ETHUSDT'),7,root=root)


def test_chart_symlink_file_is_rejected(tmp_path,monkeypatch):
    root=tmp_path/'owned'
    root.mkdir()
    foreign=tmp_path/'foreign.png'
    foreign.write_bytes(b'\x89PNG\r\n\x1a\nSECRET')
    path=root/'paper_trade_7_entry.png'
    try:
        path.symlink_to(foreign)
    except (NotImplementedError, OSError):
        pytest.skip('symlink unsupported')
    conn=FakeConn([{'id':7,'closed_at':None,'entry_chart':str(path),'exit_chart':None}])
    @contextmanager
    def connect(_):yield conn
    monkeypatch.setattr(view,'connection',connect)
    with pytest.raises(FileNotFoundError):
        view.existing_trade_chart(SimpleNamespace(symbol='ETHUSDT'),7,root=root)


def test_missing_chart_is_a_safe_failure(tmp_path,monkeypatch):
    root=tmp_path/'owned'
    root.mkdir()
    conn=FakeConn([{'id':7,'closed_at':None,'entry_chart':str(root/'paper_trade_7_entry.png'),'exit_chart':None}])
    @contextmanager
    def connect(_):yield conn
    monkeypatch.setattr(view,'connection',connect)
    with pytest.raises(FileNotFoundError):
        view.existing_trade_chart(SimpleNamespace(symbol='ETHUSDT'),7,root=root)


def test_chart_bad_header_rejected(tmp_path,monkeypatch):
    root=tmp_path/'charts'
    root.mkdir()
    fname=root/'paper_trade_7_entry.png'
    fname.write_bytes(b'NOTPNG\x00\x00')
    conn=FakeConn([{'id':7,'closed_at':None,'entry_chart':str(fname),'exit_chart':None}])
    @contextmanager
    def connect(_):yield conn
    monkeypatch.setattr(view,'connection',connect)
    with pytest.raises(FileNotFoundError):
        view.existing_trade_chart(SimpleNamespace(symbol='ETHUSDT'),7,root=root)


def test_no_manual_chart_dangerous_sql_in_new_view_module():
    code=Path(view.__file__).read_text(encoding='utf8')
    assert 'charts.render_trade' in code  # only in explanatory negative comment
    assert 'UPDATE paper_trades' not in code
    assert 'DROP ' not in code


def test_research_exposes_incomplete_cohort_without_promoting_ml(monkeypatch):
    class Query:
        def fetchone(self):
            return {'observed': 0, 'no_trade': 0, 'candidates': 0,
                    'unavailable': 0, 'first_minute': None,
                    'last_minute': None}
    class Conn:
        def execute(self,*a,**k):return Query()
    @contextmanager
    def fake(_): yield Conn()
    monkeypatch.setattr(view, 'connection', fake)
    value=view.read_view(SimpleNamespace(symbol='ETHUSDT'),'research')
    assert 'НЕ OOS' in value
    assert 'пропущено: 1440' in value
    assert 'НЕ являются no_signal' in value


def test_utf16_and_maintenance_retention_backward_compatibility():
    pages=view.pages('🚀'*5000,chunk_limit=3900,max_chunks=8)
    assert all(len(p.encode('utf-16-le'))//2<=3900 for p in pages)
    assert len(pages)>1
    assert 'DELETE ' not in Path(view.__file__).read_text(encoding='utf-8')


def test_cleanup_is_not_scheduled_even_with_old_override(monkeypatch, tmp_path):
    monkeypatch.setenv('ETH_TESTER_SAFE_CLEANUP_APPROVED', '1')
    monkeypatch.setattr(supervisor, 'verify_existing_schema', lambda _: {'verified': True})
    called=[]
    async def child(self, name, recurring_seconds=None):
        called.append(name)
        if name=='engine':
            raise asyncio.CancelledError()
        await asyncio.sleep(0)
    async def watcher(self):
        await asyncio.sleep(1000)
    async def watchdog(self, sup, settings):
        await asyncio.sleep(1000)
    monkeypatch.setattr(supervisor.Supervisor,'child',child)
    monkeypatch.setattr(supervisor.Supervisor,'_wait_shutdown',watcher)
    from eth_tester import service_health
    monkeypatch.setattr(service_health.HealthWatchdog,'run',watchdog)
    sup=supervisor.Supervisor(path=tmp_path)
    try:
        asyncio.run(sup.run(SimpleNamespace(tg_bot_token='',tg_chat_id='')))
    except asyncio.CancelledError:
        pass
    assert 'cleanup' not in called
    assert sup.status['cleanup']['state']=='disabled_unverified_retention'


def test_real_controller_dispatch_rejects_unauthorised_id_flood(monkeypatch):
    """Exercise actual Telegram controller with protocol-compatible offline stubs."""
    import sys
    import types
    fake_telegram = types.ModuleType('telegram')
    fake_ext = types.ModuleType('telegram.ext')
    fake_telegram.ReplyKeyboardMarkup = lambda *a, **kw: object()
    fake_telegram.Update = type('Update', (), {})
    fake_ext.Application = type('Application', (), {})
    fake_ext.CommandHandler = type('CommandHandler', (), {})
    fake_ext.MessageHandler = type('MessageHandler', (), {})
    fake_ext.ContextTypes = SimpleNamespace(DEFAULT_TYPE=object)
    fake_ext.filters = SimpleNamespace(TEXT=1, COMMAND=1)
    monkeypatch.setitem(sys.modules, 'telegram', fake_telegram)
    monkeypatch.setitem(sys.modules, 'telegram.ext', fake_ext)
    from eth_tester import telegram_control as tg
    sent=[]
    async def reply(text, **kwargs):sent.append(text)
    def update(uid):
        return SimpleNamespace(effective_chat=SimpleNamespace(id=100, type='private'),
            effective_user=SimpleNamespace(id=uid, is_bot=False),
            effective_message=SimpleNamespace(reply_text=reply))
    monkeypatch.setattr(tg, 'authorise', lambda settings, upd:
                        upd.effective_user.id == 42)
    monkeypatch.setattr(tg, '_audit', lambda *a, **kw: None)
    view._rate_times.clear()
    settings=SimpleNamespace(tg_chat_id='100')
    for uid in range(1000,2100):
        asyncio.run(tg.dispatch(settings, update(uid), SimpleNamespace(args=[]), 'help'))
    assert len(view._rate_times)==1
    asyncio.run(tg.dispatch(settings, update(42), SimpleNamespace(args=[]), 'help'))
    assert sent and 'ETH Tester' in sent[-1]
    view._rate_times.clear()


def test_real_controller_chart_command_never_renders_or_updates_database(monkeypatch):
    import sys
    import types
    if 'telegram' not in sys.modules:
        fake=types.ModuleType('telegram')
        fake.ReplyKeyboardMarkup=lambda *a,**kw: object()
        fake.Update=type('Update',(),{})
        ext=types.ModuleType('telegram.ext')
        for name in ('Application','CommandHandler','MessageHandler'):
            setattr(ext,name,type(name,(),{}))
        ext.ContextTypes=SimpleNamespace(DEFAULT_TYPE=object)
        ext.filters=SimpleNamespace(TEXT=1,COMMAND=1)
        monkeypatch.setitem(sys.modules,'telegram',fake)
        monkeypatch.setitem(sys.modules,'telegram.ext',ext)
    from eth_tester import telegram_control as tg
    monkeypatch.setattr(tg, 'authorise', lambda *a, **kw: True)
    monkeypatch.setattr(tg, '_audit', lambda *a, **kw: None)
    def unavailable(*a):raise FileNotFoundError('never generate')
    monkeypatch.setattr(view, 'existing_trade_chart', unavailable)
    sent=[]
    async def reply(text,**kw):sent.append(text)
    async def photo(**kw):raise AssertionError('missing chart must not be sent')
    u=SimpleNamespace(effective_user=SimpleNamespace(id=42),
                      effective_chat=SimpleNamespace(id=100),
                      effective_message=SimpleNamespace(reply_text=reply, reply_photo=photo))
    view._rate_times.clear()
    asyncio.run(tg.dispatch(SimpleNamespace(symbol='ETHUSDT'),u,
                            SimpleNamespace(args=['7']),'chart'))
    assert sent and 'не изменяет историю' in sent[0]
    view._rate_times.clear()
