"""Round13 G: actual source-level POSIX directory-FD chart-security acceptance.
No network, real PG, Windows access, service installation, or orders.
"""
import os
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_tester import telegram_views as v

HEADER = b'\x89PNG\r\n\x1a\n'


@pytest.fixture
def prepared_chart(tmp_path, monkeypatch):
    root = tmp_path / 'owned'
    root.mkdir()
    leaf = 'paper_trade_7_entry.png'
    original = HEADER + b'OWNER-ORIGINAL'
    chart = root / leaf
    chart.write_bytes(original)
    calls=[]
    class Connection:
        def execute(self, query, params=None):
            calls.append((str(query),params))
            return SimpleNamespace(fetchone=lambda: {'id':7,'closed_at':None,
                    'entry_chart':str(chart), 'exit_chart':None})
    @contextmanager
    def db(_):
        yield Connection()
    monkeypatch.setattr(v, 'connection', db)
    return SimpleNamespace(root=root, chart=chart, leaf=leaf, expected=original, calls=calls,
                           settings=SimpleNamespace(symbol='ETHUSDT'))


def test_chart_reads_only_persisted_png_without_db_writes(prepared_chart):
    c = prepared_chart
    with v.existing_trade_chart(c.settings, 7, root=c.root) as f:
        assert f.read() == c.expected
        assert f.closed is False
    assert all(q.strip().lower().startswith('select') for q, _ in c.calls)
    assert len(c.calls)==1 and c.calls[0][1] == (7,'ETHUSDT')


def test_parent_swap_AFTER_pinned_fd_keeps_original(prepared_chart, monkeypatch, tmp_path):
    c = prepared_chart
    foreign = tmp_path/'foreign'
    foreign.mkdir()
    (foreign/c.leaf).write_bytes(HEADER + b'ATTACKER-FILE')
    real_open=os.open
    switched=[]
    def replace_on_leaf(path, flags, *args, **kwargs):
        if path == c.leaf and kwargs.get('dir_fd') is not None and not switched:
            c.root.rename(tmp_path/'old-owned')
            c.root.symlink_to(foreign, target_is_directory=True)
            switched.append(True)
        return real_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(v.os,'open',replace_on_leaf)
    try:
        with v.existing_trade_chart(c.settings,7,root=c.root) as f:
            assert f.read() == c.expected
    finally:
        monkeypatch.setattr(v.os,'open',real_open)
    assert switched


def test_parent_swap_BEFORE_handle_pin_fails_closed(prepared_chart, monkeypatch, tmp_path):
    c = prepared_chart
    foreign = tmp_path/'foreign'
    foreign.mkdir()
    (foreign/c.leaf).write_bytes(HEADER + b'ATTACKER-FILE')
    real_open=os.open
    switched=[]
    def replace_before_root_part(path, flags, *args, **kwargs):
        if path == c.root.name and kwargs.get('dir_fd') is not None and not switched:
            c.root.rename(tmp_path/'old-owned')
            c.root.symlink_to(foreign, target_is_directory=True)
            switched.append(True)
        return real_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(v.os,'open',replace_before_root_part)
    try:
        with pytest.raises((OSError,FileNotFoundError)):
            v.existing_trade_chart(c.settings,7,root=c.root)
    finally:
        monkeypatch.setattr(v.os,'open',real_open)
    assert switched


def test_leaf_symlink_refused(prepared_chart,tmp_path):
    c=prepared_chart
    outsider=tmp_path/'private.png'
    outsider.write_bytes(HEADER+b'PRIVATE')
    c.chart.unlink()
    try:c.chart.symlink_to(outsider)
    except (OSError,NotImplementedError):pytest.skip('cannot create test symlinks')
    with pytest.raises((OSError, FileNotFoundError)):
        v.existing_trade_chart(c.settings,7,root=c.root)


def test_symlink_ancestor_refused(prepared_chart,tmp_path):
    c=prepared_chart
    c.root.rename(tmp_path/'moved')
    try:c.root.symlink_to(tmp_path/'moved',target_is_directory=True)
    except (OSError,NotImplementedError):pytest.skip('cannot create symlink')
    with pytest.raises((OSError,FileNotFoundError)):
        v.existing_trade_chart(c.settings,7,root=c.root)


def test_corrupt_png_refused(prepared_chart):
    c=prepared_chart
    c.chart.write_bytes(b'NOT_A_PNG')
    with pytest.raises(FileNotFoundError):
        v.existing_trade_chart(c.settings,7,root=c.root)


def test_huge_png_refused(prepared_chart):
    c=prepared_chart
    with c.chart.open('wb') as handle:
        handle.write(HEADER)
        handle.truncate(20*1024*1024+1)
    with pytest.raises(FileNotFoundError):
        v.existing_trade_chart(c.settings,7,root=c.root)


def test_unverified_windows_platform_fails_closed_before_db(prepared_chart,monkeypatch):
    c=prepared_chart
    monkeypatch.setattr(v,'_chart_dirfd_supported',lambda:False)
    with pytest.raises(FileNotFoundError):
        v.existing_trade_chart(c.settings,7,root=c.root)
    assert c.calls == []


def test_foreign_recorded_identity_rejected(prepared_chart,monkeypatch,tmp_path):
    c=prepared_chart
    class Conn:
        def execute(self,sql,params):
            return SimpleNamespace(fetchone=lambda:{'id':7,'closed_at':None,
                'entry_chart':str(tmp_path/'foreign.png'),'exit_chart':None})
    @contextmanager
    def db(_):yield Conn()
    monkeypatch.setattr(v,'connection',db)
    with pytest.raises(FileNotFoundError):
        v.existing_trade_chart(c.settings,7,root=c.root)


def test_old_connection_history_excluded_from_deletable_count(monkeypatch):
    observed=[]
    class Conn:
        def execute(self,sql,*a):
            observed.append(str(sql))
            return SimpleNamespace(fetchone=lambda:{'n':0})
    @contextmanager
    def db(_):yield Conn()
    monkeypatch.setattr(v,'connection',db)
    out=v.read_view(SimpleNamespace(symbol='ETHUSDT'),'maintenance')
    assert len(observed)==2
    assert "topic <> 'connection'" in observed[1]
    assert 'Автоматическая очистка отключена' in out
    assert 'Дистанционное удаление БД отключено' in out


def test_invalid_trade_id_never_accesses_database(prepared_chart):
    c=prepared_chart
    for trade_id in [-1,0,'7',True,2**63]:
        with pytest.raises(ValueError):
            v.existing_trade_chart(c.settings,trade_id,root=c.root)
    assert not c.calls


def test_no_cleanup_or_mutable_chart_update_in_views():
    code=Path(v.__file__).read_text()
    assert 'UPDATE paper_trades' not in code
    assert 'DELETE FROM' not in code
    assert 'DROP TABLE' not in code
