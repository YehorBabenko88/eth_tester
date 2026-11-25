"""Exercise actual B Python source with isolated fake DB (no production PG)."""
import importlib.util
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType, SimpleNamespace
import sys
import pytest

ROOT = Path(__file__).resolve().parents[1]


class SQL(str):
    def format(self,*args):
        return SQL(str(self).format(*args))


def load_retention(monkeypatch):
    pkg=ModuleType('eth_tester')
    pkg.__path__=[str(ROOT/'src/eth_tester')]
    storage=ModuleType('eth_tester.storage')
    storage.__path__=[str(ROOT/'src/eth_tester/storage')]
    storage.check_ownership=lambda conn: conn.check_owner()
    db=ModuleType('eth_tester.database')
    db.connection=lambda settings: settings.connection()
    psy=ModuleType('psycopg')
    psy.errors=SimpleNamespace(InsufficientPrivilege=type('InsufficientPrivilege',(Exception,),{}))
    sql=ModuleType('psycopg.sql')
    sql.SQL=SQL
    sql.Identifier=lambda x:SQL('"'+x+'"')
    psy.sql=sql
    for name,obj in [('eth_tester',pkg),('eth_tester.storage',storage),
                     ('eth_tester.database',db),('psycopg',psy),('psycopg.sql',sql)]:
        monkeypatch.setitem(sys.modules,name,obj)
    spec=importlib.util.spec_from_file_location('eth_tester.retention',
                                                ROOT/'src/eth_tester/retention.py')
    result=importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules,'eth_tester.retention',result)
    spec.loader.exec_module(result)
    return result


class Conn:
    def __init__(self, lock=True):
        self.statements=[]
        self.lock=lock
    def check_owner(self):
        self.statements.append('OWNERSHIP_CHECK')
    def execute(self,statement,parameters=None):
        statement=str(statement)
        self.statements.append(statement)
        if 'pg_try_advisory_xact_lock' in statement:
            return SimpleNamespace(fetchone=lambda:{'acquired':self.lock})
        return SimpleNamespace(rowcount=1)


class Settings:
    def __init__(self, conn):self.conn=conn
    @contextmanager
    def connection(self):
        yield self.conn


def test_connection_rows_are_never_ttl_purged(monkeypatch):
    r=load_retention(monkeypatch)
    conn=Conn()
    r._purge_bounded(conn,'feed_health','ts',90,10)
    assert "topic <> 'connection'" in conn.statements[-1]
    assert 'FOR UPDATE SKIP LOCKED' in conn.statements[-1]
    assert 'DELETE FROM "feed_health"' in conn.statements[-1]


def test_research_review_and_protected_tag_never_ttl_purged(monkeypatch):
    r=load_retention(monkeypatch)
    conn=Conn()
    r._purge_bounded(conn,'bot_events','ts',180,10)
    command=conn.statements[-1]
    assert "event <> 'paper_trade_review_v1'" in command
    assert "data->>'retention'" in command


def test_ownership_check_runs_after_lock_and_statement_timeouts(monkeypatch):
    r=load_retention(monkeypatch)
    monkeypatch.delenv('ETH_TESTER_OWNED_TEMP_DIR',raising=False)
    conn=Conn()
    report=r.cleanup_diagnostics(Settings(conn),max_rows_per_table=3)
    assert 'set_config' in conn.statements[0]
    assert 'set_config' in conn.statements[1]
    assert conn.statements[2]=='OWNERSHIP_CHECK'
    assert report['status']=='ok' and not report['cleanup_complete']
    assert report['status_scope']=='batch_only'
    assert report['raw_market_data_deleted']==0


def test_same_lease_cannot_overlap(monkeypatch):
    r=load_retention(monkeypatch)
    monkeypatch.delenv('ETH_TESTER_OWNED_TEMP_DIR',raising=False)
    conn=Conn(lock=False)
    with pytest.raises(RuntimeError,match='Another'):
        r.cleanup_diagnostics(Settings(conn))
    assert not any('WITH doomed' in s for s in conn.statements)


def test_cleanup_never_supports_market_trade_tables(monkeypatch):
    r=load_retention(monkeypatch)
    with pytest.raises(ValueError):
        r._purge_bounded(Conn(),'paper_trades','closed_at',1,100)


def test_cleanup_disabled_temp_on_unset_env(monkeypatch):
    r=load_retention(monkeypatch)
    monkeypatch.delenv('ETH_TESTER_OWNED_TEMP_DIR',raising=False)
    a=r.cleanup_diagnostics(Settings(Conn()))
    assert a['owned_temp']['status']=='not_configured'


def test_recovery_timeout_precedes_owner_lock(monkeypatch):
    load_retention(monkeypatch)
    spec=importlib.util.spec_from_file_location('eth_tester.storage.recovery',
                          ROOT/'src/eth_tester/storage/recovery.py')
    module=importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules,'eth_tester.storage.recovery',module)
    spec.loader.exec_module(module)
    class RConn(Conn):
        def execute(self,s,p=None):
            self.statements.append(s)
            if 'pg_try_advisory' in s:
                return SimpleNamespace(fetchone=lambda:{'locked':True})
            return SimpleNamespace(rowcount=1)
    conn=RConn()
    x=module.recover_abandoned_ingestion(conn,workers_stopped=True,max_jobs=2)
    assert x['requeued']==1 and x['raw_rows_deleted']==0
    assert 'set_config' in conn.statements[0]
    assert 'set_config' in conn.statements[1]
    assert conn.statements[2]=='OWNERSHIP_CHECK'
    assert 'SKIP LOCKED' in next(s for s in conn.statements if 'WITH old_jobs' in s)


def test_recovery_requires_actual_boolean_stop(monkeypatch):
    load_retention(monkeypatch)
    spec=importlib.util.spec_from_file_location('eth_tester.storage.recovery',
                          ROOT/'src/eth_tester/storage/recovery.py')
    module=importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules,'eth_tester.storage.recovery',module)
    spec.loader.exec_module(module)
    with pytest.raises(RuntimeError, match='Stop all'):
        module.recover_abandoned_ingestion(Conn(),workers_stopped='true')
