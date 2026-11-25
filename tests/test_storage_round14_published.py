"""Round14 Storage B: actual POSIX fd operations with synthetic files/tools only."""
import hashlib
from importlib.util import module_from_spec, spec_from_file_location
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.name != 'posix', reason='native POSIX FD boundary; Windows fails closed')
ROOT = Path(__file__).resolve().parents[1]


def load(name):
    filename = ROOT / 'src' / 'eth_tester' / 'storage' / name
    spec = spec_from_file_location('b14_' + name.replace('.', '_'), filename)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def backup(tmp_path, monkeypatch):
    helper = load('secure_backup.py')
    binary_dir = tmp_path / 'pgbin'
    binary_dir.mkdir(mode=0o700)
    for tool in ('pg_dump', 'pg_restore'):
        path = binary_dir / tool
        path.write_text('#!/bin/sh\nexit 0\n')
        path.chmod(0o700)
    monkeypatch.setenv('ETH_TESTER_PG_BIN_DIR', str(binary_dir))
    return helper, binary_dir


def test_requires_absolute_trusted_tool(tmp_path, monkeypatch):
    backup = load('secure_backup.py')
    monkeypatch.delenv('ETH_TESTER_PG_BIN_DIR', raising=False)
    with pytest.raises(RuntimeError, match='absolute trusted'):
        backup.trusted_tool('pg_restore')


def test_rejects_symlink_or_group_writable_binary(backup):
    helper, bin_dir = backup
    tool = bin_dir / 'pg_restore'
    tool.chmod(0o777)
    with pytest.raises(RuntimeError, match='Untrusted'):
        helper.trusted_tool('pg_restore')
    tool.unlink()
    tool.symlink_to('/bin/true')
    with pytest.raises(RuntimeError, match='Untrusted'):
        helper.trusted_tool('pg_restore')


def test_pinned_toc_survives_source_path_swap(backup, tmp_path, monkeypatch):
    helper, _ = backup
    archive = tmp_path / 'archive.dump'
    archive.write_bytes(b'original-owned')
    sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    def swap(args, **kw):
        assert args[1] == '--list'
        assert args[2].startswith('/proc/self/fd/')
        with open(args[2], 'rb') as opened:
            assert opened.read() == b'original-owned'
        archive.unlink()
        archive.write_bytes(b'foreign replacement')
        return SimpleNamespace(stdout='; dbname: eth_tester_ci\n99 TABLE public eth_storage_identity postgres\n')
    monkeypatch.setattr(helper.subprocess, 'run', swap)
    with helper.verified_archive(archive, expected_sha256=sha,
                                 expected_database='eth_tester_ci') as (_, _, alias, checksum):
        with open(alias, 'rb') as original:
            assert original.read() == b'original-owned'
        assert checksum == sha
    assert archive.read_bytes() == b'foreign replacement'


def test_invalid_checksum_blocks_tool_execution(backup, tmp_path, monkeypatch):
    helper, _ = backup
    archive = tmp_path / 'archive.dump'
    archive.write_bytes(b'not-approved')
    monkeypatch.setattr(helper.subprocess, 'run',
                        lambda *a, **kw: pytest.fail('tool should not run'))
    with pytest.raises(RuntimeError, match='checksum'):
        with helper.verified_archive(archive, expected_sha256='0'*64):
            pass


def test_dump_error_never_publishes_destination(backup, tmp_path, monkeypatch):
    helper, _ = backup
    target = tmp_path / 'destination.dump'
    def fail(args, **kw):
        kw['stdout'].write(b'incomplete')
        raise subprocess.CalledProcessError(1, args)
    monkeypatch.setattr(helper.subprocess, 'run', fail)
    with pytest.raises(subprocess.CalledProcessError):
        helper.dump_privately('synthetic', target, cli_dsn='no-secret', env={},
                              expected_database='eth_tester_ci')
    assert not target.exists()
    assert not list(tmp_path.glob('.eth-tester-backup-*.part'))


def test_dump_swap_does_not_unlink_foreign_inode(backup, tmp_path, monkeypatch):
    helper, _ = backup
    target = tmp_path / 'destination.dump'
    def swap(args, **kw):
        original = next(tmp_path.glob('.eth-tester-backup-*.part'))
        original.rename(tmp_path / 'original-owned')
        original.write_bytes(b'foreign-file')
        raise subprocess.CalledProcessError(1, args)
    monkeypatch.setattr(helper.subprocess, 'run', swap)
    with pytest.raises(subprocess.CalledProcessError):
        helper.dump_privately('synthetic', target, cli_dsn='no-secret', env={},
                              expected_database='eth_tester_ci')
    assert (tmp_path / 'original-owned').exists()
    assert next(tmp_path.glob('.eth-tester-backup-*.part')).read_bytes() == b'foreign-file'
    assert not target.exists()


def test_actual_subprocess_file_descriptor_roundtrip(backup, tmp_path):
    helper, tools = backup
    (tools/'pg_dump').write_text('#!/bin/sh\nprintf "fake-pg-archive"\n')
    (tools/'pg_restore').write_text('''#!/bin/sh
if [ "$1" = "--list" ]; then
  printf "; dbname: eth_tester_ci\\n99 TABLE public eth_storage_identity postgres\\n"
  exit 0
fi
for arg do case "$arg" in /proc/self/fd/*) cat "$arg" >/dev/null || exit 1 ;; esac; done
''')
    target = tmp_path / 'valid.dump'
    helper.dump_privately('synthetic', target, cli_dsn='no-secret',
                          env=os.environ.copy(), expected_database='eth_tester_ci')
    sha = hashlib.sha256(target.read_bytes()).hexdigest()
    with helper.verified_archive(target, expected_sha256=sha,
                                 expected_database='eth_tester_ci') as (binary, fd, alias, digest):
        subprocess.run([binary, '--dbname', 'synthetic', alias],
                       pass_fds=(fd,), check=True, timeout=3)
        assert digest == sha


def test_oversize_scan_aborts_before_any_delete(tmp_path):
    module = load('maintenance.py')
    root = tmp_path / 'owned'
    root.mkdir(mode=0o700)
    (root / '.eth-tester-owned-temp').write_text('eth_tester:temporary:v1\n')
    stage = root / ('eth-tester-stage-' + 'a'*32 + '.part')
    stage.write_text('owned')
    os.utime(stage, (10, 10))
    for i in range(10001):
        (root / f'padding-{i:05d}').touch()
    with pytest.raises(RuntimeError, match='no files deleted'):
        module.cleanup_owned_temp(root, now=10000)
    assert stage.read_text() == 'owned'


def test_preflight_file_identity_swap_is_skipped(tmp_path, monkeypatch):
    module = load('maintenance.py')
    root = tmp_path / 'owned'
    root.mkdir(mode=0o700)
    (root / '.eth-tester-owned-temp').write_text('eth_tester:temporary:v1\n')
    stage = root / ('eth-tester-stage-' + 'b'*32 + '.part')
    stage.write_text('old')
    os.utime(stage, (10, 10))
    stat_original = module.os.stat
    count = {'calls': 0}
    def swap(path, *a, **kw):
        if path == stage.name and 'dir_fd' in kw:
            count['calls'] += 1
            if count['calls'] == 2:
                stage.unlink()
                stage.write_text('new-foreign')
                os.utime(stage, (10, 10))
        return stat_original(path, *a, **kw)
    monkeypatch.setattr(module.os, 'stat', swap)
    report = module.cleanup_owned_temp(root, now=10000)
    assert stage.read_text() == 'new-foreign'
    assert report['deleted_owned_temp_files'] == 0


def test_staged_backup_inode_swap_before_publication_rejected(backup, tmp_path, monkeypatch):
    helper, _ = backup
    from contextlib import contextmanager
    target = tmp_path / 'must-not-exist.dump'
    displaced = tmp_path / 'original-stage'
    def dump(args, **kw):
        kw['stdout'].write(b'original dump')
        return SimpleNamespace(returncode=0)
    @contextmanager
    def fake_verified(path, **kw):
        stage = Path(path)
        stage.rename(displaced)
        stage.write_bytes(b'replacement file')
        yield ('fake', -1, 'fake', 'fake')
    monkeypatch.setattr(helper.subprocess, 'run', dump)
    monkeypatch.setattr(helper, 'verified_archive', fake_verified)
    with pytest.raises(RuntimeError, match='replaced before publication'):
        helper.dump_privately('synthetic', target, cli_dsn='no-secret', env={},
                              expected_database='eth_tester_ci')
    assert not target.exists()
    assert displaced.read_bytes() == b'original dump'
    assert next(tmp_path.glob('.eth-tester-backup-*.part')).read_bytes() == b'replacement file'
