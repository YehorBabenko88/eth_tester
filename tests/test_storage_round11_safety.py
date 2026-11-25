"""Isolated regression tests for Storage B Round11 (no PostgreSQL / external IO)."""
import importlib.util
import os
from pathlib import Path
import pytest

MODULE = Path(__file__).resolve().parents[1]/'src/eth_tester/storage/maintenance.py'
spec = importlib.util.spec_from_file_location('eth_round11_maintenance', MODULE)
maintenance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(maintenance)
MARKER = '.eth-tester-owned-temp'
NAME = 'eth-tester-stage-' + 'c'*32 + '.part'


def owned_dir(tmp_path):
    root = tmp_path/'owned'
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    (root/MARKER).write_text('eth_tester:temporary:v1\n')
    return root


def aged(path):
    path.write_text('synthetic')
    os.utime(path, (10, 10))


def test_symlink_root_is_rejected(tmp_path):
    root = owned_dir(tmp_path)
    link = tmp_path/'link'
    link.symlink_to(root, target_is_directory=True)
    if os.name != 'posix':
        pytest.skip('POSIX file descriptor behavior')
    with pytest.raises(OSError):
        maintenance.cleanup_owned_temp(link, now=10_000)


def test_root_swap_pins_directory_does_not_delete_outsider(tmp_path, monkeypatch):
    if os.name != 'posix':
        pytest.skip('POSIX file descriptor behavior')
    root=owned_dir(tmp_path)
    aged(root/NAME)
    outsider = tmp_path/'outsider'
    outsider.mkdir()
    aged(outsider/NAME)
    orig_scandir = os.scandir
    swaps=[]
    def swap(fd):
        if isinstance(fd,int) and not swaps:
            root.rename(tmp_path/'moved')
            root.symlink_to(outsider, target_is_directory=True)
            swaps.append(1)
        return orig_scandir(fd)
    monkeypatch.setattr(os,'scandir',swap)
    outcome=maintenance.cleanup_owned_temp(root,now=10000)
    assert swaps
    assert outcome['deleted_owned_temp_files']==1
    assert (outsider/NAME).read_text()=='synthetic'
    assert not (tmp_path/'moved'/NAME).exists()


def test_foreign_marker_and_symlink_marker_rejected(tmp_path):
    if os.name != 'posix':
        pytest.skip('POSIX')
    root=owned_dir(tmp_path)
    aged(root/NAME)
    (root/MARKER).write_text('not ownership marker\n')
    with pytest.raises(RuntimeError,match='marker'):
        maintenance.cleanup_owned_temp(root,now=10000)
    marker = root/MARKER
    marker.unlink()
    ext = tmp_path/'external-marker'
    ext.write_text('eth_tester:temporary:v1\n')
    marker.symlink_to(ext)
    with pytest.raises(OSError):
        maintenance.cleanup_owned_temp(root,now=10000)
    assert (root/NAME).exists()


def test_world_writable_directory_rejected(tmp_path):
    if os.name != 'posix':
        pytest.skip('POSIX')
    root=owned_dir(tmp_path)
    aged(root/NAME)
    root.chmod(0o777)
    with pytest.raises(RuntimeError,match='permissions'):
        maintenance.cleanup_owned_temp(root,now=10000)
    assert (root/NAME).exists()


def test_file_symlink_hardlink_and_backup_survive(tmp_path):
    if os.name != 'posix':
        pytest.skip('POSIX')
    root=owned_dir(tmp_path)
    outside=tmp_path/'outside'
    aged(outside)
    link=root/NAME
    link.symlink_to(outside)
    multi=root/('eth-tester-stage-'+'d'*32+'.part')
    aged(multi)
    hard=tmp_path/'hardlink'
    os.link(multi,hard)
    back=root/'archive.backup'
    aged(back)
    x=maintenance.cleanup_owned_temp(root,now=10000)
    assert x['deleted_owned_temp_files']==0
    assert link.is_symlink() and multi.exists() and hard.exists()
    assert outside.read_text()=='synthetic' and back.exists()


def test_young_file_and_regular_old_file(tmp_path):
    if os.name != 'posix':
        pytest.skip('POSIX')
    root=owned_dir(tmp_path)
    old=root/NAME
    aged(old)
    young=root/('eth-tester-stage-'+'e'*32+'.part')
    young.write_text('recent')
    os.utime(young,(9000,9000))
    outcome=maintenance.cleanup_owned_temp(root,now=10000)
    assert outcome['deleted_owned_temp_files']==1
    assert not old.exists() and young.exists()


def test_max_files_bounded(tmp_path):
    if os.name != 'posix':
        pytest.skip('POSIX')
    root=owned_dir(tmp_path)
    for i in range(5):
        aged(root/f'eth-tester-stage-{i:032x}.part')
    first=maintenance.cleanup_owned_temp(root,now=10000,max_files=2)
    assert first['deleted_owned_temp_files']==2 and first['bounded']
    second=maintenance.cleanup_owned_temp(root,now=10000,max_files=2)
    assert second['deleted_owned_temp_files']==2
    third=maintenance.cleanup_owned_temp(root,now=10000,max_files=2)
    assert third['deleted_owned_temp_files']==1


def test_unsupported_os_is_fail_closed(tmp_path, monkeypatch):
    root=owned_dir(tmp_path)
    aged(root/NAME)
    monkeypatch.setattr(maintenance.os,'name','nt')
    with pytest.raises(RuntimeError,match='atomic owner'):
        maintenance.cleanup_owned_temp(root,now=10000)
    assert (root/NAME).exists()


def test_directory_scan_is_limited_without_loading_all(tmp_path,monkeypatch):
    if os.name != 'posix':
        pytest.skip('POSIX')
    root=owned_dir(tmp_path)
    old=root/NAME
    aged(old)
    pattern=maintenance._TEMP_PATTERN
    monkeypatch.setattr(maintenance,'_TEMP_PATTERN',type('NoMatch',(),{'fullmatch':lambda self, val: None})())
    for i in range(10001):
        (root/f'not-staging-{i:05d}').touch()
    with pytest.raises(RuntimeError,match='scan budget'):
        maintenance.cleanup_owned_temp(root,now=10000)
    assert old.exists()
    monkeypatch.setattr(maintenance,'_TEMP_PATTERN',pattern)


def test_bad_arguments_fail_before_io(tmp_path):
    for opts in [{'max_files':True},{'max_files':0},{'max_files':1001},
                 {'older_than_seconds':5},{'now':True}]:
        with pytest.raises(ValueError):
            maintenance.cleanup_owned_temp(tmp_path,**opts)
