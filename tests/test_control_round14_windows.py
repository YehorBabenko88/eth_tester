"""Security acceptance probes. True NT API paths run ONLY on Windows CI temp fixtures."""
import os
from pathlib import Path
import pytest
from eth_tester import windows_chart_handle as w



def _set_process_user_owner_on_windows_test_fixture(path):
    """Create a genuinely TokenUser-owned QA fixture; never touch runtime files.

    Elevated Windows runners can create files owned by their default
    TokenOwner (Administrators group), not their TokenUser SID. The runtime
    chart opener deliberately requires the stricter exact TokenUser SID.
    """
    assert os.name == 'nt'
    assert path.exists() and not path.is_symlink()
    import ctypes
    from ctypes import wintypes

    security = ctypes.WinDLL('advapi32', use_last_error=True)
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    handle = wintypes.HANDLE
    dword = wintypes.DWORD
    pointer = ctypes.c_void_p
    kernel.GetCurrentProcess.restype = handle
    kernel.CloseHandle.argtypes = [handle]
    kernel.CloseHandle.restype = wintypes.BOOL
    kernel.LocalFree.argtypes = [pointer]
    kernel.LocalFree.restype = pointer
    security.OpenProcessToken.argtypes = [handle, dword, ctypes.POINTER(handle)]
    security.OpenProcessToken.restype = wintypes.BOOL
    security.GetTokenInformation.argtypes = [
        handle, ctypes.c_int, pointer, dword, ctypes.POINTER(dword)]
    security.GetTokenInformation.restype = wintypes.BOOL
    security.SetNamedSecurityInfoW.argtypes = [
        wintypes.LPWSTR, dword, dword, pointer, pointer, pointer, pointer]
    security.SetNamedSecurityInfoW.restype = dword
    security.GetNamedSecurityInfoW.argtypes = [
        wintypes.LPWSTR, dword, dword, ctypes.POINTER(pointer),
        pointer, pointer, pointer, ctypes.POINTER(pointer)]
    security.GetNamedSecurityInfoW.restype = dword
    security.EqualSid.argtypes = [pointer, pointer]
    security.EqualSid.restype = wintypes.BOOL

    token = handle()
    assert security.OpenProcessToken(kernel.GetCurrentProcess(), 0x8,
                                     ctypes.byref(token)), (
        f'OpenProcessToken failed: winerror={ctypes.get_last_error()}')
    try:
        required = dword()
        security.GetTokenInformation(token, 1, None, 0, ctypes.byref(required))
        assert ctypes.sizeof(pointer) <= required.value <= 65536, (
            f'TokenUser length invalid: {required.value}')
        token_data = ctypes.create_string_buffer(required.value)
        assert security.GetTokenInformation(token, 1, token_data, required,
                                            ctypes.byref(required)), (
            f'GetTokenInformation failed: winerror={ctypes.get_last_error()}')
        sid = pointer(ctypes.cast(
            token_data, ctypes.POINTER(pointer)).contents.value)
        assert sid.value, 'TokenUser SID was null'
        rc = security.SetNamedSecurityInfoW(str(path), 1, 1, sid,
                                            None, None, None)
        assert rc == 0, f'SetNamedSecurityInfoW failed: status={rc}'
        actual_owner = pointer()
        descriptor = pointer()
        rc = security.GetNamedSecurityInfoW(
            str(path), 1, 1, ctypes.byref(actual_owner),
            None, None, None, ctypes.byref(descriptor))
        try:
            assert rc == 0, f'GetNamedSecurityInfoW failed: status={rc}'
            assert actual_owner.value, 'Temporary fixture owner SID was null'
            assert security.EqualSid(actual_owner, sid), (
                'Temporary fixture owner does not match TokenUser SID')
        finally:
            if descriptor.value:
                kernel.LocalFree(descriptor)
    finally:
        kernel.CloseHandle(token)


def test_windows_chart_lexical_drive_root_and_filename():
    drive, parts = w._chart_components(r'C:\TesterLab\runtime\charts', 'paper_trade_15_exit.png')
    assert drive == 'C:\\'
    assert parts == ('TesterLab', 'runtime', 'charts')


@pytest.mark.parametrize('root', [
    r'\\server\share\charts',
    r'\\?\C:\TesterLab\runtime\charts',
    r'C:relative\charts',
    r'\root_without_drive\charts',
    r'C:\owned\..\foreign',
    r'C:\owned\bad:stream\charts',
    r'C:\owned\trailing.\charts',
    r'C:\owned\trailing \charts',
    r'C:\owned\CON\charts',
    r'C:\owned\COM1.txt\charts',
])
def test_windows_rejects_unc_device_ads_ambiguous_names(root):
    with pytest.raises(FileNotFoundError):
        w._chart_components(root, 'paper_trade_1_entry.png')


@pytest.mark.parametrize('leaf', ['paper_trade_0_entry.png', 'paper_trade_7_FOO.png',
                                  'paper_trade_7_entry.png:secret', '..\\private.png',
                                  'paper_trade_7_entry.png.'])
def test_windows_rejects_unapproved_leaf(leaf):
    with pytest.raises(FileNotFoundError):
        w._chart_components(r'C:\owned\charts', leaf)


def test_native_windows_qa_gate_fails_closed_by_default(monkeypatch):
    monkeypatch.delenv('ETH_TESTER_WINDOWS_CHART_NATIVE_QA_APPROVED', raising=False)
    assert not w.windows_chart_qa_enabled()
    with pytest.raises(FileNotFoundError):
        w.open_chart_windows_native(r'C:\owned\charts', 'paper_trade_1_entry.png')


def test_native_windows_never_reopens_png_via_full_path():
    source = Path(w.__file__).read_text(encoding='utf8')
    assert 'OBJ_DONT_REPARSE' in source
    assert 'FILE_OPEN_REPARSE_POINT' in source
    assert 'FILE_ATTRIBUTE_REPARSE_POINT' in source
    assert 'dir_fd' not in source or 'NtCreateFile' in source
    # Accept the explicit HANDLE wrapper, but prove native CreateFileW
    # still opens only the drive root and never a mutable PNG path.
    import ast
    native_opens = [node for node in ast.walk(ast.parse(source))
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == 'CreateFileW']
    assert len(native_opens) == 1
    assert isinstance(native_opens[0].args[0], ast.Name)
    assert native_opens[0].args[0].id == 'drive'
    assert 'CreateFileW(str(root)' not in source
    assert 'NtCreateFile(ctypes.byref(out)' in source
    assert 'GetSecurityInfo(handle' in source
    assert 'EqualSid(owner' in source


@pytest.mark.skipif(os.name != 'nt', reason='native NTFS handle tests require Windows')
def test_native_windows_synthetic_temp_chart(tmp_path, monkeypatch):
    monkeypatch.setenv('ETH_TESTER_WINDOWS_CHART_NATIVE_QA_APPROVED','1')
    folder = tmp_path / 'owned_charts'
    folder.mkdir()
    _set_process_user_owner_on_windows_test_fixture(folder)
    leaf='paper_trade_35_exit.png'
    png=folder/leaf
    data=b'\x89PNG\r\n\x1a\n' + b'synthetic-owner-only'
    png.write_bytes(data)
    _set_process_user_owner_on_windows_test_fixture(png)
    with w.open_chart_windows_native(folder,leaf) as opened:
        assert opened.read() == data
    assert png.read_bytes() == data


@pytest.mark.skipif(os.name != 'nt', reason='native NTFS handle tests require Windows')
def test_native_windows_reparse_leaf_refused(tmp_path, monkeypatch):
    monkeypatch.setenv('ETH_TESTER_WINDOWS_CHART_NATIVE_QA_APPROVED','1')
    folder=tmp_path/'charts'
    folder.mkdir()
    _set_process_user_owner_on_windows_test_fixture(folder)
    foreign=tmp_path/'secret.png'
    foreign.write_bytes(b'\x89PNG\r\n\x1a\n' + b'foreign')
    alias=folder/'paper_trade_35_exit.png'
    try:
        alias.symlink_to(foreign)
    except (OSError, NotImplementedError):
        pytest.skip('Windows CI user cannot create file symbolic links')
    with pytest.raises(FileNotFoundError):
        w.open_chart_windows_native(folder, alias.name)


@pytest.mark.skipif(os.name != 'nt', reason='native NTFS handle tests require Windows')
def test_native_windows_reparse_parent_refused(tmp_path, monkeypatch):
    monkeypatch.setenv('ETH_TESTER_WINDOWS_CHART_NATIVE_QA_APPROVED','1')
    real=tmp_path/'actual'
    real.mkdir()
    (real/'paper_trade_35_exit.png').write_bytes(b'\x89PNG\r\n\x1a\n' + b'foreign')
    link=tmp_path/'charts'
    try:
        link.symlink_to(real,target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip('Windows CI user cannot create directory symbolic links')
    with pytest.raises(FileNotFoundError):
        w.open_chart_windows_native(link,'paper_trade_35_exit.png')


def test_trade_view_labels_unverified_funding_and_provisional_equity(monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from eth_tester import telegram_views as view
    observed=[]
    row = {key: None for key in ('id','symbol','state','direction','signal_ts',
        'opened_at','closed_at','entry','exit','qty','stop','take','gross_pnl','net_pnl',
        'fee_paid','funding_paid','slippage_paid','exit_reason','reason','funding_verified')}
    row.update(id=7,symbol='ETHUSDT',state='closed',direction='long',net_pnl='999',
               funding_paid='0', funding_verified=False, reason={})
    class Conn:
        def execute(self, query, params):
            observed.append((query, params))
            return SimpleNamespace(fetchone=lambda:row)
    @contextmanager
    def connect(_):yield Conn()
    monkeypatch.setattr(view,'connection',connect)
    message=view.detail_trade(SimpleNamespace(symbol='ETHUSDT'),7)
    assert observed[0][1] == ('ETHUSDT',7)
    assert 'funding_verified' in observed[0][0]
    assert 'funding_verified=False' in message
    assert 'предварительные' in message
    assert '999' in message and 'сертифицированный' in message


def test_funding_flag_true_never_turns_net_into_verified_profit(monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from eth_tester import telegram_views as view
    row={key:None for key in ('id','symbol','state','direction','signal_ts','opened_at',
       'closed_at','entry','exit','qty','stop','take','gross_pnl','net_pnl',
       'fee_paid','funding_paid','slippage_paid','exit_reason','reason')}
    row.update(id=8, funding_verified=True,reason={})
    class Conn:
        def execute(self,*a,**kw):return SimpleNamespace(fetchone=lambda:row)
    @contextmanager
    def connect(_):yield Conn()
    monkeypatch.setattr(view,'connection',connect)
    text=view.detail_trade(SimpleNamespace(symbol='ETHUSDT'),8)
    assert 'funding_verified=True' in text
    assert 'не сертифицированный результат' in text
