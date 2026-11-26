"""Windows paper chart open by pinned native directory handles, never by mutable full path.

EXPERIMENTAL / QA-GATED: absent native Windows ACL/reparse acceptance this remains
fail-closed. No fallback to CreateFileW on the full PNG path is permissible.
"""

from __future__ import annotations

import os
import re
from pathlib import PureWindowsPath

_MAX_CHART_BYTES = 20 * 1024 * 1024
_PNG_HEADER = b'\x89PNG\r\n\x1a\n'


def _chart_components(root, leaf):
    """Accept only local drive root, unambiguous literal directories and fixed PNG."""
    path = PureWindowsPath(str(root))
    if (not path.is_absolute() or not re.fullmatch(r'[A-Za-z]:\\', path.anchor)
            or any(part in ('', '.', '..') for part in path.parts[1:])):
        raise FileNotFoundError('Windows chart path must be a plain local absolute drive path')
    segments = tuple(path.parts[1:])
    reserved = {'CON', 'PRN', 'AUX', 'NUL',
                *(f'COM{i}' for i in range(1, 10)),
                *(f'LPT{i}' for i in range(1, 10))}
    for part in segments:
        if (part.endswith((' ', '.')) or ':' in part or '/' in part or '\\' in part
                or any(ord(ch) < 32 for ch in part)
                or part.split('.', 1)[0].upper() in reserved):
            raise FileNotFoundError('Windows chart path component is ambiguous')
    if not segments or not re.fullmatch(r'paper_trade_[1-9][0-9]*_(entry|exit)\.png', leaf):
        raise FileNotFoundError('Windows chart identity not approved')
    return path.anchor, segments


def windows_chart_qa_enabled():
    return (os.name == 'nt' and
            os.environ.get('ETH_TESTER_WINDOWS_CHART_NATIVE_QA_APPROVED') == '1')


def open_chart_windows_native(root, leaf):
    """NT RootDirectory-relative, no-follow open, restricted-owner checks.

    Gate deliberately OFF by default. Enable only after QA validates native
    Windows reparse/junction/ACL behavior for the actual service account.
    Even with opt-in, a missing native primitive rejects instead of falling
    back to a pathname-based open. No paper trade state is ever modified.
    """
    if not windows_chart_qa_enabled():
        raise FileNotFoundError('Native Windows chart access needs QA acceptance')
    drive, segments = _chart_components(root, leaf)
    try:
        import ctypes
        import msvcrt
        from ctypes import wintypes
        ntdll = ctypes.WinDLL('ntdll', use_last_error=True)
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        security = ctypes.WinDLL('advapi32', use_last_error=True)
        HANDLE = wintypes.HANDLE
        LPVOID = ctypes.c_void_p
        ULONG = wintypes.DWORD
        class UnicodeString(ctypes.Structure):
            _fields_ = [('Length', wintypes.USHORT), ('MaximumLength', wintypes.USHORT),
                        ('Buffer', wintypes.LPWSTR)]
        class ObjectAttributes(ctypes.Structure):
            _fields_ = [('Length', ULONG), ('RootDirectory', HANDLE),
                        ('ObjectName', ctypes.POINTER(UnicodeString)),
                        ('Attributes', ULONG), ('SecurityDescriptor', LPVOID),
                        ('SecurityQualityOfService', LPVOID)]
        class IoStatusBlock(ctypes.Structure):
            _fields_ = [('Status', LPVOID), ('Information', ctypes.c_size_t)]
        class AttributeTagInfo(ctypes.Structure):
            _fields_ = [('FileAttributes', ULONG), ('ReparseTag', ULONG)]
        kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, ULONG, ULONG, LPVOID,
                                       ULONG, ULONG, HANDLE]
        kernel.CreateFileW.restype = HANDLE
        kernel.CloseHandle.argtypes = [HANDLE]
        kernel.CloseHandle.restype = wintypes.BOOL
        kernel.GetFileInformationByHandleEx.argtypes = [HANDLE, ctypes.c_int,
                                                        LPVOID, ULONG]
        kernel.GetFileInformationByHandleEx.restype = wintypes.BOOL
        kernel.GetCurrentProcess.restype = HANDLE
        security.OpenProcessToken.argtypes = [HANDLE, ULONG, ctypes.POINTER(HANDLE)]
        security.OpenProcessToken.restype = wintypes.BOOL
        security.GetTokenInformation.argtypes = [HANDLE, ctypes.c_int, LPVOID,
                                                 ULONG, ctypes.POINTER(ULONG)]
        security.GetTokenInformation.restype = wintypes.BOOL
        security.GetSecurityInfo.argtypes = [HANDLE, ULONG, ULONG,
            ctypes.POINTER(LPVOID), LPVOID, LPVOID, LPVOID, ctypes.POINTER(LPVOID)]
        security.GetSecurityInfo.restype = ULONG
        security.EqualSid.argtypes = [LPVOID, LPVOID]
        security.EqualSid.restype = wintypes.BOOL
        kernel.LocalFree.argtypes = [LPVOID]
        kernel.LocalFree.restype = LPVOID
        ntdll.NtCreateFile.argtypes = [ctypes.POINTER(HANDLE), ULONG,
            ctypes.POINTER(ObjectAttributes), ctypes.POINTER(IoStatusBlock),
            LPVOID, ULONG, ULONG, ULONG, ULONG, LPVOID, ULONG]
        ntdll.NtCreateFile.restype = ctypes.c_long
        FILE_READ_DATA = 0x1
        FILE_READ_ATTRIBUTES = 0x80
        READ_CONTROL = 0x20000
        SYNCHRONIZE = 0x100000
        FILE_SHARE_ALL = 0x7
        OPEN_EXISTING = 3
        FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
        FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
        FILE_ATTRIBUTE_DIRECTORY = 0x10
        FILE_ATTRIBUTE_REPARSE_POINT = 0x400
        FILE_OPEN = 0x1
        FILE_DIRECTORY_FILE = 0x1
        FILE_NON_DIRECTORY_FILE = 0x40
        FILE_SYNCHRONOUS_IO_NONALERT = 0x20
        FILE_OPEN_REPARSE_POINT = 0x200000
        OBJ_CASE_INSENSITIVE = 0x40
        OBJ_DONT_REPARSE = 0x1000
        FILE_ATTRIBUTE_TAG_INFO = 9
        INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

        def _attributes(handle, *, directory):
            info = AttributeTagInfo()
            if not kernel.GetFileInformationByHandleEx(handle, FILE_ATTRIBUTE_TAG_INFO,
                                                        ctypes.byref(info), ctypes.sizeof(info)):
                raise FileNotFoundError('Cannot verify native Windows chart attributes')
            if ((info.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) or
                    bool(info.FileAttributes & FILE_ATTRIBUTE_DIRECTORY) != directory):
                raise FileNotFoundError('Windows chart path contains a reparse point')

        def _child(parent, name, directory):
            # SINGLE component relative to pinned HANDLE; never reparse its parents.
            buffer = ctypes.create_unicode_buffer(name)
            encoded = name.encode('utf-16-le')
            # Keep the UTF-16 buffer alive for the duration of NtCreateFile.
            us = UnicodeString(len(encoded), ctypes.sizeof(buffer),
                               ctypes.cast(buffer, wintypes.LPWSTR))
            attributes = ObjectAttributes(ctypes.sizeof(ObjectAttributes), parent,
                        ctypes.pointer(us), OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
                        None, None)
            out = HANDLE()
            iosb = IoStatusBlock()
            opts = (FILE_SYNCHRONOUS_IO_NONALERT | FILE_OPEN_REPARSE_POINT |
                    (FILE_DIRECTORY_FILE if directory else FILE_NON_DIRECTORY_FILE))
            access = (FILE_READ_ATTRIBUTES | READ_CONTROL | SYNCHRONIZE |
                      (0 if directory else FILE_READ_DATA))
            status = ntdll.NtCreateFile(ctypes.byref(out), access,
                    ctypes.byref(attributes), ctypes.byref(iosb), None, 0,
                    FILE_SHARE_ALL, FILE_OPEN, opts, None, 0)
            if status < 0 or not out.value:
                raise FileNotFoundError('Native Windows relative chart opening denied')
            try:
                _attributes(out, directory=directory)
                return out
            except BaseException:
                kernel.CloseHandle(out)
                raise

        def _is_service_owned(handle):
            token = HANDLE()
            if not security.OpenProcessToken(kernel.GetCurrentProcess(), 0x8, ctypes.byref(token)):
                return False
            try:
                required = ULONG()
                security.GetTokenInformation(token, 1, None, 0, ctypes.byref(required))
                if required.value < ctypes.sizeof(LPVOID) or required.value > 65536:
                    return False
                value = ctypes.create_string_buffer(required.value)
                if not security.GetTokenInformation(token, 1, value, required,
                                                    ctypes.byref(required)):
                    return False
                user_sid = ctypes.cast(value, ctypes.POINTER(LPVOID)).contents.value
                owner = LPVOID()
                sd = LPVOID()
                rc = security.GetSecurityInfo(handle, 1, 1, ctypes.byref(owner),
                                              None, None, None, ctypes.byref(sd))
                try:
                    return bool(rc == 0 and owner.value and user_sid and
                                security.EqualSid(owner, LPVOID(user_sid)))
                finally:
                    if sd.value:
                        kernel.LocalFree(sd)
            finally:
                kernel.CloseHandle(token)

        # ctypes converts a HANDLE return value to Python int on Windows.
        # Wrap it before .value checks and native-handle comparison.
        root = HANDLE(kernel.CreateFileW(
            drive, FILE_READ_ATTRIBUTES | READ_CONTROL | SYNCHRONIZE,
            FILE_SHARE_ALL, None, OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, None))
        if not root or root.value == INVALID_HANDLE_VALUE:
            raise FileNotFoundError('Cannot pin Windows chart drive root')
        try:
            _attributes(root, directory=True)
            current = root
            try:
                for name in segments:
                    child = _child(current, name, directory=True)
                    if current != root:
                        kernel.CloseHandle(current)
                    current = child
                if not _is_service_owned(current):
                    raise FileNotFoundError('Windows chart folder ownership unverified')
                file_handle = _child(current, leaf, directory=False)
                try:
                    if not _is_service_owned(file_handle):
                        raise FileNotFoundError('Windows chart file ownership unverified')
                    descriptor = msvcrt.open_osfhandle(int(file_handle.value),
                                                      os.O_RDONLY | os.O_BINARY)
                    file_handle = None  # CRT owns it from this point on
                    try:
                        details = os.fstat(descriptor)
                        if not 8 <= details.st_size <= _MAX_CHART_BYTES:
                            raise FileNotFoundError('Windows chart size unverified')
                        if os.read(descriptor, 8) != _PNG_HEADER:
                            raise FileNotFoundError('Windows chart signature invalid')
                        os.lseek(descriptor, 0, os.SEEK_SET)
                        return os.fdopen(descriptor, 'rb')
                    except BaseException:
                        os.close(descriptor)
                        raise
                finally:
                    if file_handle is not None:
                        kernel.CloseHandle(file_handle)
            finally:
                if current and current.value != root.value:
                    kernel.CloseHandle(current)
        finally:
            kernel.CloseHandle(root)
    except (ImportError, AttributeError, OSError, ValueError, TypeError) as exc:
        raise FileNotFoundError('Native Windows chart verification failed') from exc
