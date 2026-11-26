"""Storage B: fail-closed original SHA for A's same-gzip rehydration.

A owns downloader/retries and calls this inside its staging transaction; B
only validates immutable original identity, never mutates the source ledger.
"""
from datetime import date
import re

_SHA = re.compile(r'[0-9a-f]{64}\Z')


def require_original_archive_sha(conn, symbol, day, downloaded_sha256, *,
                                 instrument='linear'):
    if (not isinstance(symbol, str) or not symbol or
            type(day) is not date or instrument not in ('linear','inverse','spot')
            or not isinstance(downloaded_sha256, str)
            or _SHA.fullmatch(downloaded_sha256) is None):
        raise ValueError('Invalid original archive identity')
    conn.execute("SELECT set_config('lock_timeout',%s,true)", ('2000ms',))
    conn.execute("SELECT set_config('statement_timeout',%s,true)", ('5000ms',))
    row = conn.execute('''SELECT state,sha256,row_count FROM ingestion_job
        WHERE source='trades_archive' AND symbol=%s AND day=%s FOR SHARE''',
        (symbol, day)).fetchone()
    if row is None or row['state'] != 'complete':
        raise RuntimeError('Original completed archive source not recorded')
    initial = row['sha256']
    if not isinstance(initial, str) or _SHA.fullmatch(initial) is None:
        raise RuntimeError('Original archive SHA missing/invalid; cannot rehydrate')
    if initial != downloaded_sha256:
        raise RuntimeError('Downloaded gzip SHA differs from original immutable job')
    if type(row['row_count']) is not int or row['row_count'] < 1:
        raise RuntimeError('Completed archive has no original row count')
    receipts = conn.execute('''SELECT archive_sha256,original_rows,price_bins
        FROM archive_stage_receipts WHERE symbol=%s AND instrument=%s
        AND archive_day=%s ORDER BY archive_sha256 FOR SHARE''',
        (symbol,instrument,day)).fetchall()
    for receipt in receipts:
        if receipt['archive_sha256'] != initial:
            raise RuntimeError('Conflicting same-day archive SHA membership')
        if receipt['original_rows'] != row['row_count']:
            raise RuntimeError('Original archive stage row count changed')
        bins = conn.execute('''SELECT count(*) AS total FROM archive_source_price_bins
            WHERE symbol=%s AND instrument=%s AND archive_day=%s
            AND archive_sha256=%s''',
            (symbol,instrument,day,initial)).fetchone()
        if not bins or bins['total'] != receipt['price_bins']:
            raise RuntimeError('Original archive membership bins incomplete')
    return {'original_sha256': initial, 'expected_rows': row['row_count'],
            'rehydration_needed': len(receipts) == 0,
            'source_quality': 'original_gzip_membership_only',
            'exchange_complete': False, 'ml_eligible': False}
