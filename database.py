"""
RecirQ Global — Shipment Check Database Layer
SQLite persistence for batches, units, resolutions, and settings.
"""
import sqlite3
import json
import os
from datetime import datetime

# Use /data directory on Render (persistent disk), or local directory for dev
_data_dir = '/data' if os.path.isdir('/data') else os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_data_dir, 'shipment_check.db')


def get_db():
    """Get a database connection with row_factory set."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create all tables if they don't exist."""
    conn = get_db()
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS batches (
        id INTEGER PRIMARY KEY,
        vendor TEXT NOT NULL,
        ship_date TEXT NOT NULL,
        created_at TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        cleared_at TEXT,
        headers_json TEXT,
        route_results_json TEXT,
        aging_results_json TEXT,
        qty_results_json TEXT,
        imei_results_json TEXT,
        imei_match_results_json TEXT,
        submitted_imeis_json TEXT,
        submitted_files_json TEXT,
        submitted_file_info_json TEXT,
        packing_slips_json TEXT,
        delivery_by_rma_json TEXT,
        packing_slip_files_json TEXT,
        route_fail_count INTEGER DEFAULT 0,
        aging_fail_count INTEGER DEFAULT 0,
        qty_mismatch_count INTEGER DEFAULT 0,
        imei_mismatch_count INTEGER DEFAULT 0,
        hard_stop_count INTEGER DEFAULT 0,
        unpacked_count INTEGER DEFAULT 0
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS units (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER NOT NULL,
        unit_index INTEGER NOT NULL,
        route TEXT,
        sku TEXT,
        imei TEXT,
        rma_number TEXT,
        submission_date TEXT,
        tracking_out TEXT,
        route_status TEXT,
        aging_status TEXT,
        days_since_submission INTEGER,
        hard_stop INTEGER DEFAULT 0,
        hard_stop_reason TEXT,
        fallout_reason TEXT,
        fallout_notes TEXT,
        imei_mismatch INTEGER DEFAULT 0,
        imei_mismatch_reason TEXT,
        imei_match INTEGER DEFAULT 0,
        imei_resolved TEXT,
        route_corrected INTEGER DEFAULT 0,
        original_route TEXT,
        route_correction_notes TEXT,
        aging_cleared INTEGER DEFAULT 0,
        aging_cleared_notes TEXT,
        removed_from_shipment INTEGER DEFAULT 0,
        raw_json TEXT,
        FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS imei_resolutions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER NOT NULL,
        rma_or_tracking TEXT NOT NULL,
        imei TEXT NOT NULL,
        direction TEXT NOT NULL,
        reason TEXT NOT NULL,
        resolved_at TEXT NOT NULL,
        FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
        UNIQUE(batch_id, rma_or_tracking, imei, direction)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS unpacked_fallouts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER NOT NULL,
        imei TEXT NOT NULL,
        reason TEXT NOT NULL,
        FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
        UNIQUE(batch_id, imei)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS recovered_imeis (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER NOT NULL,
        imei TEXT NOT NULL,
        rma_number TEXT,
        tracking_out TEXT,
        added_manually INTEGER DEFAULT 1,
        raw_json TEXT,
        FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')

    conn.commit()
    conn.close()


def save_batch(batch_data):
    """Save a full batch (with units) to the database. Returns the batch id."""
    conn = get_db()
    c = conn.cursor()
    bid = batch_data['id']

    c.execute('''INSERT OR REPLACE INTO batches
        (id, vendor, ship_date, created_at, status, cleared_at,
         headers_json, route_results_json, aging_results_json,
         qty_results_json, imei_results_json, imei_match_results_json,
         submitted_imeis_json, submitted_files_json, submitted_file_info_json,
         packing_slips_json, delivery_by_rma_json, packing_slip_files_json,
         route_fail_count, aging_fail_count, qty_mismatch_count,
         imei_mismatch_count, hard_stop_count, unpacked_count)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (bid, batch_data['vendor'], batch_data['shipDate'],
         batch_data['createdAt'], batch_data['status'],
         batch_data.get('clearedAt'),
         json.dumps(batch_data.get('_headers', batch_data.get('_vzHeaders', batch_data.get('_bpHeaders')))),
         json.dumps(batch_data.get('routeResults', [])),
         json.dumps(batch_data.get('agingResults', [])),
         json.dumps(batch_data.get('qtyResults', [])),
         json.dumps(batch_data.get('imeiResults', {})),
         json.dumps(batch_data.get('imeiMatchResults', [])),
         json.dumps(batch_data.get('submittedIMEIs', [])),
         json.dumps(batch_data.get('submittedFiles', [])),
         json.dumps(batch_data.get('submittedFileInfo', [])),
         json.dumps(batch_data.get('packingSlips', [])),
         json.dumps(batch_data.get('deliveryByRMA', {})),
         json.dumps(batch_data.get('packingSlipFiles', [])),
         batch_data.get('routeFailCount', 0),
         batch_data.get('agingFailCount', 0),
         batch_data.get('qtyMismatchCount', 0),
         batch_data.get('imeiMismatchCount', 0),
         batch_data.get('hardStopCount', 0),
         batch_data.get('unpackedCount', 0)))

    c.execute('DELETE FROM units WHERE batch_id=?', (bid,))
    for u in batch_data.get('units', []):
        c.execute('''INSERT INTO units
            (batch_id, unit_index, route, sku, imei, rma_number,
             submission_date, tracking_out, route_status, aging_status,
             days_since_submission, hard_stop, hard_stop_reason,
             fallout_reason, fallout_notes, imei_mismatch, imei_mismatch_reason,
             imei_match, imei_resolved, route_corrected, original_route,
             route_correction_notes, aging_cleared, aging_cleared_notes,
             removed_from_shipment, raw_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (bid, u.get('index', 0), u.get('route'), u.get('sku'),
             u.get('imei'), u.get('rmaNumber'), u.get('submissionDate'),
             u.get('trackingOut'), u.get('routeStatus'), u.get('agingStatus'),
             u.get('daysSinceSubmission'), 1 if u.get('hardStop') else 0,
             u.get('hardStopReason'), u.get('falloutReason'),
             u.get('falloutNotes'), 1 if u.get('imeiMismatch') else 0,
             u.get('imeiMismatchReason'), 1 if u.get('imeiMatch') else 0,
             u.get('imeiResolved'), 1 if u.get('routeCorrected') else 0,
             u.get('originalRoute'), u.get('routeCorrectionNotes'),
             1 if u.get('agingCleared') else 0, u.get('agingClearedNotes'),
             1 if u.get('removedFromShipment') else 0,
             json.dumps(u.get('_raw', {}))))

    c.execute('DELETE FROM imei_resolutions WHERE batch_id=?', (bid,))
    for key, res in batch_data.get('imeiMismatchResolutions', {}).items():
        c.execute('''INSERT OR REPLACE INTO imei_resolutions
            (batch_id, rma_or_tracking, imei, direction, reason, resolved_at)
            VALUES (?,?,?,?,?,?)''',
            (bid, res['rmaOrTracking'], res['imei'], res['direction'],
             res['reason'], res['resolvedAt']))

    c.execute('DELETE FROM unpacked_fallouts WHERE batch_id=?', (bid,))
    for imei, reason in batch_data.get('unpackedFallouts', {}).items():
        c.execute('INSERT INTO unpacked_fallouts (batch_id, imei, reason) VALUES (?,?,?)',
                  (bid, imei, reason))

    c.execute('DELETE FROM recovered_imeis WHERE batch_id=?', (bid,))
    for r in batch_data.get('recoveredIMEIs', []):
        c.execute('''INSERT INTO recovered_imeis
            (batch_id, imei, rma_number, tracking_out, added_manually, raw_json)
            VALUES (?,?,?,?,?,?)''',
            (bid, r['imei'], r.get('rmaNumber', ''), r.get('trackingOut', ''),
             1 if r.get('addedManually') else 0, json.dumps(r.get('_raw', {}))))

    conn.commit()
    conn.close()
    return bid


def load_batch(batch_id):
    """Load a full batch with units, resolutions, fallouts, and recovered IMEIs."""
    conn = get_db()
    c = conn.cursor()
    row = c.execute('SELECT * FROM batches WHERE id=?', (batch_id,)).fetchone()
    if not row:
        conn.close()
        return None
    batch = _row_to_batch(row)
    units = c.execute('SELECT * FROM units WHERE batch_id=? ORDER BY unit_index', (batch_id,)).fetchall()
    batch['units'] = [_row_to_unit(u) for u in units]
    resolutions = c.execute('SELECT * FROM imei_resolutions WHERE batch_id=?', (batch_id,)).fetchall()
    batch['imeiMismatchResolutions'] = {}
    for r in resolutions:
        key = f"{r['rma_or_tracking']}::{r['imei']}::{r['direction']}"
        batch['imeiMismatchResolutions'][key] = {
            'imei': r['imei'], 'rmaOrTracking': r['rma_or_tracking'],
            'direction': r['direction'], 'reason': r['reason'], 'resolvedAt': r['resolved_at'],
        }
    fallouts = c.execute('SELECT * FROM unpacked_fallouts WHERE batch_id=?', (batch_id,)).fetchall()
    batch['unpackedFallouts'] = {f['imei']: f['reason'] for f in fallouts}
    recovered = c.execute('SELECT * FROM recovered_imeis WHERE batch_id=?', (batch_id,)).fetchall()
    batch['recoveredIMEIs'] = [{
        'imei': r['imei'], 'rmaNumber': r['rma_number'], 'trackingOut': r['tracking_out'],
        'addedManually': bool(r['added_manually']),
        '_raw': json.loads(r['raw_json']) if r['raw_json'] else {},
    } for r in recovered]
    conn.close()
    return batch


def load_all_batches():
    conn = get_db()
    rows = conn.execute('SELECT id FROM batches ORDER BY created_at DESC').fetchall()
    conn.close()
    return [load_batch(r['id']) for r in rows]


def load_batches_by_vendor(vendor):
    conn = get_db()
    rows = conn.execute('SELECT id FROM batches WHERE vendor=? ORDER BY created_at DESC', (vendor,)).fetchall()
    conn.close()
    return [load_batch(r['id']) for r in rows]


def delete_batch(batch_id):
    conn = get_db()
    conn.execute('DELETE FROM batches WHERE id=?', (batch_id,))
    conn.commit()
    conn.close()


def update_batch_status(batch_id, status, cleared_at=None):
    conn = get_db()
    if cleared_at:
        conn.execute('UPDATE batches SET status=?, cleared_at=? WHERE id=?', (status, cleared_at, batch_id))
    else:
        conn.execute('UPDATE batches SET status=? WHERE id=?', (status, batch_id))
    conn.commit()
    conn.close()


def update_batch_counts(batch_id, counts):
    conn = get_db()
    fields = []
    values = []
    field_map = {
        'routeFailCount': 'route_fail_count', 'agingFailCount': 'aging_fail_count',
        'qtyMismatchCount': 'qty_mismatch_count', 'imeiMismatchCount': 'imei_mismatch_count',
        'hardStopCount': 'hard_stop_count', 'unpackedCount': 'unpacked_count',
    }
    for js_key, db_key in field_map.items():
        if js_key in counts:
            fields.append(f'{db_key}=?')
            values.append(counts[js_key])
    if fields:
        values.append(batch_id)
        conn.execute(f'UPDATE batches SET {",".join(fields)} WHERE id=?', values)
        conn.commit()
    conn.close()


def get_setting(key, default=None):
    conn = get_db()
    row = conn.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
    conn.close()
    return row['value'] if row else default


def set_setting(key, value):
    conn = get_db()
    conn.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)', (key, value))
    conn.commit()
    conn.close()


def get_all_settings():
    conn = get_db()
    rows = conn.execute('SELECT key, value FROM settings').fetchall()
    conn.close()
    return {r['key']: r['value'] for r in rows}


def _row_to_batch(row):
    vendor = row['vendor']
    headers = json.loads(row['headers_json']) if row['headers_json'] else []
    header_key = '_vzHeaders' if vendor == 'VERIZON' else '_bpHeaders'
    return {
        'id': row['id'], 'vendor': vendor, 'shipDate': row['ship_date'],
        'createdAt': row['created_at'], 'status': row['status'], 'clearedAt': row['cleared_at'],
        header_key: headers,
        'routeResults': json.loads(row['route_results_json'] or '[]'),
        'agingResults': json.loads(row['aging_results_json'] or '[]'),
        'qtyResults': json.loads(row['qty_results_json'] or '[]'),
        'imeiResults': json.loads(row['imei_results_json'] or '{}'),
        'imeiMatchResults': json.loads(row['imei_match_results_json'] or '[]'),
        'submittedIMEIs': json.loads(row['submitted_imeis_json'] or '[]'),
        'submittedFiles': json.loads(row['submitted_files_json'] or '[]'),
        'submittedFileInfo': json.loads(row['submitted_file_info_json'] or '[]'),
        'packingSlips': json.loads(row['packing_slips_json'] or '[]'),
        'deliveryByRMA': json.loads(row['delivery_by_rma_json'] or '{}'),
        'packingSlipFiles': json.loads(row['packing_slip_files_json'] or '[]'),
        'routeFailCount': row['route_fail_count'], 'agingFailCount': row['aging_fail_count'],
        'qtyMismatchCount': row['qty_mismatch_count'], 'imeiMismatchCount': row['imei_mismatch_count'],
        'hardStopCount': row['hard_stop_count'], 'unpackedCount': row['unpacked_count'],
        '_viewing': False,
    }


def _row_to_unit(row):
    return {
        'index': row['unit_index'], 'route': row['route'], 'sku': row['sku'],
        'imei': row['imei'], 'rmaNumber': row['rma_number'],
        'submissionDate': row['submission_date'], 'trackingOut': row['tracking_out'],
        'routeStatus': row['route_status'], 'agingStatus': row['aging_status'],
        'daysSinceSubmission': row['days_since_submission'],
        'hardStop': bool(row['hard_stop']), 'hardStopReason': row['hard_stop_reason'],
        'falloutReason': row['fallout_reason'], 'falloutNotes': row['fallout_notes'],
        'imeiMismatch': bool(row['imei_mismatch']), 'imeiMismatchReason': row['imei_mismatch_reason'],
        'imeiMatch': bool(row['imei_match']), 'imeiResolved': row['imei_resolved'],
        'routeCorrected': bool(row['route_corrected']), 'originalRoute': row['original_route'],
        'routeCorrectionNotes': row['route_correction_notes'],
        'agingCleared': bool(row['aging_cleared']), 'agingClearedNotes': row['aging_cleared_notes'],
        'removedFromShipment': bool(row['removed_from_shipment']),
        '_raw': json.loads(row['raw_json']) if row['raw_json'] else {},
    }


# Initialize on import
init_db()
