"""
RecirQ Global — Shipment Check Database Layer
PostgreSQL persistence (with SQLite fallback for local dev).
"""
import json
import os
from datetime import datetime

DATABASE_URL = os.environ.get('DATABASE_URL')

# ── Connection helpers ──

if DATABASE_URL:
    import psycopg2
    import psycopg2.extras

    def get_db():
        conn = psycopg2.connect(DATABASE_URL)
        return conn

    def _fetchone(cur):
        cols = [d[0] for d in cur.description] if cur.description else []
        row = cur.fetchone()
        return dict(zip(cols, row)) if row else None

    def _fetchall(cur):
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    _PH = '%s'  # PostgreSQL placeholder
else:
    import sqlite3

    def get_db():
        _data_dir = '/data' if os.path.isdir('/data') else os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(_data_dir, 'shipment_check.db')
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _fetchone(cur):
        row = cur.fetchone()
        return dict(row) if row else None

    def _fetchall(cur):
        return [dict(r) for r in cur.fetchall()]

    _PH = '?'  # SQLite placeholder


def _ph(n):
    """Return n placeholders joined by commas."""
    return ','.join([_PH] * n)


def init_db():
    """Create all tables if they don't exist."""
    conn = get_db()
    c = conn.cursor()

    # Use SERIAL for PostgreSQL, INTEGER PRIMARY KEY for SQLite auto-increment
    if DATABASE_URL:
        serial = 'SERIAL PRIMARY KEY'
        int_pk = 'BIGINT PRIMARY KEY'  # batches.id is user-supplied (Date.now() timestamp)
        batch_ref = 'BIGINT NOT NULL REFERENCES batches(id) ON DELETE CASCADE'
        bool_default = 'INTEGER DEFAULT 0'
        on_conflict = ''  # handled differently
    else:
        serial = 'INTEGER PRIMARY KEY AUTOINCREMENT'
        int_pk = 'INTEGER PRIMARY KEY'
        batch_ref = 'INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE'
        bool_default = 'INTEGER DEFAULT 0'

    c.execute(f'''CREATE TABLE IF NOT EXISTS batches (
        id {int_pk},
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
        unpacked_count INTEGER DEFAULT 0,
        submitted_imei_details_json TEXT
    )''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS units (
        id {serial},
        batch_id {batch_ref},
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
        hard_stop {bool_default},
        hard_stop_reason TEXT,
        fallout_reason TEXT,
        fallout_notes TEXT,
        imei_mismatch {bool_default},
        imei_mismatch_reason TEXT,
        imei_match {bool_default},
        imei_resolved TEXT,
        route_corrected {bool_default},
        original_route TEXT,
        route_correction_notes TEXT,
        aging_cleared {bool_default},
        aging_cleared_notes TEXT,
        removed_from_shipment {bool_default},
        raw_json TEXT
    )''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS imei_resolutions (
        id {serial},
        batch_id {batch_ref},
        rma_or_tracking TEXT NOT NULL,
        imei TEXT NOT NULL,
        direction TEXT NOT NULL,
        reason TEXT NOT NULL,
        resolved_at TEXT NOT NULL,
        UNIQUE(batch_id, rma_or_tracking, imei, direction)
    )''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS unpacked_fallouts (
        id {serial},
        batch_id {batch_ref},
        imei TEXT NOT NULL,
        reason TEXT NOT NULL,
        UNIQUE(batch_id, imei)
    )''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS recovered_imeis (
        id {serial},
        batch_id {batch_ref},
        imei TEXT NOT NULL,
        rma_number TEXT,
        tracking_out TEXT,
        added_manually INTEGER DEFAULT 1,
        raw_json TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS users (
        id {serial},
        username TEXT UNIQUE NOT NULL,
        display_name TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'user',
        created_at TEXT NOT NULL,
        is_active INTEGER DEFAULT 1
    )''')

    # Ensure role column exists for databases created before this migration
    if DATABASE_URL:
        # PostgreSQL: use savepoint so a failed ALTER doesn't abort the transaction
        c.execute("SAVEPOINT sp_role_migration")
        try:
            c.execute("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'")
        except Exception:
            c.execute("ROLLBACK TO SAVEPOINT sp_role_migration")
        c.execute("RELEASE SAVEPOINT sp_role_migration")
    else:
        try:
            c.execute("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'")
        except Exception:
            pass  # Column already exists

    # Migrate batches.id from INTEGER to BIGINT (Date.now() exceeds 32-bit INTEGER range)
    # Must drop FK constraints first, alter columns, then re-add FKs
    if DATABASE_URL:
        c.execute("SAVEPOINT sp_bigint_migration")
        try:
            # Check if migration is needed
            c.execute("""SELECT data_type FROM information_schema.columns
                         WHERE table_name='batches' AND column_name='id'""")
            col_type = c.fetchone()
            if col_type and col_type[0] == 'integer':
                # Drop FK constraints on child tables
                for tbl in ['units', 'imei_resolutions', 'unpacked_fallouts', 'recovered_imeis', 'activity_log']:
                    c.execute(f"""SELECT constraint_name FROM information_schema.table_constraints
                                 WHERE table_name='{tbl}' AND constraint_type='FOREIGN KEY'""")
                    for row in c.fetchall():
                        c.execute(f"ALTER TABLE {tbl} DROP CONSTRAINT {row[0]}")
                # Now alter all columns to BIGINT
                c.execute("ALTER TABLE batches ALTER COLUMN id TYPE BIGINT")
                c.execute("ALTER TABLE units ALTER COLUMN batch_id TYPE BIGINT")
                c.execute("ALTER TABLE imei_resolutions ALTER COLUMN batch_id TYPE BIGINT")
                c.execute("ALTER TABLE unpacked_fallouts ALTER COLUMN batch_id TYPE BIGINT")
                c.execute("ALTER TABLE recovered_imeis ALTER COLUMN batch_id TYPE BIGINT")
                c.execute("ALTER TABLE activity_log ALTER COLUMN batch_id TYPE BIGINT")
                # Re-add FK constraints
                for tbl in ['units', 'imei_resolutions', 'unpacked_fallouts', 'recovered_imeis']:
                    c.execute(f"ALTER TABLE {tbl} ADD CONSTRAINT {tbl}_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE")
                c.execute("ALTER TABLE activity_log ADD CONSTRAINT activity_log_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES batches(id)")
        except Exception as e:
            print(f"BIGINT migration error: {e}")
            c.execute("ROLLBACK TO SAVEPOINT sp_bigint_migration")
        c.execute("RELEASE SAVEPOINT sp_bigint_migration")

    # Ensure submitted_imei_details_json column exists for existing databases
    if DATABASE_URL:
        c.execute("SAVEPOINT sp_imei_details_migration")
        try:
            c.execute("ALTER TABLE batches ADD COLUMN submitted_imei_details_json TEXT")
        except Exception:
            c.execute("ROLLBACK TO SAVEPOINT sp_imei_details_migration")
        c.execute("RELEASE SAVEPOINT sp_imei_details_migration")
    else:
        try:
            c.execute("ALTER TABLE batches ADD COLUMN submitted_imei_details_json TEXT")
        except Exception:
            pass  # Column already exists

    # Ensure at least one admin exists — promote the first active user if none
    c.execute("SELECT COUNT(*) as cnt FROM users WHERE role='admin' AND is_active=1")
    admin_row = c.fetchone()
    admin_count = admin_row[0] if isinstance(admin_row, (list, tuple)) else (admin_row['cnt'] if admin_row else 0)
    if admin_count == 0:
        c.execute("SELECT id FROM users WHERE is_active=1 ORDER BY id ASC LIMIT 1")
        first = c.fetchone()
        if first:
            first_id = first[0] if isinstance(first, (list, tuple)) else first['id']
            c.execute(f"UPDATE users SET role='admin' WHERE id={_PH}", (first_id,))

    c.execute(f'''CREATE TABLE IF NOT EXISTS activity_log (
        id {serial},
        user_id INTEGER REFERENCES users(id),
        username TEXT NOT NULL,
        action TEXT NOT NULL,
        batch_id {'BIGINT' if DATABASE_URL else 'INTEGER'} REFERENCES batches(id),
        details TEXT,
        created_at TEXT NOT NULL
    )''')

    # Pick & Pack packing state table — stores all PP data as JSON
    c.execute('''CREATE TABLE IF NOT EXISTS pp_state (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )''')

    # ── Reebelo Reconciliation tables ──
    c.execute(f'''CREATE TABLE IF NOT EXISTS reebelo_runs (
        id {serial},
        timestamp TEXT NOT NULL,
        date TEXT NOT NULL,
        run_by TEXT DEFAULT '',
        total_pbi INTEGER DEFAULT 0,
        total_sheet INTEGER DEFAULT 0,
        total_common INTEGER DEFAULT 0,
        matched INTEGER DEFAULT 0,
        mismatches INTEGER DEFAULT 0,
        obr INTEGER DEFAULT 0,
        not_registered INTEGER DEFAULT 0,
        missing_sheet INTEGER DEFAULT 0,
        missing_pbi INTEGER DEFAULT 0,
        match_rate REAL DEFAULT 0.0,
        details_json TEXT DEFAULT '{{}}'
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS reebelo_config (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')

    conn.commit()
    conn.close()


# ════════════════════════════════════
# REEBELO RECONCILIATION
# ════════════════════════════════════

def save_reebelo_run(results, match_rate, run_by=""):
    """Save a Reebelo reconciliation run. Returns the run ID."""
    conn = get_db()
    c = conn.cursor()
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    dt = ts[:10]
    nr = results.get("notRegistered", [])

    detail = {
        "timestamp": ts, "date": dt,
        "totalPBI": results["totalPBI"], "totalSheet": results["totalSheet"],
        "totalCommon": results["totalCommon"], "matchRate": round(match_rate, 2),
        "matchCount": len(results["matches"]),
        "mismatches": results["mismatches"], "obrAlerts": results["obrAlerts"],
        "notRegistered": nr, "missingFromSheet": results["missingFromSheet"],
        "missingFromPBI": results["missingFromPBI"],
    }

    c.execute(f'''INSERT INTO reebelo_runs
        (timestamp, date, run_by, total_pbi, total_sheet, total_common,
         matched, mismatches, obr, not_registered, missing_sheet, missing_pbi,
         match_rate, details_json)
        VALUES ({_ph(14)})''',
        (ts, dt, run_by, results["totalPBI"], results["totalSheet"], results["totalCommon"],
         len(results["matches"]), len(results["mismatches"]),
         len(results["obrAlerts"]), len(nr),
         len(results["missingFromSheet"]), len(results["missingFromPBI"]),
         round(match_rate, 2), json.dumps(detail, default=str)))
    conn.commit()
    run_id = c.lastrowid
    conn.close()
    return run_id


def get_reebelo_trends(limit=100):
    """Get Reebelo trend data for chart and table."""
    conn = get_db()
    c = conn.cursor()
    c.execute(f'''SELECT id, timestamp, date, run_by, total_pbi, total_sheet,
                   matched, mismatches, obr, not_registered,
                   missing_sheet, missing_pbi, match_rate
              FROM reebelo_runs ORDER BY timestamp DESC LIMIT {_PH}''', (limit,))
    rows = _fetchall(c)
    conn.close()
    results = []
    for row in reversed(rows):
        row["time"] = row["timestamp"][11:] if len(row["timestamp"]) > 10 else ""
        row["pbi"] = row["total_pbi"]
        row["sheet"] = row["total_sheet"]
        results.append(row)
    return results


def get_reebelo_run_detail(run_id):
    """Get full detail JSON for a specific Reebelo run."""
    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT details_json FROM reebelo_runs WHERE id = {_PH}', (run_id,))
    row = _fetchone(c)
    conn.close()
    if row:
        return json.loads(row['details_json'])
    return None


def get_reebelo_config():
    """Get all Reebelo config values as a dict."""
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT key, value FROM reebelo_config')
    rows = _fetchall(c)
    conn.close()
    return {r['key']: r['value'] for r in rows}


def set_reebelo_config(key, value):
    """Set a Reebelo config value."""
    conn = get_db()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute(f'INSERT INTO reebelo_config (key, value) VALUES ({_ph(2)}) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value',
                  (key, value))
    else:
        c.execute(f'INSERT OR REPLACE INTO reebelo_config (key, value) VALUES ({_ph(2)})', (key, value))
    conn.commit()
    conn.close()


# ════════════════════════════════════
# PICK & PACK STATE
# ════════════════════════════════════

def save_pp_state(key, value_json):
    """Save a Pick & Pack state value (ppJobs or ppCompletedRMAs) as JSON."""
    import datetime
    conn = get_db()
    c = conn.cursor()
    now = datetime.datetime.utcnow().isoformat()
    if DATABASE_URL:
        c.execute('''INSERT INTO pp_state (key, value, updated_at)
                     VALUES (%s, %s, %s)
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at''',
                  (key, value_json, now))
    else:
        c.execute('''INSERT OR REPLACE INTO pp_state (key, value, updated_at)
                     VALUES (?, ?, ?)''', (key, value_json, now))
    conn.commit()
    conn.close()

def get_pp_state(key):
    """Get a Pick & Pack state value by key. Returns the JSON string or None."""
    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT value FROM pp_state WHERE key = {_PH}', (key,))
    row = c.fetchone()
    conn.close()
    if row:
        return row[0] if isinstance(row, (list, tuple)) else row['value']
    return None

def get_all_pp_state():
    """Get all Pick & Pack state as a dict of key: value_json."""
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT key, value FROM pp_state')
    rows = c.fetchall()
    conn.close()
    result = {}
    for row in rows:
        k = row[0] if isinstance(row, (list, tuple)) else row['key']
        v = row[1] if isinstance(row, (list, tuple)) else row['value']
        result[k] = v
    return result


# ════════════════════════════════════
# BATCH CRUD
# ════════════════════════════════════

def save_batch(batch_data):
    """Save a full batch (with units) to the database. Returns the batch id."""
    conn = get_db()
    c = conn.cursor()
    bid = batch_data['id']

    ph = _PH
    if DATABASE_URL:
        # PostgreSQL: upsert
        c.execute(f'''INSERT INTO batches
            (id, vendor, ship_date, created_at, status, cleared_at,
             headers_json, route_results_json, aging_results_json,
             qty_results_json, imei_results_json, imei_match_results_json,
             submitted_imeis_json, submitted_files_json, submitted_file_info_json,
             packing_slips_json, delivery_by_rma_json, packing_slip_files_json,
             route_fail_count, aging_fail_count, qty_mismatch_count,
             imei_mismatch_count, hard_stop_count, unpacked_count,
             submitted_imei_details_json)
            VALUES ({_ph(25)})
            ON CONFLICT (id) DO UPDATE SET
             vendor=EXCLUDED.vendor, ship_date=EXCLUDED.ship_date,
             created_at=EXCLUDED.created_at, status=EXCLUDED.status,
             cleared_at=EXCLUDED.cleared_at, headers_json=EXCLUDED.headers_json,
             route_results_json=EXCLUDED.route_results_json,
             aging_results_json=EXCLUDED.aging_results_json,
             qty_results_json=EXCLUDED.qty_results_json,
             imei_results_json=EXCLUDED.imei_results_json,
             imei_match_results_json=EXCLUDED.imei_match_results_json,
             submitted_imeis_json=EXCLUDED.submitted_imeis_json,
             submitted_files_json=EXCLUDED.submitted_files_json,
             submitted_file_info_json=EXCLUDED.submitted_file_info_json,
             packing_slips_json=EXCLUDED.packing_slips_json,
             delivery_by_rma_json=EXCLUDED.delivery_by_rma_json,
             packing_slip_files_json=EXCLUDED.packing_slip_files_json,
             route_fail_count=EXCLUDED.route_fail_count,
             aging_fail_count=EXCLUDED.aging_fail_count,
             qty_mismatch_count=EXCLUDED.qty_mismatch_count,
             imei_mismatch_count=EXCLUDED.imei_mismatch_count,
             hard_stop_count=EXCLUDED.hard_stop_count,
             unpacked_count=EXCLUDED.unpacked_count,
             submitted_imei_details_json=EXCLUDED.submitted_imei_details_json''',
            _batch_values(bid, batch_data))
    else:
        # SQLite: INSERT OR REPLACE
        c.execute(f'''INSERT OR REPLACE INTO batches
            (id, vendor, ship_date, created_at, status, cleared_at,
             headers_json, route_results_json, aging_results_json,
             qty_results_json, imei_results_json, imei_match_results_json,
             submitted_imeis_json, submitted_files_json, submitted_file_info_json,
             packing_slips_json, delivery_by_rma_json, packing_slip_files_json,
             route_fail_count, aging_fail_count, qty_mismatch_count,
             imei_mismatch_count, hard_stop_count, unpacked_count,
             submitted_imei_details_json)
            VALUES ({_ph(25)})''',
            _batch_values(bid, batch_data))

    # Delete existing units for this batch and re-insert
    c.execute(f'DELETE FROM units WHERE batch_id={ph}', (bid,))
    for u in batch_data.get('units', []):
        c.execute(f'''INSERT INTO units
            (batch_id, unit_index, route, sku, imei, rma_number,
             submission_date, tracking_out, route_status, aging_status,
             days_since_submission, hard_stop, hard_stop_reason,
             fallout_reason, fallout_notes, imei_mismatch, imei_mismatch_reason,
             imei_match, imei_resolved, route_corrected, original_route,
             route_correction_notes, aging_cleared, aging_cleared_notes,
             removed_from_shipment, raw_json)
            VALUES ({_ph(26)})''',
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

    # Save IMEI resolutions
    c.execute(f'DELETE FROM imei_resolutions WHERE batch_id={ph}', (bid,))
    for key, res in batch_data.get('imeiMismatchResolutions', {}).items():
        if DATABASE_URL:
            c.execute(f'''INSERT INTO imei_resolutions
                (batch_id, rma_or_tracking, imei, direction, reason, resolved_at)
                VALUES ({_ph(6)})
                ON CONFLICT (batch_id, rma_or_tracking, imei, direction) DO UPDATE SET
                reason=EXCLUDED.reason, resolved_at=EXCLUDED.resolved_at''',
                (bid, res['rmaOrTracking'], res['imei'], res['direction'],
                 res['reason'], res['resolvedAt']))
        else:
            c.execute(f'''INSERT OR REPLACE INTO imei_resolutions
                (batch_id, rma_or_tracking, imei, direction, reason, resolved_at)
                VALUES ({_ph(6)})''',
                (bid, res['rmaOrTracking'], res['imei'], res['direction'],
                 res['reason'], res['resolvedAt']))

    # Save unpacked fallouts
    c.execute(f'DELETE FROM unpacked_fallouts WHERE batch_id={ph}', (bid,))
    for imei, reason in batch_data.get('unpackedFallouts', {}).items():
        c.execute(f'INSERT INTO unpacked_fallouts (batch_id, imei, reason) VALUES ({_ph(3)})',
                  (bid, imei, reason))

    # Save recovered IMEIs
    c.execute(f'DELETE FROM recovered_imeis WHERE batch_id={ph}', (bid,))
    for r in batch_data.get('recoveredIMEIs', []):
        c.execute(f'''INSERT INTO recovered_imeis
            (batch_id, imei, rma_number, tracking_out, added_manually, raw_json)
            VALUES ({_ph(6)})''',
            (bid, r['imei'], r.get('rmaNumber', ''), r.get('trackingOut', ''),
             1 if r.get('addedManually') else 0, json.dumps(r.get('_raw', {}))))

    conn.commit()
    conn.close()
    return bid


def _batch_values(bid, batch_data):
    """Build the values tuple for batch insert/upsert."""
    return (
        bid, batch_data['vendor'], batch_data['shipDate'],
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
        batch_data.get('unpackedCount', 0),
        json.dumps(batch_data.get('submittedIMEIDetails', {})),
    )


def load_batch(batch_id):
    """Load a full batch with units, resolutions, fallouts, and recovered IMEIs."""
    conn = get_db()
    c = conn.cursor()
    ph = _PH

    c.execute(f'SELECT * FROM batches WHERE id={ph}', (batch_id,))
    row = _fetchone(c)
    if not row:
        conn.close()
        return None

    batch = _row_to_batch(row)

    c.execute(f'SELECT * FROM units WHERE batch_id={ph} ORDER BY unit_index', (batch_id,))
    batch['units'] = [_row_to_unit(u) for u in _fetchall(c)]

    c.execute(f'SELECT * FROM imei_resolutions WHERE batch_id={ph}', (batch_id,))
    batch['imeiMismatchResolutions'] = {}
    for r in _fetchall(c):
        key = f"{r['rma_or_tracking']}::{r['imei']}::{r['direction']}"
        batch['imeiMismatchResolutions'][key] = {
            'imei': r['imei'],
            'rmaOrTracking': r['rma_or_tracking'],
            'direction': r['direction'],
            'reason': r['reason'],
            'resolvedAt': r['resolved_at'],
        }

    c.execute(f'SELECT * FROM unpacked_fallouts WHERE batch_id={ph}', (batch_id,))
    batch['unpackedFallouts'] = {f['imei']: f['reason'] for f in _fetchall(c)}

    c.execute(f'SELECT * FROM recovered_imeis WHERE batch_id={ph}', (batch_id,))
    batch['recoveredIMEIs'] = [{
        'imei': r['imei'],
        'rmaNumber': r['rma_number'],
        'trackingOut': r['tracking_out'],
        'addedManually': bool(r['added_manually']),
        '_raw': json.loads(r['raw_json']) if r['raw_json'] else {},
    } for r in _fetchall(c)]

    conn.close()
    return batch


def load_all_batches():
    """Load all batches ordered by creation date desc."""
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id FROM batches ORDER BY created_at DESC')
    rows = _fetchall(c)
    conn.close()
    return [load_batch(r['id']) for r in rows]


def load_batches_by_vendor(vendor):
    """Load all batches for a specific vendor."""
    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT id FROM batches WHERE vendor={_PH} ORDER BY created_at DESC', (vendor,))
    rows = _fetchall(c)
    conn.close()
    return [load_batch(r['id']) for r in rows]


def delete_batch(batch_id):
    """Delete a batch and all related data (explicit child deletion + cascade)."""
    conn = get_db()
    c = conn.cursor()
    # Explicitly delete child rows first (in case CASCADE isn't active)
    c.execute(f'DELETE FROM recovered_imeis WHERE batch_id={_PH}', (batch_id,))
    c.execute(f'DELETE FROM unpacked_fallouts WHERE batch_id={_PH}', (batch_id,))
    c.execute(f'DELETE FROM imei_resolutions WHERE batch_id={_PH}', (batch_id,))
    c.execute(f'DELETE FROM units WHERE batch_id={_PH}', (batch_id,))
    c.execute(f'DELETE FROM activity_log WHERE batch_id={_PH}', (batch_id,))
    c.execute(f'DELETE FROM batches WHERE id={_PH}', (batch_id,))
    conn.commit()
    conn.close()


def update_batch_status(batch_id, status, cleared_at=None):
    """Update batch status and optional cleared_at timestamp."""
    conn = get_db()
    c = conn.cursor()
    ph = _PH
    if cleared_at:
        c.execute(f'UPDATE batches SET status={ph}, cleared_at={ph} WHERE id={ph}',
                  (status, cleared_at, batch_id))
    else:
        c.execute(f'UPDATE batches SET status={ph} WHERE id={ph}', (status, batch_id))
    conn.commit()
    conn.close()


def update_batch_counts(batch_id, counts):
    """Update batch count fields."""
    conn = get_db()
    c = conn.cursor()
    ph = _PH
    field_map = {
        'routeFailCount': 'route_fail_count',
        'agingFailCount': 'aging_fail_count',
        'qtyMismatchCount': 'qty_mismatch_count',
        'imeiMismatchCount': 'imei_mismatch_count',
        'hardStopCount': 'hard_stop_count',
        'unpackedCount': 'unpacked_count',
    }
    fields = []
    values = []
    for js_key, db_key in field_map.items():
        if js_key in counts:
            fields.append(f'{db_key}={ph}')
            values.append(counts[js_key])
    if fields:
        values.append(batch_id)
        c.execute(f'UPDATE batches SET {",".join(fields)} WHERE id={ph}', values)
        conn.commit()
    conn.close()


# ════════════════════════════════════
# SETTINGS
# ════════════════════════════════════

def get_setting(key, default=None):
    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT value FROM settings WHERE key={_PH}', (key,))
    row = _fetchone(c)
    conn.close()
    return row['value'] if row else default


def set_setting(key, value):
    conn = get_db()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute(f'INSERT INTO settings (key, value) VALUES ({_ph(2)}) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value',
                  (key, value))
    else:
        c.execute(f'INSERT OR REPLACE INTO settings (key, value) VALUES ({_ph(2)})', (key, value))
    conn.commit()
    conn.close()


def get_all_settings():
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT key, value FROM settings')
    rows = _fetchall(c)
    conn.close()
    return {r['key']: r['value'] for r in rows}


# ════════════════════════════════════
# HELPERS
# ════════════════════════════════════

def _safe_json(row, key, default=None):
    """Safely read a JSON column that may not exist yet (migration pending)."""
    try:
        val = row[key]
        return json.loads(val or 'null') if val is not None else (default if default is not None else {})
    except (KeyError, IndexError):
        return default if default is not None else {}


def _row_to_batch(row):
    """Convert a database row to a batch dict matching the JS batch object shape."""
    vendor = row['vendor']
    headers = json.loads(row['headers_json']) if row['headers_json'] else []
    header_key = '_vzHeaders' if vendor == 'VERIZON' else '_bpHeaders'

    return {
        'id': row['id'],
        'vendor': vendor,
        'shipDate': row['ship_date'],
        'createdAt': row['created_at'],
        'status': row['status'],
        'clearedAt': row['cleared_at'],
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
        'routeFailCount': row['route_fail_count'],
        'agingFailCount': row['aging_fail_count'],
        'qtyMismatchCount': row['qty_mismatch_count'],
        'imeiMismatchCount': row['imei_mismatch_count'],
        'hardStopCount': row['hard_stop_count'],
        'unpackedCount': row['unpacked_count'],
        'submittedIMEIDetails': _safe_json(row, 'submitted_imei_details_json', {}),
        '_viewing': False,
    }


def _row_to_unit(row):
    """Convert a database row to a unit dict matching the JS unit object shape."""
    return {
        'index': row['unit_index'],
        'route': row['route'],
        'sku': row['sku'],
        'imei': row['imei'],
        'rmaNumber': row['rma_number'],
        'submissionDate': row['submission_date'],
        'trackingOut': row['tracking_out'],
        'routeStatus': row['route_status'],
        'agingStatus': row['aging_status'],
        'daysSinceSubmission': row['days_since_submission'],
        'hardStop': bool(row['hard_stop']),
        'hardStopReason': row['hard_stop_reason'],
        'falloutReason': row['fallout_reason'],
        'falloutNotes': row['fallout_notes'],
        'imeiMismatch': bool(row['imei_mismatch']),
        'imeiMismatchReason': row['imei_mismatch_reason'],
        'imeiMatch': bool(row['imei_match']),
        'imeiResolved': row['imei_resolved'],
        'routeCorrected': bool(row['route_corrected']),
        'originalRoute': row['original_route'],
        'routeCorrectionNotes': row['route_correction_notes'],
        'agingCleared': bool(row['aging_cleared']),
        'agingClearedNotes': row['aging_cleared_notes'],
        'removedFromShipment': bool(row['removed_from_shipment']),
        '_raw': json.loads(row['raw_json']) if row['raw_json'] else {},
    }


# ════════════════════════════════════
# USER MANAGEMENT
# ════════════════════════════════════

def create_user(username, display_name, password_hash, role='user'):
    """Create a new user. Returns the user id."""
    conn = get_db()
    c = conn.cursor()
    c.execute(f'INSERT INTO users (username, display_name, password_hash, role, created_at) VALUES ({_ph(5)})',
              (username, display_name, password_hash, role, datetime.utcnow().isoformat()))
    conn.commit()
    user_id = c.lastrowid
    conn.close()
    return user_id


def get_user_by_username(username):
    """Get a user by username. Returns dict or None."""
    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT id, username, display_name, password_hash, role, created_at, is_active FROM users WHERE username={_PH}', (username,))
    row = _fetchone(c)
    conn.close()
    return row


def get_all_users():
    """Get all active users (without password_hash)."""
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id, username, display_name, role, created_at FROM users WHERE is_active=1')
    rows = _fetchall(c)
    conn.close()
    return rows


def update_user_role(user_id, role):
    """Update a user's role."""
    conn = get_db()
    c = conn.cursor()
    c.execute(f'UPDATE users SET role={_PH} WHERE id={_PH}', (role, user_id))
    conn.commit()
    conn.close()


def get_user_count():
    """Get the count of active users."""
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) as cnt FROM users WHERE is_active=1')
    row = _fetchone(c)
    conn.close()
    return row['cnt'] if row else 0


def delete_user(user_id):
    """Soft delete a user (set is_active=0)."""
    conn = get_db()
    c = conn.cursor()
    c.execute(f'UPDATE users SET is_active=0 WHERE id={_PH}', (user_id,))
    conn.commit()
    conn.close()


def update_user_password(user_id, new_password_hash):
    """Update a user's password hash."""
    conn = get_db()
    c = conn.cursor()
    c.execute(f'UPDATE users SET password_hash={_PH} WHERE id={_PH}', (new_password_hash, user_id))
    conn.commit()
    conn.close()


# ════════════════════════════════════
# ACTIVITY LOGGING
# ════════════════════════════════════

def log_activity(user_id, username, action, batch_id=None, details=None):
    """Log an activity to the activity_log table."""
    conn = get_db()
    c = conn.cursor()
    c.execute(f'INSERT INTO activity_log (user_id, username, action, batch_id, details, created_at) VALUES ({_ph(6)})',
              (user_id, username, action, batch_id, details, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()


def get_activity_log(batch_id=None, limit=100):
    """Get recent activity log entries, optionally filtered by batch_id."""
    conn = get_db()
    c = conn.cursor()
    if batch_id:
        c.execute(f'SELECT * FROM activity_log WHERE batch_id={_PH} ORDER BY created_at DESC LIMIT {_PH}',
                  (batch_id, limit))
    else:
        c.execute(f'SELECT * FROM activity_log ORDER BY created_at DESC LIMIT {_PH}', (limit,))
    rows = _fetchall(c)
    conn.close()
    return rows


# Initialize on import
init_db()
