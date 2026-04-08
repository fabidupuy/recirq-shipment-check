"""
RecirQ Global — Shipment Check Server
Flask web server that serves the Shipment Check app and persists data to SQLite.
"""
from flask import Flask, render_template, request, jsonify, send_from_directory, send_file, Response
import database as db
import os
import io
import math
import json
import hashlib
import uuid
from datetime import datetime, date

# S3 setup (optional — only if AWS credentials are configured)
s3_client = None
S3_BUCKET = os.environ.get('AWS_S3_BUCKET', 'recirq-packing-photos')
S3_REGION = os.environ.get('AWS_S3_REGION', 'us-east-2')
try:
    import boto3
    if os.environ.get('AWS_ACCESS_KEY_ID'):
        s3_client = boto3.client('s3',
            region_name=S3_REGION,
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        )
        print(f"  S3 configured: bucket={S3_BUCKET}, region={S3_REGION}")
    else:
        print("  S3 not configured (no AWS_ACCESS_KEY_ID)")
except ImportError:
    print("  S3 not available (boto3 not installed)")

app = Flask(__name__, template_folder='templates', static_folder='static')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max upload


# ════════════════════════════════════
# PAGE ROUTES
# ════════════════════════════════════

@app.route('/')
def index():
    """Serve the main application."""
    print("Serving index.html...")
    try:
        result = render_template('index.html')
        print(f"index.html rendered OK, length={len(result)}")
        return result
    except Exception as e:
        print(f"ERROR rendering index.html: {e}")
        return f"<h1>Error</h1><pre>{e}</pre>", 500


# ════════════════════════════════════
# BATCH API
# ════════════════════════════════════

@app.route('/api/batches', methods=['GET'])
def get_batches():
    """Get all batches (optionally filtered by vendor)."""
    vendor = request.args.get('vendor')
    if vendor:
        batches = db.load_batches_by_vendor(vendor.upper())
    else:
        batches = db.load_all_batches()
    return jsonify(batches)


@app.route('/api/batches/<int:batch_id>', methods=['GET'])
def get_batch(batch_id):
    """Get a single batch with all its data."""
    batch = db.load_batch(batch_id)
    if not batch:
        return jsonify({'error': 'Batch not found'}), 404
    return jsonify(batch)


@app.route('/api/batches', methods=['POST'])
def create_batch():
    """Save a new batch (or update an existing one)."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    try:
        batch_id = db.save_batch(data)
        return jsonify({'id': batch_id, 'status': 'saved'})
    except Exception as e:
        print(f"[ERROR] save_batch failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/batches/<int:batch_id>', methods=['PUT'])
def update_batch(batch_id):
    """Update an existing batch (full replacement)."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    data['id'] = batch_id
    db.save_batch(data)
    return jsonify({'id': batch_id, 'status': 'updated'})


@app.route('/api/batches/<int:batch_id>', methods=['DELETE'])
def delete_batch_route(batch_id):
    """Delete a batch and all related data."""
    db.delete_batch(batch_id)
    return jsonify({'status': 'deleted'})


@app.route('/api/batches/<int:batch_id>/status', methods=['PATCH'])
def patch_batch_status(batch_id):
    """Update batch status (e.g., clear to ship)."""
    data = request.get_json()
    status = data.get('status')
    cleared_at = data.get('clearedAt')
    if status:
        db.update_batch_status(batch_id, status, cleared_at)
    counts = {k: v for k, v in data.items() if k in (
        'routeFailCount', 'agingFailCount', 'qtyMismatchCount',
        'imeiMismatchCount', 'hardStopCount', 'unpackedCount'
    )}
    if counts:
        db.update_batch_counts(batch_id, counts)
    return jsonify({'status': 'updated'})


# ════════════════════════════════════
# AUTHENTICATION API
# ════════════════════════════════════

def hash_password(password):
    """Hash a password using SHA-256."""
    return hashlib.sha256(password.encode()).hexdigest()


@app.route('/api/auth/status', methods=['GET'])
def auth_status():
    """Check if any users exist (for first-time setup)."""
    count = db.get_user_count()
    return jsonify({'hasUsers': count > 0, 'userCount': count})


@app.route('/api/auth/login', methods=['POST'])
def login():
    """Authenticate a user with username and password."""
    data = request.get_json()
    username = data.get('username', '').strip()
    password = data.get('password', '')

    if not username or not password:
        return jsonify({'success': False, 'error': 'Username and password required'}), 400

    user = db.get_user_by_username(username)
    if not user or user['password_hash'] != hash_password(password):
        return jsonify({'success': False, 'error': 'Invalid username or password'}), 401

    if not user['is_active']:
        return jsonify({'success': False, 'error': 'User account is inactive'}), 403

    return jsonify({
        'success': True,
        'user': {
            'id': user['id'],
            'username': user['username'],
            'displayName': user['display_name'],
            'role': user.get('role', 'user')
        }
    })


# ════════════════════════════════════
# USER MANAGEMENT API
# ════════════════════════════════════

@app.route('/api/users', methods=['GET'])
def get_users():
    """Get all active users."""
    users = db.get_all_users()
    return jsonify(users)


@app.route('/api/users', methods=['POST'])
def create_user():
    """Create a new user."""
    data = request.get_json()
    username = data.get('username', '').strip()
    display_name = data.get('displayName', '').strip()
    password = data.get('password', '')
    role = data.get('role', 'user')

    if not username or not display_name or not password:
        return jsonify({'success': False, 'error': 'Username, display name, and password required'}), 400

    # Validate role
    if role not in ('admin', 'user'):
        role = 'user'

    # If this is the very first user, force admin role
    if db.get_user_count() == 0:
        role = 'admin'

    existing = db.get_user_by_username(username)
    if existing:
        return jsonify({'success': False, 'error': 'Username already exists'}), 409

    password_hash = hash_password(password)
    user_id = db.create_user(username, display_name, password_hash, role)

    return jsonify({
        'success': True,
        'user': {
            'id': user_id,
            'username': username,
            'displayName': display_name,
            'role': role
        }
    }), 201


@app.route('/api/users/<int:user_id>', methods=['DELETE'])
def delete_user_route(user_id):
    """Soft-delete a user."""
    db.delete_user(user_id)
    return jsonify({'success': True, 'message': 'User deleted'})


@app.route('/api/users/<int:user_id>/password', methods=['POST'])
def update_user_password(user_id):
    """Update a user's password."""
    data = request.get_json()
    password = data.get('password', '')

    if not password:
        return jsonify({'success': False, 'error': 'Password required'}), 400

    password_hash = hash_password(password)
    db.update_user_password(user_id, password_hash)

    return jsonify({'success': True, 'message': 'Password updated'})


@app.route('/api/users/<int:user_id>/role', methods=['POST'])
def update_user_role(user_id):
    """Update a user's role."""
    data = request.get_json()
    role = data.get('role', '')

    if role not in ('admin', 'user'):
        return jsonify({'success': False, 'error': 'Invalid role. Must be admin or user.'}), 400

    db.update_user_role(user_id, role)

    return jsonify({'success': True, 'message': f'Role updated to {role}'})


# ════════════════════════════════════
# ACTIVITY LOGGING API
# ════════════════════════════════════

@app.route('/api/activity', methods=['POST'])
def log_activity_route():
    """Log an activity."""
    data = request.get_json()
    user_id = data.get('userId')
    username = data.get('username', '')
    action = data.get('action', '')
    batch_id = data.get('batchId')
    details = data.get('details')

    if not user_id or not action:
        return jsonify({'success': False, 'error': 'userId and action required'}), 400

    if details and isinstance(details, dict):
        details = json.dumps(details)

    db.log_activity(user_id, username, action, batch_id, details)
    return jsonify({'success': True})


@app.route('/api/activity', methods=['GET'])
def get_activity_log_route():
    """Get activity log, optionally filtered by batch_id."""
    batch_id = request.args.get('batch_id', type=int)
    limit = request.args.get('limit', 100, type=int)

    log_entries = db.get_activity_log(batch_id=batch_id, limit=limit)
    return jsonify(log_entries)


@app.route('/api/activity/<int:batch_id>', methods=['GET'])
def get_batch_activity(batch_id):
    """Get activity log for a specific batch."""
    limit = request.args.get('limit', 100, type=int)
    log_entries = db.get_activity_log(batch_id=batch_id, limit=limit)
    return jsonify(log_entries)


# ════════════════════════════════════
# PICK & PACK STATE API
# ════════════════════════════════════

@app.route('/api/pp/state', methods=['GET'])
def get_pp_state():
    """Get all Pick & Pack state (ppJobs, ppCompletedRMAs, ppJobs_version)."""
    state = db.get_all_pp_state()
    # Include version for optimistic concurrency control
    version_json = db.get_pp_state('ppJobs_version')
    state['ppJobs_version'] = int(version_json) if version_json else 0
    return jsonify(state)


@app.route('/api/pp/state/<key>', methods=['POST'])
def save_pp_state(key):
    """Save a Pick & Pack state value with optimistic concurrency control."""
    try:
        data = request.get_json()
        value = data.get('value', {})
        client_version = data.get('version')  # version the client thinks it has

        # Optimistic concurrency: reject stale saves for ppJobs
        if key == 'ppJobs':
            version_json = db.get_pp_state('ppJobs_version')
            server_version = int(version_json) if version_json else 0
            # Reject if: (a) client sends wrong version, or (b) client sends no version
            # but server version > 0 (meaning new code has already saved at least once)
            if client_version is None and server_version > 0:
                return jsonify({
                    'status': 'conflict',
                    'message': 'Old client without version support. Please refresh.',
                    'server_version': server_version
                }), 409
            if client_version is not None and client_version != server_version:
                return jsonify({
                    'status': 'conflict',
                    'message': 'State was modified by another tab. Reloading.',
                    'server_version': server_version,
                    'client_version': client_version
                }), 409
            new_version = server_version + 1
            db.save_pp_state('ppJobs_version', str(new_version))
            value_json = json.dumps(value)
            db.save_pp_state(key, value_json)
            return jsonify({'status': 'saved', 'key': key, 'size': len(value_json), 'version': new_version})

        # Guard ppCompletedRMAs: preserve server-side tracking fixes from being overwritten by stale tabs
        if key == 'ppCompletedRMAs' and isinstance(value, list):
            existing_json = db.get_pp_state('ppCompletedRMAs')
            if existing_json:
                existing = json.loads(existing_json)
                server_tracking = {}
                for e in existing:
                    if e.get('trackingUpdatedAt'):
                        k = e.get('rma', '') + '|' + e.get('completedAt', '')
                        server_tracking[k] = {
                            'tracking': e['tracking'],
                            'trackingUpdatedAt': e['trackingUpdatedAt']
                        }
                if server_tracking:
                    for e in value:
                        k = e.get('rma', '') + '|' + e.get('completedAt', '')
                        if k in server_tracking:
                            sv = server_tracking[k]
                            incoming_ts = e.get('trackingUpdatedAt', '')
                            server_ts = sv['trackingUpdatedAt']
                            if server_ts > (incoming_ts or ''):
                                e['tracking'] = sv['tracking']
                                e['trackingUpdatedAt'] = sv['trackingUpdatedAt']
                                for u in e.get('units', []):
                                    if u.get('trackingNumber') and u['trackingNumber'] != sv['tracking']:
                                        u['trackingNumber'] = sv['tracking']

        value_json = json.dumps(value)
        db.save_pp_state(key, value_json)
        return jsonify({'status': 'saved', 'key': key, 'size': len(value_json)})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/pp/jobs/<vendor>', methods=['DELETE'])
def delete_pp_job(vendor):
    """Delete a packing job for a specific vendor (admin only)."""
    # Load current ppJobs, remove the vendor, save back
    state_json = db.get_pp_state('ppJobs')
    if not state_json:
        return jsonify({'error': 'No packing jobs found'}), 404
    jobs = json.loads(state_json)
    if vendor not in jobs:
        return jsonify({'error': f'No job found for vendor {vendor}'}), 404
    unit_count = len(jobs[vendor].get('units', []))
    del jobs[vendor]
    db.save_pp_state('ppJobs', json.dumps(jobs))
    return jsonify({'status': 'deleted', 'vendor': vendor, 'unitsRemoved': unit_count})


# ════════════════════════════════════
# PHOTO UPLOAD API (S3)
# ════════════════════════════════════

@app.route('/api/photos/upload', methods=['POST'])
def upload_photo():
    """Upload a photo through the server to S3 (avoids CORS issues)."""
    if not s3_client:
        return jsonify({'error': 'S3 not configured'}), 500

    if 'photo' not in request.files:
        return jsonify({'error': 'No photo file provided'}), 400

    file = request.files['photo']
    imei = request.form.get('imei', 'unknown')
    photo_type = request.form.get('type', 'unit')
    vendor = request.form.get('vendor', '')
    photo_id = str(uuid.uuid4())[:8]

    from datetime import date
    today = date.today().isoformat()
    key = f"{today}/{vendor}/{photo_type}/{imei}_{photo_id}.jpg"

    try:
        s3_client.upload_fileobj(
            file,
            S3_BUCKET,
            key,
            ExtraArgs={'ContentType': 'image/jpeg'}
        )
        # Generate a presigned GET URL for viewing
        view_url = s3_client.generate_presigned_url('get_object',
            Params={'Bucket': S3_BUCKET, 'Key': key},
            ExpiresIn=604800,  # 7 days
        )
        return jsonify({'viewUrl': view_url, 'key': key, 'photoId': photo_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/photos/save', methods=['POST'])
def save_photo_refs():
    """Save photo references for a specific IMEI/box. Merges into existing ppPhotos state."""
    data = request.get_json()
    key = data.get('key', '')  # e.g., 'box-326134905' or '355606780524844'
    photos = data.get('photos', [])
    print(f"[PhotoSave] key={key!r}, photos_count={len(photos)}")
    if not key or not photos:
        return jsonify({'error': 'key and photos required'}), 400

    # Merge into ppPhotos state
    state_json = db.get_pp_state('ppPhotos')
    all_photos = json.loads(state_json) if state_json else {}
    if key not in all_photos:
        all_photos[key] = []
    all_photos[key].extend(photos)
    db.save_pp_state('ppPhotos', json.dumps(all_photos))
    print(f"[PhotoSave] Saved. Total for key {key!r}: {len(all_photos[key])}. All keys: {list(all_photos.keys())}")
    return jsonify({'status': 'saved', 'count': len(all_photos[key])})


def _refresh_presigned_urls(photo_list):
    """Regenerate presigned URLs for photos whose S3 keys are known.
    This ensures photos remain viewable even after the original URL expires."""
    if not s3_client or not photo_list:
        return photo_list
    refreshed = []
    for p in photo_list:
        s3_key = p.get('key')
        if s3_key:
            try:
                p['viewUrl'] = s3_client.generate_presigned_url('get_object',
                    Params={'Bucket': S3_BUCKET, 'Key': s3_key},
                    ExpiresIn=604800)  # 7 days
            except Exception:
                pass  # Keep existing viewUrl if regeneration fails
        refreshed.append(p)
    return refreshed


@app.route('/api/photos/list/<key>', methods=['GET'])
def list_photos(key):
    """List all photos for an IMEI or box key, with fresh presigned URLs."""
    state = db.get_pp_state('ppPhotos')
    if state:
        photos = json.loads(state)
        result = photos.get(key, [])
        print(f"[PhotoList] key={key!r}, found={len(result)} photos. All keys: {list(photos.keys())}")
        return jsonify(_refresh_presigned_urls(result))
    print(f"[PhotoList] key={key!r}, NO ppPhotos state at all")
    return jsonify([])


@app.route('/api/photos/migrate-box0', methods=['GET'])
def migrate_box0_photos():
    """One-time migration: photos saved under the buggy 'box-0' key need to be
    distributed to the correct 'box-{N}' keys.  We look at each photo's S3 key
    path which contains the IMEI field (which was always correct, e.g. 'box-1')
    and re-key accordingly."""
    state_json = db.get_pp_state('ppPhotos')
    if not state_json:
        return jsonify({'migrated': 0})
    all_p = json.loads(state_json)
    box0 = all_p.get('box-0', [])
    if not box0:
        return jsonify({'migrated': 0})

    migrated = 0
    remaining = []
    for photo in box0:
        s3_key = photo.get('key', '')
        # S3 key format: date/vendor/type/box-N_photoId.jpg
        # Extract the 'box-N' part from the filename
        filename = s3_key.rsplit('/', 1)[-1] if '/' in s3_key else s3_key
        # filename looks like 'box-1_abc12345.jpg' or 'box-0_abc12345.jpg'
        parts = filename.split('_', 1)
        real_key = parts[0] if parts[0].startswith('box-') else None

        if real_key and real_key != 'box-0':
            # Move to correct key
            if real_key not in all_p:
                all_p[real_key] = []
            all_p[real_key].append(photo)
            migrated += 1
            print(f"[PhotoMigrate] Moved photo from box-0 -> {real_key}: {s3_key}")
        else:
            remaining.append(photo)

    if migrated > 0:
        all_p['box-0'] = remaining
        if not remaining:
            del all_p['box-0']
        db.save_pp_state('ppPhotos', json.dumps(all_p))
        print(f"[PhotoMigrate] Done. Migrated {migrated} photos. Remaining in box-0: {len(remaining)}")

    return jsonify({'migrated': migrated, 'remaining': len(remaining)})


@app.route('/api/fix/swap-tracking', methods=['GET'])
def fix_swap_tracking():
    """One-time fix: swap tracking numbers between two Brightpoint groups."""
    state_json = db.get_pp_state('ppCompletedRMAs')
    if not state_json:
        return jsonify({'error': 'No completed RMAs found'}), 404
    entries = json.loads(state_json)
    swapped = 0
    for e in entries:
        if e.get('vendor') != 'VERIZON' and e.get('submissionDate') == '3/17/2026' and e.get('tracking') == 'BRP03232026':
            e['tracking'] = 'BRP03182026'
            swapped += 1
        elif e.get('vendor') != 'VERIZON' and e.get('submissionDate') == '3/19/2026' and e.get('tracking', '') in ('', 'NO_TRACKING'):
            e['tracking'] = 'BRP03232026'
            swapped += 1
    if swapped > 0:
        db.save_pp_state('ppCompletedRMAs', json.dumps(entries))
    return jsonify({'swapped': swapped, 'details': '3/17 -> BRP03182026, 3/19 -> BRP03232026'})


@app.route('/api/fix/rename-tracking', methods=['GET'])
def fix_rename_tracking():
    """One-time fix: rename a tracking number. Usage: ?old=X&new=Y&rma=Z (rma optional)"""
    old_val = request.args.get('old', '')
    new_val = request.args.get('new', '')
    rma_filter = request.args.get('rma', '')
    if not old_val or not new_val:
        return jsonify({'error': 'Provide ?old=...&new=... query params (optional &rma=... to target specific RMA)'}), 400
    state_json = db.get_pp_state('ppCompletedRMAs')
    if not state_json:
        return jsonify({'error': 'No completed RMAs found'}), 404
    entries = json.loads(state_json)
    updated = 0
    units_updated = 0
    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()
    for e in entries:
        if e.get('tracking') == old_val:
            if rma_filter and e.get('rma') != rma_filter:
                continue
            e['tracking'] = new_val
            e['trackingUpdatedAt'] = now_iso
            updated += 1
            # Also update unit-level trackingNumber so dashboard counts match
            for u in e.get('units', []):
                if u.get('trackingNumber') == old_val:
                    u['trackingNumber'] = new_val
                    units_updated += 1
    if updated > 0:
        db.save_pp_state('ppCompletedRMAs', json.dumps(entries))
    return jsonify({'updated': updated, 'units_updated': units_updated, 'old': old_val, 'new': new_val, 'rma': rma_filter or 'all'})


@app.route('/api/fix/mark-shipped', methods=['GET'])
def fix_mark_shipped():
    """Mark a tracking group as already shipped (hides from auto-fill).
    Usage: /api/fix/mark-shipped?tracking=BRP03182026-1
    """
    tracking_val = request.args.get('tracking', '')
    if not tracking_val:
        return jsonify({'error': 'Provide ?tracking=... query param'}), 400
    state_json = db.get_pp_state('ppCompletedRMAs')
    if not state_json:
        return jsonify({'error': 'No completed RMAs found'}), 404
    entries = json.loads(state_json)
    marked = 0
    for e in entries:
        if e.get('tracking') == tracking_val:
            e['loadedToShipmentCheck'] = True
            marked += 1
    if marked > 0:
        db.save_pp_state('ppCompletedRMAs', json.dumps(entries))
    return jsonify({'marked_shipped': marked, 'tracking': tracking_val})


@app.route('/api/fix/dedup-completed', methods=['GET'])
def fix_dedup_completed():
    """Remove duplicate entries from ppCompletedRMAs, keeping the first of each RMA."""
    state_json = db.get_pp_state('ppCompletedRMAs')
    if not state_json:
        return jsonify({'error': 'No completed RMAs found'}), 404
    entries = json.loads(state_json)
    seen = set()
    deduped = []
    removed = 0
    for e in entries:
        key = e.get('rma', '') + '|' + e.get('vendor', '')
        if e.get('rma', '').startswith('FALLOUT'):
            # Fallouts use a different key to allow per-date fallouts
            key = e.get('rma', '') + '|' + e.get('vendor', '') + '|' + e.get('submissionDate', '')
        if key in seen:
            removed += 1
            continue
        seen.add(key)
        deduped.append(e)
    if removed > 0:
        db.save_pp_state('ppCompletedRMAs', json.dumps(deduped))
    return jsonify({'removed': removed, 'before': len(entries), 'after': len(deduped)})


@app.route('/api/photos/all', methods=['GET'])
def all_photos():
    """Get all photo references with fresh presigned URLs."""
    state = db.get_pp_state('ppPhotos')
    if state:
        all_photos_data = json.loads(state)
        for key in all_photos_data:
            all_photos_data[key] = _refresh_presigned_urls(all_photos_data[key])
        return jsonify(all_photos_data)
    return jsonify({})


# In-memory store for short photo tokens (they expire in 10 min anyway)
_photo_tokens = {}

@app.route('/api/photos/token', methods=['POST'])
def create_photo_token():
    """Create a short token for photo upload QR codes."""
    import time
    data = request.get_json()
    token_id = uuid.uuid4().hex[:8]
    _photo_tokens[token_id] = data
    # Clean up expired tokens
    now = time.time() * 1000
    expired = [k for k, v in _photo_tokens.items() if v.get('expires', 0) < now]
    for k in expired:
        del _photo_tokens[k]
    return jsonify({'token': token_id})


@app.route('/photo/<token>', methods=['GET'])
def photo_upload_page(token):
    """Serve the phone camera upload page. Supports both short tokens and legacy base64 tokens."""
    # Check if it's a short token
    if token in _photo_tokens:
        import base64
        full_token = base64.b64encode(json.dumps(_photo_tokens[token]).encode()).decode()
        return render_template('photo_upload.html', token=full_token)
    # Legacy: token is already base64-encoded JSON
    return render_template('photo_upload.html', token=token)


# ════════════════════════════════════
# SHIPPING LABEL UPLOAD API
# ════════════════════════════════════

@app.route('/api/labels/upload', methods=['POST'])
def upload_label():
    """Upload a shipping label (PDF or image) to S3."""
    if not s3_client:
        return jsonify({'error': 'S3 not configured'}), 500

    if 'label' not in request.files:
        return jsonify({'error': 'No label file provided'}), 400

    file = request.files['label']
    batch_id = request.form.get('batchId', 'unknown')
    vendor = request.form.get('vendor', '')
    label_id = str(uuid.uuid4())[:8]

    # Determine content type
    filename = file.filename or 'label'
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else 'pdf'
    content_types = {
        'pdf': 'application/pdf',
        'png': 'image/png',
        'jpg': 'image/jpeg',
        'jpeg': 'image/jpeg',
    }
    content_type = content_types.get(ext, 'application/octet-stream')

    from datetime import date
    today = date.today().isoformat()
    key = f"{today}/{vendor}/labels/batch_{batch_id}_{label_id}.{ext}"

    try:
        s3_client.upload_fileobj(
            file,
            S3_BUCKET,
            key,
            ExtraArgs={'ContentType': content_type}
        )
        view_url = s3_client.generate_presigned_url('get_object',
            Params={'Bucket': S3_BUCKET, 'Key': key},
            ExpiresIn=604800,
        )
        return jsonify({
            'viewUrl': view_url,
            'key': key,
            'labelId': label_id,
            'filename': filename,
            'contentType': content_type
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/labels/all', methods=['GET'])
def get_all_labels():
    """Get all labels for all batches, with refreshed presigned URLs."""
    state_json = db.get_pp_state('ppLabels')
    all_labels = json.loads(state_json) if state_json else {}
    if s3_client:
        for batch_id in all_labels:
            for lbl in all_labels[batch_id]:
                s3_key = lbl.get('key')
                if s3_key:
                    try:
                        lbl['viewUrl'] = s3_client.generate_presigned_url('get_object',
                            Params={'Bucket': S3_BUCKET, 'Key': s3_key},
                            ExpiresIn=604800)
                    except Exception:
                        pass
    return jsonify(all_labels)


@app.route('/api/labels/<batch_id>', methods=['GET'])
def get_labels(batch_id):
    """Get all labels for a batch, with refreshed presigned URLs."""
    state_json = db.get_pp_state('ppLabels')
    all_labels = json.loads(state_json) if state_json else {}
    labels = all_labels.get(str(batch_id), [])
    # Refresh presigned URLs
    if s3_client:
        for lbl in labels:
            s3_key = lbl.get('key')
            if s3_key:
                try:
                    lbl['viewUrl'] = s3_client.generate_presigned_url('get_object',
                        Params={'Bucket': S3_BUCKET, 'Key': s3_key},
                        ExpiresIn=604800)
                except Exception:
                    pass
    return jsonify(labels)


@app.route('/api/labels/save', methods=['POST'])
def save_label_ref():
    """Save a label reference for a batch."""
    data = request.get_json()
    batch_id = str(data.get('batchId', ''))
    label = data.get('label', {})
    if not batch_id or not label:
        return jsonify({'error': 'batchId and label required'}), 400

    state_json = db.get_pp_state('ppLabels')
    all_labels = json.loads(state_json) if state_json else {}
    if batch_id not in all_labels:
        all_labels[batch_id] = []
    all_labels[batch_id].append(label)
    db.save_pp_state('ppLabels', json.dumps(all_labels))
    return jsonify({'status': 'saved', 'count': len(all_labels[batch_id])})


# ════════════════════════════════════
# SLACK PROXY API (for threading support)
# ════════════════════════════════════

@app.route('/api/slack/post', methods=['POST'])
def slack_post_message():
    """Post a message to Slack via bot token and return the thread timestamp."""
    import requests as req
    data = request.get_json()
    bot_token = data.get('botToken', '')
    channel_id = data.get('channelId', '')
    text = data.get('text', '')
    thread_ts = data.get('threadTs', None)

    if not bot_token or not channel_id or not text:
        return jsonify({'error': 'botToken, channelId, and text required'}), 400

    headers = {
        'Authorization': f'Bearer {bot_token}',
        'Content-Type': 'application/json; charset=utf-8'
    }
    payload = {
        'channel': channel_id,
        'text': text,
        'unfurl_links': False,
    }
    if thread_ts:
        payload['thread_ts'] = thread_ts

    try:
        resp = req.post('https://slack.com/api/chat.postMessage', json=payload, headers=headers, timeout=10)
        result = resp.json()
        if result.get('ok'):
            return jsonify({'ok': True, 'ts': result.get('ts'), 'channel': result.get('channel')})
        else:
            return jsonify({'ok': False, 'error': result.get('error', 'Unknown')}), 400
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/slack/upload', methods=['POST'])
def slack_upload_file():
    """Upload a file to Slack channel or thread via bot token."""
    import requests as req
    bot_token = request.form.get('botToken', '')
    channel_id = request.form.get('channelId', '')
    thread_ts = request.form.get('threadTs', '')
    title = request.form.get('title', 'File')
    comment = request.form.get('comment', '')

    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if not bot_token or not channel_id:
        return jsonify({'error': 'botToken and channelId required'}), 400

    headers = {'Authorization': f'Bearer {bot_token}'}
    form = {
        'channels': (None, channel_id),
        'title': (None, title),
    }
    if thread_ts:
        form['thread_ts'] = (None, thread_ts)
    if comment:
        form['initial_comment'] = (None, comment)

    files_data = {'file': (file.filename or 'file', file.stream, file.content_type or 'application/octet-stream')}

    try:
        resp = req.post('https://slack.com/api/files.upload',
            headers=headers, files={**files_data, **form}, timeout=30)
        result = resp.json()
        if result.get('ok'):
            return jsonify({'ok': True})
        else:
            return jsonify({'ok': False, 'error': result.get('error', 'Unknown')}), 400
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/slack/search', methods=['POST'])
def slack_search_message():
    """Search for a message in a Slack channel by text content.
    Used to find the clearance message thread_ts for batches cleared before threading was added."""
    import requests as req
    data = request.get_json()
    bot_token = data.get('botToken', '')
    channel_id = data.get('channelId', '')
    search_text = data.get('searchText', '')

    if not bot_token or not channel_id or not search_text:
        return jsonify({'ok': False, 'error': 'botToken, channelId, and searchText required'}), 400

    headers = {'Authorization': f'Bearer {bot_token}'}

    try:
        # Use conversations.history to scan recent messages
        resp = req.get('https://slack.com/api/conversations.history',
            headers=headers,
            params={'channel': channel_id, 'limit': 100},
            timeout=10)
        result = resp.json()
        if not result.get('ok'):
            return jsonify({'ok': False, 'error': result.get('error', 'Unknown')}), 400

        # Search through messages for one containing the search text
        for msg in result.get('messages', []):
            text = msg.get('text', '')
            if search_text in text:
                return jsonify({'ok': True, 'ts': msg.get('ts')})

        return jsonify({'ok': False, 'error': 'Message not found'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


# ════════════════════════════════════
# EMAIL API
# ════════════════════════════════════

@app.route('/api/email/test', methods=['POST'])
def email_test():
    """Send a test email to verify SMTP configuration."""
    import smtplib
    from email.mime.text import MIMEText
    data = request.get_json()
    sender = data.get('sender', '')
    app_password = data.get('appPassword', '')
    to = data.get('to', sender)

    if not sender or not app_password:
        return jsonify({'ok': False, 'error': 'Sender and app password required'}), 400

    try:
        msg = MIMEText('This is a test email from RecirQ 3PL Reventory.\n\nEmail configuration is working correctly!')
        msg['Subject'] = 'RecirQ Shipment Check — Test Email'
        msg['From'] = sender
        msg['To'] = to

        with smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=10) as smtp:
            smtp.login(sender, app_password)
            smtp.sendmail(sender, [to], msg.as_string())

        return jsonify({'ok': True})
    except smtplib.SMTPAuthenticationError:
        return jsonify({'ok': False, 'error': 'Authentication failed. Check your email and app password.'}), 400
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/email/send-victra', methods=['POST'])
def email_send_victra():
    """Send a shipment email to Victra with photos and CSV attached."""
    import smtplib
    import requests as req
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders

    data = request.get_json()
    sender = data.get('sender', '')
    app_password = data.get('appPassword', '')
    to = data.get('to', '')
    cc = data.get('cc', '')
    vendor = data.get('vendor', '')
    ship_date = data.get('shipDate', '')
    trackings = data.get('trackings', [])
    unit_count = data.get('unitCount', 0)
    removed_count = data.get('removedCount', 0)
    photos = data.get('photos', [])
    csv_content = data.get('csvContent', '')
    batch_id = data.get('batchId', '')

    if not sender or not app_password or not to:
        return jsonify({'ok': False, 'error': 'Sender, app password, and recipients required'}), 400

    try:
        msg = MIMEMultipart()
        tracking_subj = ', '.join(trackings) if trackings else 'No Tracking'
        msg['Subject'] = f'{vendor} {tracking_subj} ASN {ship_date}'
        msg['From'] = sender
        msg['To'] = to
        if cc:
            msg['Cc'] = cc

        # Build HTML body
        tracking_str = ', '.join(trackings) if trackings else '—'
        body_html = f"""
        <div style="font-family:Arial,sans-serif;max-width:600px;">
            <h2 style="color:#2d5016;">RecirQ Global — Shipment Notification</h2>
            <table style="border-collapse:collapse;width:100%;margin:16px 0;">
                <tr><td style="padding:8px;border:1px solid #ddd;font-weight:bold;background:#f5f5f5;width:140px;">Vendor</td><td style="padding:8px;border:1px solid #ddd;">{vendor}</td></tr>
                <tr><td style="padding:8px;border:1px solid #ddd;font-weight:bold;background:#f5f5f5;">Ship Date</td><td style="padding:8px;border:1px solid #ddd;">{ship_date}</td></tr>
                <tr><td style="padding:8px;border:1px solid #ddd;font-weight:bold;background:#f5f5f5;">Tracking #(s)</td><td style="padding:8px;border:1px solid #ddd;font-family:monospace;">{tracking_str}</td></tr>
                <tr><td style="padding:8px;border:1px solid #ddd;font-weight:bold;background:#f5f5f5;">Units Shipped</td><td style="padding:8px;border:1px solid #ddd;">{unit_count}</td></tr>
                {"<tr><td style='padding:8px;border:1px solid #ddd;font-weight:bold;background:#f5f5f5;'>Units Removed</td><td style='padding:8px;border:1px solid #ddd;color:red;'>" + str(removed_count) + "</td></tr>" if removed_count > 0 else ""}
            </table>
            <p style="color:#888;font-size:12px;">Ship advice CSV and shipment photos are attached.</p>
            <hr style="border:none;border-top:1px solid #ddd;margin:16px 0;">
            <p style="color:#aaa;font-size:11px;">Sent from RecirQ 3PL Reventory</p>
        </div>
        """
        msg.attach(MIMEText(body_html, 'html'))

        # Attach CSV
        if csv_content:
            csv_part = MIMEBase('text', 'csv')
            csv_part.set_payload(csv_content.encode('utf-8'))
            encoders.encode_base64(csv_part)
            csv_part.add_header('Content-Disposition', 'attachment', filename=f'Ship_Advice_{vendor}_{ship_date}.csv')
            msg.attach(csv_part)

        # Attach photos (download from S3 presigned URLs)
        for i, photo in enumerate(photos):
            try:
                s3_key = photo.get('key', '')
                view_url = photo.get('viewUrl', '')
                # Regenerate presigned URL if we have the key
                if s3_key and s3_client:
                    view_url = s3_client.generate_presigned_url('get_object',
                        Params={'Bucket': S3_BUCKET, 'Key': s3_key},
                        ExpiresIn=300)
                if view_url:
                    photo_resp = req.get(view_url, timeout=15)
                    if photo_resp.status_code == 200:
                        photo_part = MIMEBase('image', 'jpeg')
                        photo_part.set_payload(photo_resp.content)
                        encoders.encode_base64(photo_part)
                        photo_part.add_header('Content-Disposition', 'attachment', filename=f'shipment_photo_{i+1}.jpg')
                        msg.attach(photo_part)
            except Exception as pe:
                print(f"  Failed to attach photo {i+1}: {pe}")

        # Send
        recipients = [r.strip() for r in to.split(',') if r.strip()]
        if cc:
            recipients += [r.strip() for r in cc.split(',') if r.strip()]

        with smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=30) as smtp:
            smtp.login(sender, app_password)
            smtp.sendmail(sender, recipients, msg.as_string())

        return jsonify({'ok': True, 'recipientCount': len(recipients)})
    except smtplib.SMTPAuthenticationError:
        return jsonify({'ok': False, 'error': 'Gmail authentication failed. Check app password.'}), 400
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


# ════════════════════════════════════
# SETTINGS API
# ════════════════════════════════════

@app.route('/api/settings', methods=['GET'])
def get_settings():
    """Get all app settings."""
    return jsonify(db.get_all_settings())


@app.route('/api/settings', methods=['POST'])
def save_settings():
    """Save app settings (key-value pairs)."""
    data = request.get_json()
    for key, value in data.items():
        db.set_setting(key, value)
    return jsonify({'status': 'saved'})


# ════════════════════════════════════
# REEBELO RECONCILIATION
# ════════════════════════════════════

def _reebelo_normalize(val):
    """Normalize a grade/disposition value for comparison."""
    if not val or val == "nan" or val == "None" or val == "N/A" or str(val).strip() == "":
        return ""
    return str(val).strip().upper().replace("_", "-")

_REEBELO_DISP_TO_GRADE = {
    "SHIP": "RTV-REEB",
    "DISPUTE": "DISPUTE",
    "ESCALATE": "EXCEPTION",
    "RTV-REEB": "RTV-REEB",
    "EXCEPTION": "EXCEPTION",
}

def _reebelo_parse_upload(file_bytes, filename):
    import pandas as pd
    from pathlib import Path
    ext = Path(filename).suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(io.BytesIO(file_bytes), dtype=str)
    else:
        df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
    df.columns = df.columns.str.strip()
    return df

def _reebelo_detect_type(df):
    cols = [c.lower() for c in df.columns]
    if any("internal grade" in c or "internalgrade" in c for c in cols):
        return "pbi"
    if any("disposition" in c for c in cols):
        return "sheet"
    return None

def _reebelo_find_col(df, candidates):
    for c in candidates:
        for col in df.columns:
            if col.lower().strip() == c.lower():
                return col
    for c in candidates:
        for col in df.columns:
            if c.lower() in col.lower():
                return col
    return None

def _reebelo_reconcile(pbi_df, sheet_df):
    pi = _reebelo_find_col(pbi_df, ["IMEI", "imei"])
    pg = _reebelo_find_col(pbi_df, ["Internal Grade", "InternalGrade"])
    si = _reebelo_find_col(sheet_df, ["IMEI", "imei"])
    sd = _reebelo_find_col(sheet_df, ["Disposition", "disposition"])

    if not all([pi, pg, si, sd]):
        missing = []
        if not pi: missing.append("IMEI in PowerBI")
        if not pg: missing.append("Internal Grade in PowerBI")
        if not si: missing.append("IMEI in Spreadsheet")
        if not sd: missing.append("Disposition in Spreadsheet")
        return {"error": f"Missing columns: {', '.join(missing)}"}

    pbi_df["_imei"] = pbi_df[pi].fillna("").astype(str).str.strip()
    sheet_df["_imei"] = sheet_df[si].fillna("").astype(str).str.strip()
    pbi_df = pbi_df[pbi_df["_imei"].notna() & (pbi_df["_imei"] != "") & (pbi_df["_imei"] != "nan")]
    sheet_df = sheet_df[sheet_df["_imei"].notna() & (sheet_df["_imei"] != "") & (sheet_df["_imei"] != "nan")]
    pbi_df["_g"] = pbi_df[pg].fillna("").astype(str).str.strip()
    sheet_df["_d"] = sheet_df[sd].fillna("").astype(str).str.strip()

    pbi_set = set(pbi_df["_imei"])
    sheet_set = set(sheet_df["_imei"])
    common = pbi_set & sheet_set

    pbi_lk = pbi_df.set_index("_imei")["_g"].to_dict()
    sheet_lk = sheet_df.set_index("_imei")["_d"].to_dict()

    matches, mismatches, obr, not_registered = [], [], [], []
    for imei in sorted(common):
        g_raw, d_raw = pbi_lk.get(imei, ""), sheet_lk.get(imei, "")
        g_norm = _reebelo_normalize(g_raw)
        d_norm = _reebelo_normalize(d_raw)

        if g_norm == "OBR":
            obr.append({"imei": imei, "grade": g_raw, "disp": d_raw})
            continue

        if d_norm == "":
            not_registered.append({"imei": imei, "grade": g_raw, "disp": "(empty)", "type": "Not Registered on Spreadsheet"})
            continue

        exp = _REEBELO_DISP_TO_GRADE.get(d_norm)
        if exp is None:
            mismatches.append({"imei": imei, "grade": g_raw, "disp": d_raw, "expected": f"UNKNOWN: '{d_raw}'", "type": "Unknown Disposition"})
        elif g_norm != exp:
            mismatches.append({"imei": imei, "grade": g_raw, "disp": d_raw, "expected": exp.replace("-", "_") if "_" in g_raw else exp, "type": "Route Mismatch"})
        else:
            matches.append({"imei": imei, "grade": g_raw, "disp": d_raw})

    ms = [{"imei": i, "grade": pbi_lk.get(i, "")} for i in sorted(pbi_set - sheet_set)]
    mp = [{"imei": i, "disp": sheet_lk.get(i, "")} for i in sorted(sheet_set - pbi_set)]

    return {
        "totalPBI": len(pbi_set), "totalSheet": len(sheet_set), "totalCommon": len(common),
        "matches": matches, "mismatches": mismatches, "obrAlerts": obr,
        "notRegistered": not_registered, "missingFromSheet": ms, "missingFromPBI": mp,
    }

def _reebelo_sanitize(obj):
    """Replace NaN/None/numpy values with JSON-safe equivalents."""
    if isinstance(obj, dict):
        return {k: _reebelo_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_reebelo_sanitize(v) for v in obj]
    if obj is None:
        return ""
    try:
        if isinstance(obj, (int, float)):
            if math.isnan(obj) or math.isinf(obj):
                return ""
    except (TypeError, ValueError):
        pass
    try:
        if hasattr(obj, 'item'):
            val = obj.item()
            if val is None:
                return ""
            if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                return ""
            return val
    except (TypeError, ValueError, OverflowError):
        pass
    if isinstance(obj, str) and obj.lower() in ("nan", "none", "nat"):
        return ""
    return obj

def _reebelo_build_slack_msg(r, dt):
    nr = r.get("notRegistered", [])
    tc = r["totalCommon"] - len(r["obrAlerts"]) - len(nr)
    mr = (len(r["matches"]) / tc * 100) if tc > 0 else 0
    ti = len(r["mismatches"]) + len(r["missingFromSheet"]) + len(r["missingFromPBI"]) + len(nr)
    st = "PASS" if mr >= 95 else "WARNING" if mr >= 85 else "FAIL"
    em = ":white_check_mark:" if st == "PASS" else ":warning:" if st == "WARNING" else ":red_circle:"
    msg = f"""{em} *Reebelo Routing Reconciliation — {dt}*\n\n*Status: {st}* | Match Rate: *{mr:.1f}%*\n\n• PBI: {r['totalPBI']} | Sheet: {r['totalSheet']} | Matched: {len(r['matches'])} | Mismatches: {len(r['mismatches'])}\n• OBR: {len(r['obrAlerts'])} | Not Registered: {len(nr)}\n• Missing Sheet: {len(r['missingFromSheet'])} | Missing PBI: {len(r['missingFromPBI'])}\n• *Total Issues: {ti}*"""
    if r["mismatches"]:
        msg += "\n\n*Top Mismatches:*"
        for m in r["mismatches"][:5]:
            msg += f"\n  `{m['imei']}` — PBI: {m['grade']} vs Sheet: {m['disp']}"
    if nr:
        msg += "\n\n*:rotating_light: Not Registered on Spreadsheet:*"
        for n in nr[:5]:
            msg += f"\n  `{n['imei']}` — PBI Grade: {n['grade']}"
    return msg

def _reebelo_send_slack(url, msg):
    import requests as req_lib
    if not url or "PASTE" in url:
        return False
    try:
        return req_lib.post(url, json={"text": msg}, timeout=10).status_code == 200
    except Exception:
        return False

def _reebelo_send_email(r, dt, cfg):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    if not cfg.get("email_enabled") or cfg.get("email_enabled") == "False":
        return False
    if not cfg.get("email_from") or not cfg.get("email_app_password"):
        return False
    nr = r.get("notRegistered", [])
    tc = r["totalCommon"] - len(r["obrAlerts"]) - len(nr)
    mr = (len(r["matches"]) / tc * 100) if tc > 0 else 0
    ti = len(r["mismatches"]) + len(r["missingFromSheet"]) + len(r["missingFromPBI"]) + len(nr)
    st = "PASS" if mr >= 95 else "WARNING" if mr >= 85 else "FAIL"
    sc = {"PASS": "#27AE60", "WARNING": "#F39C12", "FAIL": "#E74C3C"}[st]
    emoji = {"PASS": "\u2705", "WARNING": "\u26a0\ufe0f", "FAIL": "\U0001f534"}[st]

    mm = "".join(
        f"<tr><td style='padding:6px 10px;border-bottom:1px solid #eee'>{m['imei']}</td>"
        f"<td style='padding:6px 10px;border-bottom:1px solid #eee'>{m['grade']}</td>"
        f"<td style='padding:6px 10px;border-bottom:1px solid #eee'>{m['disp']}</td></tr>"
        for m in r["mismatches"][:10]
    )
    body = f"""<div style="font-family:Segoe UI,Arial,sans-serif;max-width:600px;margin:0 auto">
    <div style="background:linear-gradient(135deg,#1B2A4A,#2C3E6B);color:#fff;padding:20px 28px;border-radius:10px 10px 0 0">
    <h2 style="margin:0;font-size:18px">Reebelo Routing Reconciliation</h2>
    <p style="margin:4px 0 0;opacity:.8;font-size:13px">{dt}</p></div>
    <div style="background:#fff;padding:24px 28px;border:1px solid #e0e0e0;border-top:none;border-radius:0 0 10px 10px">
    <div style="display:inline-block;padding:6px 16px;border-radius:16px;background:{sc};color:#fff;font-weight:700;font-size:14px">{st} — {mr:.1f}%</div>
    <table style="width:100%;border-collapse:collapse;margin:16px 0;font-size:14px">
    <tr><td style="padding:6px 0;color:#555">PBI</td><td style="font-weight:600">{r['totalPBI']}</td>
    <td style="color:#555">Sheet</td><td style="font-weight:600">{r['totalSheet']}</td></tr>
    <tr><td style="padding:6px 0;color:#555">Matched</td><td style="font-weight:600;color:#27AE60">{len(r['matches'])}</td>
    <td style="color:#555">Mismatches</td><td style="font-weight:600;color:#E74C3C">{len(r['mismatches'])}</td></tr>
    <tr><td style="color:#555">OBR</td><td style="font-weight:600;color:#F39C12">{len(r['obrAlerts'])}</td>
    <td style="color:#555">Issues</td><td style="font-weight:600;color:#E74C3C">{ti}</td></tr></table>
    {"<h3 style='font-size:14px;margin:16px 0 8px'>Top Mismatches</h3><table style='width:100%;border-collapse:collapse;font-size:13px'><tr style='background:#f8f9fa'><th style='padding:8px;text-align:left'>IMEI</th><th style='padding:8px;text-align:left'>PBI</th><th style='padding:8px;text-align:left'>Sheet</th></tr>" + mm + "</table>" if r['mismatches'] else ""}
    </div></div>"""

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"{emoji} Reebelo Reconciliation — {dt} — {st} — {mr:.1f}%"
        msg["From"] = cfg["email_from"]
        msg["To"] = cfg.get("email_to", "")
        msg.attach(MIMEText(body, "html"))
        port = int(cfg.get("email_smtp_port", 587))
        with smtplib.SMTP(cfg.get("email_smtp_server", "smtp.gmail.com"), port) as s:
            s.starttls()
            s.login(cfg["email_from"], cfg["email_app_password"])
            s.send_message(msg)
        return True
    except Exception:
        return False

def _reebelo_generate_excel(r, dt):
    import pandas as pd
    output = io.BytesIO()
    nr = r.get("notRegistered", [])
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        tc = r["totalCommon"] - len(r["obrAlerts"]) - len(nr)
        mr = (len(r["matches"]) / tc * 100) if tc > 0 else 0
        ti = len(r["mismatches"]) + len(r["missingFromSheet"]) + len(r["missingFromPBI"]) + len(nr)
        summary = pd.DataFrame([
            {"Metric": "Report Date", "Value": dt},
            {"Metric": "PBI Records", "Value": r["totalPBI"]},
            {"Metric": "Sheet Records", "Value": r["totalSheet"]},
            {"Metric": "Matched", "Value": len(r["matches"])},
            {"Metric": "Mismatches", "Value": len(r["mismatches"])},
            {"Metric": "OBR Alerts", "Value": len(r["obrAlerts"])},
            {"Metric": "Not Registered", "Value": len(nr)},
            {"Metric": "Missing from Sheet", "Value": len(r["missingFromSheet"])},
            {"Metric": "Missing from PBI", "Value": len(r["missingFromPBI"])},
            {"Metric": "Total Issues", "Value": ti},
            {"Metric": "Match Rate", "Value": f"{mr:.1f}%"},
        ])
        summary.to_excel(writer, sheet_name="Summary", index=False)
        if r["mismatches"]:
            pd.DataFrame(r["mismatches"]).to_excel(writer, sheet_name="Route Mismatches", index=False)
        if r["obrAlerts"]:
            pd.DataFrame(r["obrAlerts"]).to_excel(writer, sheet_name="OBR Alerts", index=False)
        if nr:
            pd.DataFrame(nr).to_excel(writer, sheet_name="Not Registered", index=False)
        if r["missingFromSheet"]:
            pd.DataFrame(r["missingFromSheet"]).to_excel(writer, sheet_name="Missing from Sheet", index=False)
        if r["missingFromPBI"]:
            pd.DataFrame(r["missingFromPBI"]).to_excel(writer, sheet_name="Missing from PBI", index=False)
        if r["matches"]:
            pd.DataFrame(r["matches"]).to_excel(writer, sheet_name="Matched", index=False)
    return output.getvalue()

# In-memory store for last Reebelo Excel download
_reebelo_last_excel = b""

@app.route('/api/reebelo/reconcile', methods=['POST'])
def reebelo_reconcile():
    """Run Reebelo reconciliation on two uploaded files."""
    global _reebelo_last_excel
    try:
        if "file1" not in request.files or "file2" not in request.files:
            return jsonify({"error": "Need 2 files"}), 400

        f1 = request.files["file1"]
        f2 = request.files["file2"]

        pbi_df, sheet_df = None, None
        pbi_name, sheet_name = "", ""

        for fname, fobj in [(f1.filename, f1), (f2.filename, f2)]:
            data = fobj.read()
            df = _reebelo_parse_upload(data, fname)
            ft = _reebelo_detect_type(df)
            if ft == "pbi" and pbi_df is None:
                pbi_df, pbi_name = df, fname
            elif ft == "sheet" and sheet_df is None:
                sheet_df, sheet_name = df, fname

        if pbi_df is None or sheet_df is None:
            detected = []
            if pbi_df is not None: detected.append("PowerBI")
            if sheet_df is not None: detected.append("Sheet")
            return jsonify({"error": f"Could not detect file types. Detected: {', '.join(detected) or 'neither'}. Make sure one file has 'Internal Grade' column and the other has 'Disposition'."}), 400

        results = _reebelo_reconcile(pbi_df, sheet_df)
        if "error" in results:
            return jsonify(results), 400

        dt = date.today().isoformat()
        nr = results.get("notRegistered", [])
        tc = results["totalCommon"] - len(results["obrAlerts"]) - len(nr)
        mr = (len(results["matches"]) / tc * 100) if tc > 0 else 0

        # Save to database
        run_id = db.save_reebelo_run(results, mr)

        # Generate Excel
        try:
            _reebelo_last_excel = _reebelo_generate_excel(results, dt)
        except Exception as ex:
            print(f"[WARN] Reebelo Excel generation failed: {ex}")
            _reebelo_last_excel = b""

        # Send alerts
        cfg = db.get_reebelo_config()
        alerts = {"slack_channel": False, "slack_dm": False, "email": False}
        slack_msg = _reebelo_build_slack_msg(results, dt)

        try:
            if cfg.get("slack_webhook_channel") and "PASTE" not in cfg.get("slack_webhook_channel", ""):
                alerts["slack_channel"] = _reebelo_send_slack(cfg["slack_webhook_channel"], slack_msg)
            if cfg.get("slack_webhook_dm") and "PASTE" not in cfg.get("slack_webhook_dm", ""):
                alerts["slack_dm"] = _reebelo_send_slack(cfg["slack_webhook_dm"], slack_msg)
            if cfg.get("email_enabled") == "True":
                alerts["email"] = _reebelo_send_email(results, dt, cfg)
        except Exception as ex:
            print(f"[WARN] Reebelo alert sending failed: {ex}")

        results["alerts"] = alerts
        results["files"] = {"pbi": pbi_name, "sheet": sheet_name}
        results["date"] = dt
        results["run_id"] = run_id

        return jsonify(_reebelo_sanitize(results))

    except Exception as ex:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(ex)}"}), 500


@app.route('/api/reebelo/download')
def reebelo_download():
    """Download the last Reebelo reconciliation as Excel."""
    global _reebelo_last_excel
    if _reebelo_last_excel:
        return send_file(
            io.BytesIO(_reebelo_last_excel),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"Reebelo_Reconciliation_{date.today().isoformat()}.xlsx"
        )
    return jsonify({"error": "No report generated yet"}), 404


@app.route('/api/reebelo/trends')
def reebelo_trends():
    """Get Reebelo trend data."""
    return jsonify(_reebelo_sanitize(db.get_reebelo_trends()))


@app.route('/api/reebelo/detail/<path:query>')
def reebelo_detail(query):
    """Get Reebelo run detail by ID or date."""
    try:
        run_id = int(query)
        detail = db.get_reebelo_run_detail(run_id)
        if detail:
            return jsonify(_reebelo_sanitize(detail))
    except ValueError:
        pass
    # Try as date — not implemented for simplicity; return 404
    return jsonify({"error": "No detail data found"}), 404


@app.route('/api/reebelo/config', methods=['GET'])
def reebelo_get_config():
    """Get Reebelo alert config (passwords masked)."""
    cfg = db.get_reebelo_config()
    safe = {}
    for k, v in cfg.items():
        if "password" in k and v:
            safe[k] = "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022"
        else:
            safe[k] = v
    safe["slack_configured"] = bool(cfg.get("slack_webhook_channel")) and "PASTE" not in cfg.get("slack_webhook_channel", "")
    safe["email_configured"] = bool(cfg.get("email_enabled") == "True" and cfg.get("email_from") and cfg.get("email_app_password"))
    return jsonify(_reebelo_sanitize(safe))


@app.route('/api/reebelo/config', methods=['POST'])
def reebelo_save_config():
    """Save Reebelo alert config."""
    data = request.get_json()
    cfg = db.get_reebelo_config()
    for k, v in data.items():
        if "password" in k and v == "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022":
            continue
        db.set_reebelo_config(k, str(v))
    return jsonify({"status": "saved"})


# ════════════════════════════════════
# STARTUP
# ════════════════════════════════════

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    debug = os.environ.get('FLASK_ENV') != 'production'
    print("\n" + "=" * 56)
    print("  RecirQ Global — 3PL Reventory Server")
    print("=" * 56)
    print(f"  Database: {'PostgreSQL' if db.DATABASE_URL else 'SQLite (local)'}")
    print(f"  Open in your browser: http://localhost:{port}")
    print(f"  For other devices on your network, use:")
    print(f"    http://<this-computer-ip>:{port}")
    print("=" * 56 + "\n")
    app.run(host='0.0.0.0', port=port, debug=debug)
