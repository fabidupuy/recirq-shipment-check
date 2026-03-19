"""
RecirQ Global — Shipment Check Server
Flask web server that serves the Shipment Check app and persists data to SQLite.
"""
from flask import Flask, render_template, request, jsonify, send_from_directory
import database as db
import os
import json
import hashlib
import uuid

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
    """Serve the main application — read file directly to bypass Jinja parsing."""
    import pathlib
    html_path = pathlib.Path(__file__).parent / 'templates' / 'index.html'
    return html_path.read_text(encoding='utf-8'), 200, {'Content-Type': 'text/html; charset=utf-8'}


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
    batch_id = db.save_batch(data)
    return jsonify({'id': batch_id, 'status': 'saved'})


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
    """Get all Pick & Pack state (ppJobs, ppCompletedRMAs)."""
    state = db.get_all_pp_state()
    return jsonify(state)


@app.route('/api/pp/state/<key>', methods=['POST'])
def save_pp_state(key):
    """Save a Pick & Pack state value."""
    data = request.get_json()
    value_json = json.dumps(data.get('value', {}))
    db.save_pp_state(key, value_json)
    return jsonify({'status': 'saved', 'key': key})


# ════════════════════════════════════
# PHOTO UPLOAD API (S3)
# ════════════════════════════════════

@app.route('/api/photos/presign', methods=['POST'])
def get_presigned_upload_url():
    """Generate a presigned S3 URL for direct upload from the phone."""
    if not s3_client:
        return jsonify({'error': 'S3 not configured'}), 500
    data = request.get_json()
    imei = data.get('imei', 'unknown')
    photo_type = data.get('type', 'unit')  # 'unit' or 'box'
    vendor = data.get('vendor', '')
    file_ext = data.get('ext', 'jpg')
    photo_id = str(uuid.uuid4())[:8]

    # Organize by date/vendor/type
    from datetime import date
    today = date.today().isoformat()
    key = f"{today}/{vendor}/{photo_type}/{imei}_{photo_id}.{file_ext}"

    content_type = 'image/jpeg' if file_ext in ('jpg', 'jpeg') else f'image/{file_ext}'

    try:
        url = s3_client.generate_presigned_url('put_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': key,
                'ContentType': content_type,
            },
            ExpiresIn=600,  # 10 minutes
        )
        # Generate a presigned GET URL for viewing (works even if bucket is not public)
        view_url = s3_client.generate_presigned_url('get_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': key,
            },
            ExpiresIn=604800,  # 7 days
        )
        return jsonify({'uploadUrl': url, 'viewUrl': view_url, 'key': key, 'photoId': photo_id, 'contentType': content_type})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/photos/save', methods=['POST'])
def save_photo_refs():
    """Save photo references for a specific IMEI/box. Merges into existing ppPhotos state."""
    data = request.get_json()
    key = data.get('key', '')  # e.g., 'box-326134905' or '355606780524844'
    photos = data.get('photos', [])
    if not key or not photos:
        return jsonify({'error': 'key and photos required'}), 400

    # Merge into ppPhotos state
    state_json = db.get_pp_state('ppPhotos')
    all_photos = json.loads(state_json) if state_json else {}
    if key not in all_photos:
        all_photos[key] = []
    all_photos[key].extend(photos)
    db.save_pp_state('ppPhotos', json.dumps(all_photos))
    return jsonify({'status': 'saved', 'count': len(all_photos[key])})


@app.route('/api/photos/list/<key>', methods=['GET'])
def list_photos(key):
    """List all photos for an IMEI or box key."""
    state = db.get_pp_state('ppPhotos')
    if state:
        photos = json.loads(state)
        return jsonify(photos.get(key, []))
    return jsonify([])


@app.route('/api/photos/all', methods=['GET'])
def all_photos():
    """Get all photo references."""
    state = db.get_pp_state('ppPhotos')
    if state:
        return jsonify(json.loads(state))
    return jsonify({})


@app.route('/photo/<token>', methods=['GET'])
def photo_upload_page(token):
    """Serve the phone camera upload page."""
    return render_template('photo_upload.html', token=token)


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
