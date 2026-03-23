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
