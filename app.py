import urllib.parse
import secrets
import logging
import json
import requests
import time
import os
import uuid
from datetime import datetime, timezone, timedelta
from flask import Flask, redirect, request, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix
from config import Config
from extensions import redis_client, limiter, db_pool
from auth_manager import HydraTokenManager

IST = timezone(timedelta(hours=5, minutes=30))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("HydraAPI")

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
limiter.init_app(app)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "operational", "environment": Config.ENVIRONMENT}), 200

@app.route('/login/5paisa')
@limiter.limit("5 per minute")
def initiate_5paisa_login():
    req_id = str(uuid.uuid4())
    state_token = secrets.token_urlsafe(32)
    client_ip = request.remote_addr 
    
    state_data = {
        "req_id": req_id,
        "created_at": time.time(),
        "ip": client_ip
    }
    
    redis_client.setex(f"oauth_state:{state_token}", 300, json.dumps(state_data))
    params = {'VendorKey': Config.API_KEY, 'ResponseURL': Config.CALLBACK_URL, 'State': state_token}
    logger.info(json.dumps({"req_id": req_id, "event": "LOGIN_INITIATED", "ip": client_ip}))
    return redirect(f"{Config.PAISA_OAUTH_URL}/WebVendorLogin/VLogin/Index?{urllib.parse.urlencode(params)}")

@app.route('/callback/5paisa')
def handle_5paisa_callback():
    request_token = request.args.get('RequestToken')
    returned_state = request.args.get('state')
    
    if not request_token or not returned_state:
        return jsonify({"error": "Malformed request"}), 400
        
    state_key = f"oauth_state:{returned_state}"
    raw_state_data = redis_client.get(state_key)
    
    if not raw_state_data:
        return jsonify({"error": "State missing or expired."}), 403
        
    state_data = json.loads(raw_state_data)
    req_id = state_data.get("req_id", "UNKNOWN")
    client_ip = request.remote_addr
    
    if state_data.get("ip") != client_ip:
        logger.critical(json.dumps({"req_id": req_id, "event": "AUTH_REPLAY_ATTEMPT", "request_ip": client_ip}))
        redis_client.delete(state_key)
        return jsonify({"error": "IP Mismatch."}), 403
        
    redis_client.delete(state_key)

    exchange_url = f"{Config.PAISA_API_URL}/VendorsAPI/Service1.svc/GetAccessToken"
    payload = {"head": {"Key": Config.API_KEY}, "body": {"RequestToken": request_token, "EncryKey": Config.ENCRYPTION_KEY, "UserId": Config.USER_ID}}
    
    try:
        response = requests.post(exchange_url, json=payload, headers={"Content-Type": "application/json"}, timeout=(3, 7))
        response.raise_for_status() 
        data = response.json()
        status_code = data.get('head', {}).get('Status')
        
        if status_code == 0:
            body = data['body']
            client_code = body.get('ClientCode')
            
            # SRE FIX: Validate Client Code Mapping if expected
            expected_code = state_data.get("expected_client_code")
            if expected_code and client_code != expected_code:
                logger.critical(json.dumps({"req_id": req_id, "event": "CLIENT_CODE_MISMATCH", "expected": expected_code, "received": client_code}))
                return jsonify({"error": "Client identity mismatch."}), 403

            metadata = {k: v for k, v in body.items() if k != "AccessToken"}
            metadata["login_timestamp"] = datetime.now(IST).isoformat()
            
            HydraTokenManager.save_session(db_pool, client_code, body['AccessToken'], metadata, req_id)
            
            logger.info(json.dumps({"req_id": req_id, "event": "AUTH_SUCCESS", "client": client_code, "segments_count": len(metadata)}))
            return jsonify({"status": "Trading engine primed."})
            
        elif status_code == 2:
            logger.error(json.dumps({"req_id": req_id, "event": "EXCHANGE_FAILED", "reason": "Invalid credentials"}))
            return jsonify({"error": "Authentication mismatch."}), 401
            
        elif status_code == 9:
            logger.critical(json.dumps({"req_id": req_id, "event": "EXCHANGE_FAILED", "reason": "Invalid Session generated"}))
            return jsonify({"error": "5paisa Session Invalidated."}), 401
            
        else:
            logger.error(json.dumps({"req_id": req_id, "event": "EXCHANGE_FAILED", "status_code": status_code}))
            return jsonify({"error": f"API Error: {status_code}"}), 502
            
    except requests.exceptions.Timeout:
        logger.critical(json.dumps({"req_id": req_id, "event": "NETWORK_TIMEOUT"}))
        return jsonify({"error": "Upstream Gateway Timeout"}), 504
    except requests.exceptions.RequestException as e:
        logger.critical(json.dumps({"req_id": req_id, "event": "NETWORK_FAILURE", "error": str(e)}))
        return jsonify({"error": "Internal Communication Error"}), 502

if __name__ == '__main__':
    # Local Dev execution only. Production uses Procfile.
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
