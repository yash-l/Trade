import logging
import json
from datetime import datetime, timedelta, timezone
from psycopg2.extras import RealDictCursor
from extensions import cipher_suite, redis_client

IST = timezone(timedelta(hours=5, minutes=30))
logger = logging.getLogger("HydraAuth")

class AuthenticationExpiredError(Exception): pass

class HydraTokenManager:
    
    @staticmethod
    def calculate_midnight_expiry() -> datetime:
        now_ist = datetime.now(IST)
        tomorrow = now_ist.date() + timedelta(days=1)
        return datetime.combine(tomorrow, datetime.min.time(), tzinfo=IST) - timedelta(seconds=5)

    @staticmethod
    def save_session(db_pool, client_code: str, access_token: str, metadata: dict, req_id: str):
        expiry_time = HydraTokenManager.calculate_midnight_expiry()
        encrypted_token = cipher_suite.encrypt(access_token.encode('utf-8')).decode('utf-8')
        
        conn = db_pool.getconn()
        try:
            conn.autocommit = False 
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO auth_sessions (client_code, token, expiry, metadata) 
                    VALUES (%s, %s, %s, %s) 
                    ON CONFLICT (client_code) 
                    DO UPDATE SET token = EXCLUDED.token, expiry = EXCLUDED.expiry, metadata = EXCLUDED.metadata;
                """, (client_code, encrypted_token, expiry_time, json.dumps(metadata)))
            conn.commit()
            logger.info(json.dumps({"req_id": req_id, "event": "AUTH_PERSISTED", "client": client_code, "expiry_ist": expiry_time.isoformat()}))
        except Exception as e:
            conn.rollback()
            logger.critical(json.dumps({"req_id": req_id, "event": "DB_PERSIST_FAILED", "error": str(e)}))
            raise
        finally:
            db_pool.putconn(conn)

    @staticmethod
    def get_decrypted_token(db_pool, client_code: str, req_id: str) -> str:
        conn = db_pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT token, expiry FROM auth_sessions WHERE client_code = %s", (client_code,))
                record = cursor.fetchone()
                
            if not record:
                raise AuthenticationExpiredError("No session found in DB.")
                
            token = record['token']
            expiry = record['expiry']
            
            if datetime.now(IST) >= expiry:
                HydraTokenManager.trigger_async_reauth(client_code, "EXPIRED_AT_RETRIEVAL", req_id)
                raise AuthenticationExpiredError("Session expired absolute limit.")
                
            return cipher_suite.decrypt(token.encode('utf-8')).decode('utf-8')
        finally:
            db_pool.putconn(conn)

    @staticmethod
    def trigger_async_reauth(client_code: str, reason: str, req_id: str):
        lock_key = f"reauth_lock:{client_code}"
        if redis_client.set(lock_key, "locked", ex=30, nx=True):
            payload = json.dumps({"req_id": req_id, "client_code": client_code, "timestamp": datetime.now(IST).isoformat(), "reason": reason})
            redis_client.lpush("hydra:reauth_queue", payload)
            logger.critical(json.dumps({"req_id": req_id, "event": "SESSION_INVALIDATED_QUEUE_PUSH", "client": client_code, "reason": reason}))
