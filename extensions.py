import os
import logging
import redis
import psycopg2.pool
from cryptography.fernet import Fernet, MultiFernet
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from config import Config

logger = logging.getLogger("HydraBoot")

# --- Redis TLS & HA ---
use_ssl = Config.REDIS_URL.startswith("rediss://")
if Config.ENVIRONMENT == "production" and not use_ssl:
    raise RuntimeError("CRITICAL SRE HALT: Redis must use TLS (rediss://) in production.")

redis_client = redis.from_url(
    Config.REDIS_URL, decode_responses=True, socket_timeout=5, 
    socket_connect_timeout=5, retry_on_timeout=True,
    ssl=use_ssl, ssl_cert_reqs="required" if use_ssl else None
)

limiter = Limiter(key_func=get_remote_address, storage_uri=Config.REDIS_URL)

# --- MultiFernet Key Order ---
if not Config.TOKEN_ENCRYPTION_KEY:
    raise RuntimeError("FATAL: ACTIVE TOKEN_ENCRYPTION_KEY missing.")

active_key = Fernet(Config.TOKEN_ENCRYPTION_KEY)
keys = [active_key]
if Config.PREVIOUS_ENCRYPTION_KEY:
    keys.append(Fernet(Config.PREVIOUS_ENCRYPTION_KEY))

cipher_suite = MultiFernet(keys)

# --- PostgreSQL Tuned Pool ---
try:
    # Tuned for Gunicorn: 1 min connection, 5 max per worker.
    db_pool = psycopg2.pool.ThreadedConnectionPool(1, 5, dsn=Config.DATABASE_URL)
    if db_pool:
        logger.info("PostgreSQL ThreadedConnectionPool initialized.")
except Exception as e:
    raise RuntimeError(f"FATAL: Database pool failed to initialize: {e}")

