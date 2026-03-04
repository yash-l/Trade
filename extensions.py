import os
import logging
import redis
import psycopg2.pool
from cryptography.fernet import Fernet, MultiFernet
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from config import Config

logger = logging.getLogger("HydraBoot")

use_ssl = Config.REDIS_URL.startswith("rediss://")

redis_kwargs = {
    "decode_responses": True,
    "socket_timeout": 5,
    "socket_connect_timeout": 5,
    "retry_on_timeout": True
}

if use_ssl:
    redis_kwargs["ssl"] = True
    redis_kwargs["ssl_cert_reqs"] = "required"

redis_client = redis.from_url(Config.REDIS_URL, **redis_kwargs)
limiter = Limiter(key_func=get_remote_address, storage_uri=Config.REDIS_URL)

if not Config.TOKEN_ENCRYPTION_KEY:
    raise RuntimeError("FATAL: ACTIVE TOKEN_ENCRYPTION_KEY missing.")

active_key = Fernet(Config.TOKEN_ENCRYPTION_KEY)
keys = [active_key]
if Config.PREVIOUS_ENCRYPTION_KEY:
    keys.append(Fernet(Config.PREVIOUS_ENCRYPTION_KEY))

cipher_suite = MultiFernet(keys)

try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(1, 5, dsn=Config.DATABASE_URL)
    if db_pool:
        logger.info("PostgreSQL ThreadedConnectionPool initialized.")
except Exception as e:
    raise RuntimeError(f"FATAL: Database pool failed to initialize: {e}")
