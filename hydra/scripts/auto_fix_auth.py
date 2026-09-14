import os

home_dir = os.path.expanduser("~")
hydra_dir = os.path.join(home_dir, "hydra")
core_dir = os.path.join(hydra_dir, "core")

print("🛠️ Initiating Self-Healing Auth Protocol...")

# 1. Sanitize .env
env_path = os.path.join(hydra_dir, ".env")
env_content = """5PAISA_VENDOR_KEY=kiFauE4G3aPebavZqzaCDXL5ZqLWpcYC
5PAISA_ENCRYPTION_KEY=Z7w27HD7OGCobFo48ACHhoQEjqtXP6ra
5PAISA_USER_ID=Xl3r3TUSHdF
5PAISA_RESPONSE_URL=http://127.0.0.1:5000/callback/5paisa
REDIS_URL=redis://127.0.0.1:6379/0
HYDRA_TOKEN_ENCRYPTION_KEY=omFuXzjGba5TNe9eHfvKDZGp-pplrxnVbYS
DATABASE_URL=postgresql://localhost/hydra_db
"""
with open(env_path, "w") as f:
    f.write(env_content)
print("✅ .env vault sanitized.")

# 2. Inject Introspection Broker
broker_path = os.path.join(core_dir, "broker.py")
broker_content = """import logging
import sys
import os

# Ensure core is in the python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import auth_manager

logger = logging.getLogger("HydraBroker")

def get_live_broker():
    logger.info("🔐 Requesting live authenticated session from Auth Manager...")
    
    factory = None
    possible_names = ['get_client', 'login', 'authenticate', 'create_client', 'init_client', 'login_user']
    
    for func_name in possible_names:
        if hasattr(auth_manager, func_name):
            factory = getattr(auth_manager, func_name)
            logger.info(f"⚡ Auto-detected auth function: auth_manager.{func_name}()")
            break
            
    if not factory:
        available_funcs = [d for d in dir(auth_manager) if not d.startswith('__')]
        logger.critical(f"❌ Could not automatically find the login function! Found these instead: {available_funcs}")
        raise RuntimeError("Missing auth function in auth_manager.py")
        
    client = factory()
    logger.info("✅ 5paisa Live Session Established.")
    return client
"""
with open(broker_path, "w") as f:
    f.write(broker_content)
print("✅ Introspection Broker injected.")
