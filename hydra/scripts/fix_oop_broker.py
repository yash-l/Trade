import os

core_dir = os.path.expanduser("~/hydra/core")
broker_path = os.path.join(core_dir, "broker.py")

broker_content = """import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from auth_manager import HydraTokenManager

logger = logging.getLogger("HydraBroker")

def get_live_broker():
    logger.info("🔐 Booting HydraTokenManager Object...")
    try:
        # Instantiate your custom class
        manager = HydraTokenManager()
        
        # Hunt for the method that returns the 5paisa client
        methods = ['get_client', 'login', 'authenticate', 'create_session', 'init_client', 'get_session']
        for m in methods:
            if hasattr(manager, m):
                logger.info(f"⚡ Auto-detected OOP method: HydraTokenManager.{m}()")
                client = getattr(manager, m)()
                logger.info("✅ 5paisa Live Session Established.")
                return client
                
        # If it fails, print the available methods for debugging
        available = [d for d in dir(manager) if not d.startswith('_')]
        logger.critical(f"❌ No valid login method found in HydraTokenManager. Available: {available}")
        raise RuntimeError("Missing auth method")
        
    except Exception as e:
        logger.critical(f"❌ HydraTokenManager initialization failed: {e}")
        raise e
"""
with open(broker_path, "w") as f:
    f.write(broker_content)
    
print("✅ Broker upgraded for Object-Oriented Auth.")
