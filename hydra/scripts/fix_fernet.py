import os
import re
from cryptography.fernet import Fernet

env_path = os.path.expanduser("~/hydra/.env")

# Generate a mathematically perfect 44-character Base64 Fernet key
valid_key = Fernet.generate_key().decode()

with open(env_path, "r") as f:
    content = f.read()

# Replace the broken, truncated key with the valid one
new_content = re.sub(
    r"HYDRA_TOKEN_ENCRYPTION_KEY=.*", 
    f"HYDRA_TOKEN_ENCRYPTION_KEY={valid_key}", 
    content
)

with open(env_path, "w") as f:
    f.write(new_content)

print(f"✅ Injected valid 32-byte Fernet key: {valid_key}")
print("🚀 Launching Hydra...")
