import subprocess
import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

modules = [
    "module1.py",
    "module2.py",
    "module3.py",
    "module4.py",
    "module5.py",
    "module6.py",
    "module7.py"
]

processes = []

for module in modules:
    path = os.path.join(BASE_DIR, "core", module)
    print(f"Starting {path}")
    p = subprocess.Popen([sys.executable, path])
    processes.append(p)

for p in processes:
    p.wait()
