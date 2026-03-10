import os
import sys
import time
import subprocess

MODULES = [
    "../core/module1.py",
    "../core/module2.py",
    "../core/module3.py",
    "../core/module4.py",
    "../core/module5.py",
    "../core/module6.py",
    "../core/module7.py"  # master_control.py equivalent
]

processes = []

if __name__ == "__main__":
    print("🚀 Booting Hydra Subsystems...")
    try:
        for mod in MODULES:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            mod_path = os.path.normpath(os.path.join(script_dir, mod))
            
            if os.path.exists(mod_path):
                print(f"Starting {os.path.basename(mod_path)}")
                p = subprocess.Popen([sys.executable, mod_path])
                processes.append(p)
                time.sleep(1.2) # Stagger boot sequence
            else:
                print(f"⚠️ Warning: {os.path.basename(mod_path)} not found.")
                
        print("\n✅ System Online. Press CTRL+C to safely exit.\n")
        
        for p in processes:
            p.wait()
            
    except KeyboardInterrupt:
        print("\n[SYSTEM] CTRL+C Detected. Initiating safe cascade shutdown...")
        for p in processes:
            try: p.terminate()
            except: pass
        
        time.sleep(1.5)
        print("✅ Hydra successfully powered down.")
        sys.exit(0)
