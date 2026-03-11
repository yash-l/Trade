import os

target = os.path.expanduser("~/hydra/core/module7.py")

print("🎨 Overhauling Hydra UI with Spectre Design Language...")

spectre_ui = """
@app.route('/')
def dashboard():
    \"\"\"Serves the Hydra Dashboard with Spectre Old Money Design\"\"\"
    return '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>HYDRA | SPECTRE</title>
        <link href="https://fonts.googleapis.com/css2?family=Cinzel:wght@400;700&family=Lora:ital,wght@0,400;0,600&display=swap" rel="stylesheet">
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
        <style>
            :root {
                --bg: #F4F1EA; --sidebar-bg: #1A2F23; --surface: #FFFFFF;
                --text-main: #1C1C1C; --gold: #C5A059; --border: #D8D4C8;
            }
            body { background: var(--bg); color: var(--text-main); font-family: 'Lora', serif; margin: 0; display: flex; min-height: 100vh; }
            .sidebar { width: 260px; background: var(--sidebar-bg); border-right: 1px solid var(--gold); padding: 2rem 1.5rem; position: fixed; height: 100vh; color: #E0D8C8; }
            .brand { font-family: 'Cinzel', serif; font-size: 1.5rem; color: var(--gold); text-decoration: none; display: flex; align-items: center; margin-bottom: 3rem; }
            .nav-item { padding: 0.8rem; color: #E0D8C8; text-decoration: none; display: flex; align-items: center; gap: 15px; font-family: 'Cinzel', serif; font-size: 0.8rem; letter-spacing: 0.1em; }
            .main { flex: 1; margin-left: 260px; padding: 3rem 4rem; }
            h1 { font-family: 'Cinzel', serif; font-size: 2rem; color: var(--sidebar-bg); margin: 0; }
            .subtitle { color: #5A5A5A; font-size: 0.9rem; border-left: 3px solid var(--gold); padding-left: 1rem; margin-bottom: 2.5rem; font-style: italic; }
            .stat-card { background: var(--surface); border: 1px solid var(--border); padding: 1.5rem; position: relative; box-shadow: 0 4px 15px rgba(26, 47, 35, 0.08); }
            .stat-card::after { content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 3px; background: var(--sidebar-bg); }
            .stat-val { font-size: 2rem; color: var(--sidebar-bg); font-family: 'Cinzel', serif; }
            .btn { background: var(--sidebar-bg); color: var(--gold); padding: 14px 20px; font-family: 'Cinzel', serif; text-decoration: none; display: block; text-align: center; font-size: 0.75rem; font-weight: 700; letter-spacing: 0.1em; }
        </style>
    </head>
    <body>
        <div class="sidebar">
            <a href="/" class="brand"><i class="fas fa-chess-knight"></i><span>HYDRA</span></a>
            <div class="menu-label" style="font-size:0.6rem; opacity:0.5; letter-spacing:0.2em; margin-bottom:1rem;">OPERATIONS</div>
            <a href="/" class="nav-item"><i class="fas fa-columns"></i><span>DASHBOARD</span></a>
            <a href="/login" class="nav-item"><i class="fas fa-key"></i><span>AUTH VAULT</span></a>
        </div>
        <div class="main">
            <h1>Mission Control</h1>
            <div class="subtitle">TRADING ENGINE INFRASTRUCTURE</div>
            <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 1.5rem;">
                <div class="stat-card">
                    <div style="font-size:0.65rem; font-family:Cinzel; letter-spacing:0.1em;">SYSTEM STATE</div>
                    <div class="stat-val">OPERATIONAL</div>
                </div>
                <div class="stat-card">
                    <div style="font-size:0.65rem; font-family:Cinzel; letter-spacing:0.1em;">MARKET SESSION</div>
                    <div class="stat-val">CLOSED</div>
                </div>
            </div>
            <div style="margin-top:2.5rem; max-width: 400px;">
                <a href="/login" class="btn">RE-AUTHORIZE TERMINAL</a>
            </div>
        </div>
    </body>
    </html>
    '''
"""

try:
    with open(target, 'r') as f:
        content = f.read()

    # Find the existing dashboard and replace it
    if "@app.route('/')" in content:
        import re
        content = re.sub(r"@app\.route\('\/'\).*?'''", spectre_ui, content, flags=re.DOTALL)
        with open(target, 'w') as f:
            f.write(content)
        print("✅ Hydra Dashboard successfully updated with Spectre designs!")
except Exception as e:
    print(f"❌ Error: {e}")
