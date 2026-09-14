#!/usr/bin/env python3
from pathlib import Path
import re
root=Path(__file__).resolve().parents[1]
html=(root/"dashboard/index.html").read_text()
js=(root/"dashboard/app.js").read_text()
pages=set(re.findall(r'id="page-([a-z0-9_-]+)"',html))
nav=set(re.findall(r'data-page="([a-z0-9_-]+)"',html))
actions=set(re.findall(r'data-action="([a-z0-9_-]+)"',html))
known={"refresh","export-trades","export-events","export-training","research-audit","oauth","live-check","reconcile"}
missing_pages=sorted(nav-pages)
missing_actions=sorted(actions-known)
assert not missing_pages, f"unmapped pages: {missing_pages}"
assert not missing_actions, f"unmapped actions: {missing_actions}"
for p in nav: assert f"pageMeta" in js and p in js, f"page metadata missing: {p}"
print(f"pages={len(pages)} nav_targets={len(nav)} actions={len(actions)} mapping=PASS")
