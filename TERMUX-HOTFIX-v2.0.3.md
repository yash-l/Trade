# HYDRA v2.0.4 Termux SQLite Hotfix

- Removes `/tmp` from self-test; Termux does not guarantee a Linux-style `/tmp`.
- Anchors relative HYDRA database paths to the application root.
- Writes default DB to `<app>/data/hydra.sqlite3`.
- Adds an SQLite write probe during installation.
- Installer exits non-zero on any failed self-test.
- Journal now reports the exact DB path on open failure.
- Retains v2.0.2 timezone fallback and truthful health-checked startup.
