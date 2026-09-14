# HYDRA Ω-APEX Android

Native Android shell for the HYDRA Ω-APEX trading system.

## Safety
- PAPER mode is the default and shown as locked.
- No live order is sent by this Android shell.
- 5paisa fields are blank on first run and stored locally using Android Keystore AES-GCM.
- Default HYDRA backend is `http://127.0.0.1:8181`.
- The WebView opens the existing HYDRA dashboard; it does not fabricate market data.

## Build
The repository workflow installs Gradle 8.10.2 and Java 17, then runs `gradle assembleDebug` from this `android/` directory. No Gradle wrapper is required.

## Important integration boundary
Saving 5paisa credentials in the Android vault does not automatically inject them into the Termux `.env`. The existing HYDRA broker/OAuth integration remains the authority for broker authentication. This app intentionally does not claim that credential synchronization or live execution is implemented.
