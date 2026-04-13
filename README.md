# Fluxer Bot JS

JavaScript runtime rewrite of the Python bot, using Fluxer.js.

## Status

This folder provides a runnable JS bot with core parity for:

- Prefix command handling (`/` and `!` by default)
- Moderation commands (`kick`, `ban`, `unban`, `mute`, `unmute`, `warnings`)
- Server stats card commands (`serverstats`, `serverinfo`, `stats`)
- Always-on leveling with image rank cards inspired by Mee6 (`rank`, `level`, `leaderboard`)
- Security and join-gate flow (`setverificationurl`, `setraidsettings`, `raidgate`, `verifyjoin`, `rejectjoin`, `pendingverifications`, `raidsnapshot`)
- Staff TOTP flow for privileged commands (`totpsetup`, `totpauth`, `totpstatus`, `totplogout`) with 30-day reauthorization window
- Auto moderation for spam bursts, duplicate spam, mention/link spam, and automatic timeout actions
- Reaction role mapping and live role assignment on reaction add or remove
- Word blacklist loading and enforcement
- Optional Gemini-backed `ask` and `joke`
- Next.js dashboard (`web_dashboard_ts`) with Fluxer OAuth, shared-server control panel, command toggles, warning visibility, raid gate actions, and TOTP-gated protected writes

## Setup

1. Copy `.env.example` to `.env` and set `FLUXER_BOT_TOKEN`.
2. Install dependencies:

```bash
npm install
```

3. Start bot:

```bash
npm run start
```

## Web Dashboard (Dyno-style Control Panel)

From `bot_js`:

1. Configure dashboard environment:

```bash
copy ..\web_dashboard_ts\.env.example ..\web_dashboard_ts\.env
```

2. Install dashboard dependencies:

```bash
npm --prefix ..\web_dashboard_ts install
```

3. Run dashboard:

```bash
npm run dashboard:dev
```

The dashboard is available at `http://localhost:3000` by default.

For production and resale deployments, use PostgreSQL in `..\web_dashboard_ts\.env`.

## Notes

- Fluxer.js guides currently recommend `intents: 0`.
- In production, keep secrets in environment variables or a secret manager. Plaintext `token.txt` and `google.txt` fallbacks are intended for local development only.
- SQLite defaults to `./data/warnings.db`.
- Blacklist words are persisted in `data/words.json`.
- For `reactionroleadd` and `reactionroleremove`, use the actual emoji character (example: `🫡`) or custom emoji format (`<:name:id>`), not text aliases like `:saluting_face:`.

## Rust Raid ML Sidecar (Optional)

To run live raid scoring through Rust instead of in-process JS fallback:

1. Start the sidecar:

```bash
npm run ml:sidecar
```

Press Ctrl+C to stop the sidecar. The wrapper exits cleanly without Cargo STATUS_CONTROL_C_EXIT noise.

2. Start the bot in Rust mode (no manual env export needed):

```bash
npm run start:rust
```

Single-command deployment (build sidecar, start sidecar, and start bot together):

```bash
npm run start:rust:all
```

Press Ctrl+C once to stop both processes.

For watch mode:

```bash
npm run dev:rust
```

You can still override these values in `.env` if needed:

```bash
RAID_ML_BACKEND=rust
RAID_ML_SERVICE_URL=http://127.0.0.1:8787
```

When the sidecar is unavailable, the bot automatically falls back to the local JS raid model.
The bot also runs a periodic sidecar health check (`RAID_ML_HEALTH_CHECK_INTERVAL_MS`) and logs when the sidecar disconnects or reconnects.
