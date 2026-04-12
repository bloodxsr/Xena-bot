# Fluxer Bot JS

JavaScript runtime rewrite of the Python bot, using Fluxer.js.

## Status

This folder provides a runnable JS bot with core parity for:

- Prefix command handling (`/` and `!` by default)
- Moderation commands (`kick`, `ban`, `unban`, `mute`, `unmute`, `warnings`)
- Security and join-gate flow (`setverificationurl`, `setraidsettings`, `raidgate`, `verifyjoin`, `rejectjoin`, `pendingverifications`, `raidsnapshot`)
- Staff TOTP flow for privileged commands (`totpsetup`, `totpauth`, `totpstatus`, `totplogout`) with 30-day reauthorization window
- Auto moderation for spam bursts, duplicate spam, mention/link spam, and automatic timeout actions
- Reaction role mapping and live role assignment on reaction add or remove
- Word blacklist loading and enforcement
- Optional Gemini-backed `ask` and `joke`

## Setup

1. Copy `.env.example` to `.env` and set `FLUXER_BOT_TOKEN` (or create `bot_js/token.txt`).
2. Install dependencies:

```bash
npm install
```

3. Start bot:

```bash
npm run start
```

## Notes

- Fluxer.js guides currently recommend `intents: 0`.
- SQLite defaults to `./data/warnings.db`.
- Blacklist words are persisted in `data/words.json`.
- For `reactionroleadd` and `reactionroleremove`, use the actual emoji character (example: `🫡`) or custom emoji format (`<:name:id>`), not text aliases like `:saluting_face:`.

## Rust Raid ML Sidecar (Optional)

To run live raid scoring through Rust instead of in-process JS fallback:

1. Start the sidecar:

```bash
npm run ml:sidecar
```

2. Start the bot in Rust mode (no manual env export needed):

```bash
npm run start:rust
```

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
