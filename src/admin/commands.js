import {
  buildTotpAuthUri,
  formatTotpSecret,
  generateTotpSecret,
  normalizeTotpCode,
  verifyTotpCode
} from "../utilities/totp.js";

const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const TOTP_ATTEMPT_WINDOW_MS = 5 * 60 * 1000;
const TOTP_ATTEMPT_MAX_ATTEMPTS = 6;
const TOTP_ATTEMPT_BLOCK_MS = 10 * 60 * 1000;
const totpAttemptBuckets = new Map();

function pruneTotpAttemptBuckets(nowMs) {
  if (totpAttemptBuckets.size <= 5000) {
    return;
  }

  for (const [key, bucket] of totpAttemptBuckets.entries()) {
    if (nowMs >= bucket.windowUntil && (!bucket.blockedUntil || nowMs >= bucket.blockedUntil)) {
      totpAttemptBuckets.delete(key);
    }
  }
}

function getTotpAttemptBucket(key) {
  const nowMs = Date.now();
  pruneTotpAttemptBuckets(nowMs);

  const current = totpAttemptBuckets.get(key);
  if (!current || nowMs >= current.windowUntil) {
    const created = {
      attempts: 0,
      windowUntil: nowMs + TOTP_ATTEMPT_WINDOW_MS,
      blockedUntil: 0
    };
    totpAttemptBuckets.set(key, created);
    return created;
  }

  if (current.blockedUntil && nowMs >= current.blockedUntil) {
    current.attempts = 0;
    current.windowUntil = nowMs + TOTP_ATTEMPT_WINDOW_MS;
    current.blockedUntil = 0;
  }

  return current;
}

function getTotpRetryAfterSeconds(key) {
  const bucket = getTotpAttemptBucket(key);
  const nowMs = Date.now();
  if (bucket.blockedUntil && nowMs < bucket.blockedUntil) {
    return Math.max(1, Math.ceil((bucket.blockedUntil - nowMs) / 1000));
  }

  return 0;
}

function registerTotpFailure(key) {
  const bucket = getTotpAttemptBucket(key);
  bucket.attempts += 1;

  if (bucket.attempts >= TOTP_ATTEMPT_MAX_ATTEMPTS) {
    bucket.blockedUntil = Date.now() + TOTP_ATTEMPT_BLOCK_MS;
  }
}

function clearTotpFailures(key) {
  totpAttemptBuckets.delete(key);
}

function buildTotpAuthorizationState(record, authWindowDays) {
  const result = {
    enrolled: Boolean(record?.enabled && record?.secret_base32),
    authorized: false,
    last_verified_at: record?.last_verified_at ? String(record.last_verified_at) : null,
    expires_at: null,
    remaining_days: 0
  };

  if (!result.enrolled || !result.last_verified_at) {
    return result;
  }

  const lastVerifiedAtMs = Date.parse(result.last_verified_at);
  if (!Number.isFinite(lastVerifiedAtMs)) {
    return result;
  }

  const ttlMs = Math.max(1, Number(authWindowDays || 30)) * ONE_DAY_MS;
  const expiresAtMs = lastVerifiedAtMs + ttlMs;
  const remainingMs = expiresAtMs - Date.now();

  result.expires_at = new Date(expiresAtMs).toISOString();
  result.authorized = remainingMs > 0;
  result.remaining_days = result.authorized ? Math.ceil(remainingMs / ONE_DAY_MS) : 0;

  return result;
}

async function trySendDirectMessage(user, content) {
  if (!user || typeof user.send !== "function") {
    return false;
  }

  try {
    await user.send(content);
    return true;
  } catch {
    return false;
  }
}

export function createAdminCommandHandlers({
  PermissionFlags,
  requirePermission,
  resolveGuildFromMessage,
  safeReply,
  db,
  parseSnowflake,
  config
}) {
  return {
    async serverconfig({ message }) {
      if (!(await requirePermission(message, PermissionFlags.Administrator))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        await safeReply(message, "This command only works in a server.");
        return;
      }

      const cfg = db.getGuildConfig(guild.id);
      await safeReply(
        message,
        [
          `guild_id: ${cfg.guild_id}`,
          `log_channel_id: ${cfg.log_channel_id || "-"}`,
          `welcome_channel_id: ${cfg.welcome_channel_id || "-"}`,
          `rules_channel_id: ${cfg.rules_channel_id || "-"}`,
          `chat_channel_id: ${cfg.chat_channel_id || "-"}`,
          `help_channel_id: ${cfg.help_channel_id || "-"}`,
          `about_channel_id: ${cfg.about_channel_id || "-"}`,
          `perks_channel_id: ${cfg.perks_channel_id || "-"}`,
          "leveling_enabled: true (always on)",
          `leveling_channel_id: ${cfg.leveling_channel_id || "-"}`,
          `admin_role_name: ${cfg.admin_role_name}`,
          `mod_role_name: ${cfg.mod_role_name}`,
          `verification_url: ${cfg.verification_url || "off"}`,
          "raid_detection_enabled: true (always on)",
          `totp_required_for_staff: ${config.totp.enabled}`,
          `totp_auth_window_days: ${config.totp.authWindowDays}`,
          `raid_gate_threshold: ${cfg.raid_gate_threshold}`,
          `raid_join_rate_threshold: ${cfg.raid_join_rate_threshold}`,
          `raid_monitor_window_seconds: ${cfg.raid_monitor_window_seconds}`,
          `gate_duration_seconds: ${cfg.gate_duration_seconds}`,
          `join_gate_mode: ${cfg.join_gate_mode}`
        ].join("\n")
      );
    },

    async setlogchannel({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.Administrator))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        return;
      }

      const value = String(args[0] || "").trim();
      const channelId = ["off", "none", "clear", "0", ""].includes(value.toLowerCase()) ? null : parseSnowflake(value);
      if (value && !channelId && !["off", "none", "clear", "0"].includes(value.toLowerCase())) {
        await safeReply(message, "Provide a valid channel ID or off.");
        return;
      }

      db.updateGuildConfig(guild.id, { log_channel_id: channelId });
      await safeReply(message, `Log channel ${channelId ? `set to ${channelId}` : "cleared"}.`);
    },

    async setwelcomechannel({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.Administrator))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        return;
      }

      const value = String(args[0] || "").trim();
      const channelId = ["off", "none", "clear", "0", ""].includes(value.toLowerCase()) ? null : parseSnowflake(value);
      if (value && !channelId && !["off", "none", "clear", "0"].includes(value.toLowerCase())) {
        await safeReply(message, "Provide a valid channel ID or off.");
        return;
      }

      db.updateGuildConfig(guild.id, { welcome_channel_id: channelId });
      await safeReply(message, `Welcome channel ${channelId ? `set to ${channelId}` : "cleared"}.`);
    },

    async setresourcechannels({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.Administrator))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        return;
      }

      if (args.length < 5) {
        await safeReply(message, "Usage: setresourcechannels <rules> <chat> <help> <about> <perks>");
        return;
      }

      const [rules, chat, help, about, perks] = args.map(parseSnowflake);
      if (!rules || !chat || !help || !about || !perks) {
        await safeReply(message, "All 5 channel IDs must be valid snowflakes.");
        return;
      }

      db.updateGuildConfig(guild.id, {
        rules_channel_id: rules,
        chat_channel_id: chat,
        help_channel_id: help,
        about_channel_id: about,
        perks_channel_id: perks
      });

      await safeReply(message, "Resource channels updated.");
    },

    async setroles({ message, body }) {
      if (!(await requirePermission(message, PermissionFlags.Administrator))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        return;
      }

      const payload = body.replace(/^setroles\s+/i, "").trim();
      if (!payload.includes("|")) {
        await safeReply(message, "Usage: setroles <AdminRoleName> | <ModRoleName>");
        return;
      }

      const [adminRole, modRole] = payload.split("|").map((entry) => entry.trim());
      if (!adminRole || !modRole) {
        await safeReply(message, "Both role names are required.");
        return;
      }

      db.updateGuildConfig(guild.id, {
        admin_role_name: adminRole,
        mod_role_name: modRole
      });

      await safeReply(message, `Role names updated: admin=${adminRole}, mod=${modRole}.`);
    },

    async setverificationurl({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.Administrator))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        return;
      }

      const value = args.join(" ").trim();
      const lowered = value.toLowerCase();
      if (["", "off", "none", "clear", "null"].includes(lowered)) {
        db.updateGuildConfig(guild.id, { verification_url: null });
        await safeReply(message, "Verification URL cleared. Manual review mode is active.");
        return;
      }

      if (!value.startsWith("http://") && !value.startsWith("https://")) {
        await safeReply(message, "Provide a full http:// or https:// URL, or off.");
        return;
      }

      db.updateGuildConfig(guild.id, { verification_url: value });
      await safeReply(message, "Verification URL updated.");
    },

    async setraidsettings({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.Administrator))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        return;
      }

      if (args.length < 5) {
        await safeReply(
          message,
          "Usage: setraidsettings <threshold> <join_rate_threshold> <window_seconds> <gate_duration_seconds> <timeout|kick>"
        );
        return;
      }

      const threshold = Math.max(0, Math.min(Number(args[0]), 1));
      const joinRateThreshold = Math.max(1, Math.min(Number(args[1]), 1000));
      const windowSeconds = Math.max(30, Math.min(Number(args[2]), 3600));
      const gateDurationSeconds = Math.max(60, Math.min(Number(args[3]), 86400));
      const mode = String(args[4] || "timeout").toLowerCase() === "kick" ? "kick" : "timeout";

      db.updateGuildConfig(guild.id, {
        raid_gate_threshold: threshold,
        raid_join_rate_threshold: joinRateThreshold,
        raid_monitor_window_seconds: windowSeconds,
        gate_duration_seconds: gateDurationSeconds,
        join_gate_mode: mode
      });

      await safeReply(
        message,
        `Raid settings updated: threshold=${threshold}, join_rate=${joinRateThreshold}, window=${windowSeconds}s, duration=${gateDurationSeconds}s, mode=${mode}.`
      );
    },

    async totpsetup({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.Administrator, undefined, { skipTotp: true }))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        await safeReply(message, "This command only works in a server.");
        return;
      }

      const userId = message.author?.id;
      if (!userId) {
        await safeReply(message, "Unable to resolve your account for TOTP setup.");
        return;
      }

      const rotate = ["rotate", "reset", "new"].includes(String(args[0] || "").trim().toLowerCase());
      let record = db.getStaffTotpAuth(guild.id, userId);

      if (!record || rotate) {
        record = db.upsertStaffTotpAuth({
          guildId: guild.id,
          userId,
          secretBase32: generateTotpSecret(config.totp.secretLength),
          enabled: true,
          lastVerifiedAt: null
        });
      }

      const accountName = `${message.author?.username || userId}@${guild.id}`;
      const otpAuthUri = buildTotpAuthUri({
        secretBase32: record.secret_base32,
        issuer: config.totp.issuer,
        accountName,
        digits: config.totp.codeDigits,
        periodSeconds: config.totp.periodSeconds,
        algorithm: "SHA1"
      });

      const dmSent = await trySendDirectMessage(
        message.author,
        [
          "Fluxer staff TOTP setup",
          `guild_id: ${guild.id}`,
          `issuer: ${config.totp.issuer}`,
          `secret: ${formatTotpSecret(record.secret_base32)}`,
          `otpauth_uri: ${otpAuthUri}`,
          "Add this to any authenticator app, then run: totpauth <6-digit-code>"
        ].join("\n")
      );

      if (!dmSent) {
        await safeReply(
          message,
          "I could not DM your TOTP setup details. Enable server DMs, then run totpsetup again.",
          { title: "TOTP", kind: "warning" }
        );
        return;
      }

      await safeReply(
        message,
        [
          `${rotate ? "TOTP secret rotated." : "TOTP secret ready."}`,
          "Setup details were sent to your DM.",
          `Use totpauth <code> to authorize staff commands for ${config.totp.authWindowDays} days.`
        ].join("\n"),
        { title: "TOTP", kind: "success" }
      );
    },

    async totpauth({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.Administrator, undefined, { skipTotp: true }))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        await safeReply(message, "This command only works in a server.");
        return;
      }

      const userId = message.author?.id;
      if (!userId) {
        await safeReply(message, "Unable to resolve your account for TOTP authorization.");
        return;
      }

      const rateLimitKey = `${guild.id}:${userId}`;
      const retryAfterSeconds = getTotpRetryAfterSeconds(rateLimitKey);
      if (retryAfterSeconds > 0) {
        await safeReply(message, `Too many invalid TOTP attempts. Retry in ${retryAfterSeconds}s.`, {
          title: "TOTP",
          kind: "warning"
        });
        return;
      }

      const code = normalizeTotpCode(args[0]);
      if (!code) {
        await safeReply(message, "Usage: totpauth <6-digit-code>", { title: "TOTP", kind: "warning" });
        return;
      }

      const record = db.getStaffTotpAuth(guild.id, userId);
      if (!record || !record.enabled || !record.secret_base32) {
        await safeReply(message, "TOTP is not set up yet. Run totpsetup first.", { title: "TOTP", kind: "warning" });
        return;
      }

      const valid = verifyTotpCode(record.secret_base32, code, {
        digits: config.totp.codeDigits,
        periodSeconds: config.totp.periodSeconds,
        windowSteps: config.totp.verifyWindowSteps,
        algorithm: "sha1"
      });

      if (!valid) {
        registerTotpFailure(rateLimitKey);
        const nextRetryAfter = getTotpRetryAfterSeconds(rateLimitKey);
        await safeReply(message, "Invalid TOTP code. Check your authenticator app time and try again.", {
          title: "TOTP",
          kind: "error"
        });

        if (nextRetryAfter > 0) {
          await safeReply(message, `Too many invalid TOTP attempts. Retry in ${nextRetryAfter}s.`, {
            title: "TOTP",
            kind: "warning"
          });
        }

        return;
      }

      clearTotpFailures(rateLimitKey);

      const updated = db.markStaffTotpVerified(guild.id, userId);
      const status = buildTotpAuthorizationState(updated, config.totp.authWindowDays);

      await safeReply(
        message,
        [
          "TOTP verified successfully.",
          `authorized: ${status.authorized}`,
          `expires_at: ${status.expires_at || "-"}`,
          `window_days: ${config.totp.authWindowDays}`
        ].join("\n"),
        { title: "TOTP", kind: "success" }
      );
    },

    async totpstatus({ message }) {
      if (!(await requirePermission(message, PermissionFlags.Administrator, undefined, { skipTotp: true }))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        await safeReply(message, "This command only works in a server.");
        return;
      }

      const userId = message.author?.id;
      if (!userId) {
        await safeReply(message, "Unable to resolve your account for TOTP status.");
        return;
      }

      const record = db.getStaffTotpAuth(guild.id, userId);
      const status = buildTotpAuthorizationState(record, config.totp.authWindowDays);

      await safeReply(
        message,
        [
          `enrolled: ${status.enrolled}`,
          `authorized: ${status.authorized}`,
          `last_verified_at: ${status.last_verified_at || "-"}`,
          `expires_at: ${status.expires_at || "-"}`,
          `remaining_days: ${status.remaining_days}`,
          `window_days: ${config.totp.authWindowDays}`
        ].join("\n"),
        { title: "TOTP", kind: status.authorized ? "success" : "warning" }
      );
    },

    async totplogout({ message }) {
      if (!(await requirePermission(message, PermissionFlags.Administrator, undefined, { skipTotp: true }))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        await safeReply(message, "This command only works in a server.");
        return;
      }

      const userId = message.author?.id;
      if (!userId) {
        await safeReply(message, "Unable to resolve your account for TOTP logout.");
        return;
      }

      const record = db.getStaffTotpAuth(guild.id, userId);
      if (!record) {
        await safeReply(message, "No TOTP enrollment found. Run totpsetup first.", { title: "TOTP", kind: "warning" });
        return;
      }

      db.clearStaffTotpVerification(guild.id, userId);
      await safeReply(message, "TOTP authorization cleared. Run totpauth <code> to authorize again.", {
        title: "TOTP",
        kind: "info"
      });
    },

    async setlevelingchannel({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.Administrator))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        return;
      }

      const value = String(args[0] || "").trim();
      const channelId = ["off", "none", "clear", "0", ""].includes(value.toLowerCase()) ? null : parseSnowflake(value);
      if (value && !channelId && !["off", "none", "clear", "0"].includes(value.toLowerCase())) {
        await safeReply(message, "Provide a valid channel ID or off.");
        return;
      }

      db.updateGuildConfig(guild.id, { leveling_channel_id: channelId });
      await safeReply(message, `Leveling channel ${channelId ? `set to ${channelId}` : "cleared"}.`);
    }
  };
}
