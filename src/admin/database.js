import Database from "better-sqlite3";

function nowIso() {
  return new Date().toISOString();
}

function toSnowflakeText(value) {
  if (value == null) {
    return null;
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  const text = String(value).trim();
  return /^\d{5,22}$/.test(text) ? text : null;
}

function toInteger(value, fallback = 0) {
  if (value == null) {
    return fallback;
  }

  if (typeof value === "bigint") {
    return Number(value);
  }

  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }

  return Math.trunc(numeric);
}

function toFloat(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function toBoolean(value) {
  return toInteger(value, 0) !== 0;
}

function parseMetadata(value) {
  if (value == null) {
    return null;
  }

  const text = String(value).trim();
  if (!text) {
    return null;
  }

  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

function xpRequiredForNextLevel(level) {
  const normalized = Math.max(0, toInteger(level, 0));
  return 5 * normalized * normalized + 50 * normalized + 100;
}

function totalXpForLevel(level) {
  const targetLevel = Math.max(0, toInteger(level, 0));
  let total = 0;
  for (let current = 0; current < targetLevel; current += 1) {
    total += xpRequiredForNextLevel(current);
  }
  return total;
}

function levelFromTotalXp(totalXp) {
  let level = 0;
  let remainingXp = Math.max(0, toInteger(totalXp, 0));

  while (remainingXp >= xpRequiredForNextLevel(level) && level < 1000) {
    remainingXp -= xpRequiredForNextLevel(level);
    level += 1;
  }

  return level;
}

function buildLevelProgress(totalXp, level) {
  const normalizedXp = Math.max(0, toInteger(totalXp, 0));
  const normalizedLevel = Math.max(0, toInteger(level, 0));
  const currentLevelXp = totalXpForLevel(normalizedLevel);
  const nextLevelXp = totalXpForLevel(normalizedLevel + 1);
  const progressXp = Math.max(0, normalizedXp - currentLevelXp);
  const progressRequired = Math.max(1, nextLevelXp - currentLevelXp);
  const progressPercent = Math.max(0, Math.min(Math.round((progressXp / progressRequired) * 100), 100));

  return {
    currentLevelXp,
    nextLevelXp,
    progressXp,
    progressRequired,
    progressPercent
  };
}

export class BotDatabase {
  constructor(dbPath) {
    this.db = new Database(dbPath);
    this.db.defaultSafeIntegers(true);
    this.db.pragma("journal_mode = WAL");
    this.initialize();
  }

  initialize() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS warnings (
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        warning_count INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id)
      );

      CREATE TABLE IF NOT EXISTS warning_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        actor_user_id INTEGER,
        reason TEXT,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS moderation_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        actor_user_id INTEGER,
        target_user_id INTEGER,
        action TEXT NOT NULL,
        reason TEXT,
        channel_id INTEGER,
        message_id INTEGER,
        metadata TEXT,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS guild_config (
        guild_id INTEGER PRIMARY KEY,
        log_channel_id INTEGER,
        welcome_channel_id INTEGER,
        rules_channel_id INTEGER,
        chat_channel_id INTEGER,
        help_channel_id INTEGER,
        about_channel_id INTEGER,
        perks_channel_id INTEGER,
        leveling_channel_id INTEGER,
        admin_role_name TEXT NOT NULL DEFAULT 'Admin',
        mod_role_name TEXT NOT NULL DEFAULT 'Moderator',
        sync_mode TEXT NOT NULL DEFAULT 'global',
        sync_guild_id INTEGER,
        verification_url TEXT,
        leveling_enabled INTEGER NOT NULL DEFAULT 1,
        raid_detection_enabled INTEGER NOT NULL DEFAULT 1,
        raid_gate_threshold REAL NOT NULL DEFAULT 0.72,
        raid_monitor_window_seconds INTEGER NOT NULL DEFAULT 90,
        raid_join_rate_threshold INTEGER NOT NULL DEFAULT 8,
        gate_duration_seconds INTEGER NOT NULL DEFAULT 900,
        join_gate_mode TEXT NOT NULL DEFAULT 'timeout'
      );

      CREATE TABLE IF NOT EXISTS raid_state (
        guild_id INTEGER PRIMARY KEY,
        gate_active INTEGER NOT NULL DEFAULT 0,
        gate_reason TEXT,
        gate_until TEXT,
        updated_at TEXT
      );

      CREATE TABLE IF NOT EXISTS verification_queue (
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        status TEXT NOT NULL,
        risk_score REAL NOT NULL,
        verification_url TEXT,
        reason TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        verified_by_user_id INTEGER,
        PRIMARY KEY (guild_id, user_id)
      );

      CREATE TABLE IF NOT EXISTS join_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        account_age_days REAL NOT NULL,
        has_avatar INTEGER NOT NULL,
        profile_score REAL NOT NULL,
        join_rate REAL NOT NULL,
        young_account_ratio REAL NOT NULL,
        risk_score REAL NOT NULL,
        risk_level TEXT NOT NULL,
        action TEXT NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS reaction_roles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        channel_id INTEGER NOT NULL,
        message_id INTEGER NOT NULL,
        emoji_key TEXT NOT NULL,
        emoji_display TEXT NOT NULL,
        role_id INTEGER NOT NULL,
        created_by_user_id INTEGER,
        created_at TEXT NOT NULL,
        UNIQUE (guild_id, channel_id, message_id, emoji_key, role_id)
      );

      CREATE TABLE IF NOT EXISTS member_levels (
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        xp INTEGER NOT NULL DEFAULT 0,
        level INTEGER NOT NULL DEFAULT 0,
        message_count INTEGER NOT NULL DEFAULT 0,
        last_xp_at TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id)
      );

      CREATE TABLE IF NOT EXISTS staff_totp (
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        secret_base32 TEXT NOT NULL,
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        last_verified_at TEXT,
        PRIMARY KEY (guild_id, user_id)
      );

      CREATE INDEX IF NOT EXISTS idx_verification_queue_pending
      ON verification_queue (guild_id, status, updated_at);

      CREATE INDEX IF NOT EXISTS idx_join_events_lookup
      ON join_events (guild_id, id);

      CREATE INDEX IF NOT EXISTS idx_reaction_roles_lookup
      ON reaction_roles (guild_id, channel_id, message_id, emoji_key);

      CREATE INDEX IF NOT EXISTS idx_member_levels_rank
      ON member_levels (guild_id, level DESC, xp DESC, message_count DESC, user_id ASC);

      CREATE INDEX IF NOT EXISTS idx_staff_totp_last_verified
      ON staff_totp (guild_id, last_verified_at);
    `);

    this.ensureTableColumn("guild_config", "leveling_enabled", "INTEGER NOT NULL DEFAULT 1");
    this.ensureTableColumn("guild_config", "leveling_channel_id", "INTEGER");
    this.db.exec("UPDATE guild_config SET leveling_enabled = 1 WHERE leveling_enabled IS NULL OR leveling_enabled != 1");
    this.db.exec("UPDATE guild_config SET raid_detection_enabled = 1 WHERE raid_detection_enabled IS NULL OR raid_detection_enabled != 1");
  }

  ensureTableColumn(tableName, columnName, columnDefinition) {
    const rows = this.db.prepare(`PRAGMA table_info(${tableName})`).all();
    const exists = rows.some((row) => String(row?.name || "").toLowerCase() === columnName.toLowerCase());

    if (!exists) {
      this.db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnDefinition}`);
    }
  }

  ensureGuildConfig(guildId) {
    const id = toSnowflakeText(guildId);
    if (!id) {
      throw new Error("invalid guild id");
    }

    this.db.prepare("INSERT OR IGNORE INTO guild_config (guild_id) VALUES (?)").run(id);
  }

  getGuildConfig(guildId) {
    this.ensureGuildConfig(guildId);

    const row = this.db.prepare("SELECT * FROM guild_config WHERE guild_id = ?").get(guildId);
    if (!row) {
      throw new Error("missing guild config row");
    }

    return {
      guild_id: toSnowflakeText(row.guild_id),
      log_channel_id: toSnowflakeText(row.log_channel_id),
      welcome_channel_id: toSnowflakeText(row.welcome_channel_id),
      rules_channel_id: toSnowflakeText(row.rules_channel_id),
      chat_channel_id: toSnowflakeText(row.chat_channel_id),
      help_channel_id: toSnowflakeText(row.help_channel_id),
      about_channel_id: toSnowflakeText(row.about_channel_id),
      perks_channel_id: toSnowflakeText(row.perks_channel_id),
      leveling_channel_id: toSnowflakeText(row.leveling_channel_id),
      admin_role_name: String(row.admin_role_name || "Admin"),
      mod_role_name: String(row.mod_role_name || "Moderator"),
      sync_mode: String(row.sync_mode || "global"),
      sync_guild_id: toSnowflakeText(row.sync_guild_id),
      verification_url: row.verification_url == null ? null : String(row.verification_url),
      leveling_enabled: true,
      raid_detection_enabled: true,
      raid_gate_threshold: toFloat(row.raid_gate_threshold, 0.72),
      raid_monitor_window_seconds: toInteger(row.raid_monitor_window_seconds, 90),
      raid_join_rate_threshold: toInteger(row.raid_join_rate_threshold, 8),
      gate_duration_seconds: toInteger(row.gate_duration_seconds, 900),
      join_gate_mode: String(row.join_gate_mode || "timeout") === "kick" ? "kick" : "timeout"
    };
  }

  updateGuildConfig(guildId, updates) {
    const allowed = new Set([
      "log_channel_id",
      "welcome_channel_id",
      "rules_channel_id",
      "chat_channel_id",
      "help_channel_id",
      "about_channel_id",
      "perks_channel_id",
      "leveling_channel_id",
      "admin_role_name",
      "mod_role_name",
      "sync_mode",
      "sync_guild_id",
      "verification_url",
      "raid_gate_threshold",
      "raid_monitor_window_seconds",
      "raid_join_rate_threshold",
      "gate_duration_seconds",
      "join_gate_mode"
    ]);

    const entries = Object.entries(updates).filter(([key]) => allowed.has(key));
    if (entries.length === 0) {
      return this.getGuildConfig(guildId);
    }

    this.ensureGuildConfig(guildId);

    const normalized = {};
    for (const [key, rawValue] of entries) {
      if (key.endsWith("_channel_id") || key === "sync_guild_id") {
        normalized[key] = toSnowflakeText(rawValue);
        continue;
      }

      if (key === "raid_gate_threshold") {
        normalized[key] = Math.max(0, Math.min(Number(rawValue), 1));
        continue;
      }

      if (["raid_monitor_window_seconds", "raid_join_rate_threshold", "gate_duration_seconds"].includes(key)) {
        normalized[key] = Math.max(1, toInteger(rawValue, 1));
        continue;
      }

      if (key === "join_gate_mode") {
        normalized[key] = String(rawValue || "timeout").toLowerCase() === "kick" ? "kick" : "timeout";
        continue;
      }

      if (key === "verification_url") {
        const value = String(rawValue ?? "").trim();
        normalized[key] = value === "" ? null : value;
        continue;
      }

      normalized[key] = rawValue;
    }

    const fields = Object.keys(normalized);
    const sql = `UPDATE guild_config SET ${fields.map((key) => `${key} = @${key}`).join(", ")} WHERE guild_id = @guild_id`;
    this.db.prepare(sql).run({ guild_id: guildId, ...normalized });

    return this.getGuildConfig(guildId);
  }

  getStaffTotpAuth(guildId, userId) {
    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedUserId = toSnowflakeText(userId);

    if (!normalizedGuildId || !normalizedUserId) {
      return null;
    }

    const row = this.db
      .prepare(
        `
          SELECT guild_id, user_id, secret_base32, enabled, created_at, updated_at, last_verified_at
          FROM staff_totp
          WHERE guild_id = ? AND user_id = ?
        `
      )
      .get(normalizedGuildId, normalizedUserId);

    if (!row) {
      return null;
    }

    return {
      guild_id: toSnowflakeText(row.guild_id),
      user_id: toSnowflakeText(row.user_id),
      secret_base32: String(row.secret_base32 || ""),
      enabled: toBoolean(row.enabled),
      created_at: String(row.created_at || nowIso()),
      updated_at: String(row.updated_at || nowIso()),
      last_verified_at: row.last_verified_at == null ? null : String(row.last_verified_at)
    };
  }

  upsertStaffTotpAuth({ guildId, userId, secretBase32, enabled = true, lastVerifiedAt = null }) {
    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedUserId = toSnowflakeText(userId);
    const normalizedSecret = String(secretBase32 || "")
      .trim()
      .toUpperCase();

    if (!normalizedGuildId || !normalizedUserId || !normalizedSecret) {
      throw new Error("invalid totp enrollment payload");
    }

    const now = nowIso();
    this.db
      .prepare(
        `
          INSERT INTO staff_totp (
            guild_id, user_id, secret_base32, enabled, created_at, updated_at, last_verified_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(guild_id, user_id)
          DO UPDATE SET
            secret_base32 = excluded.secret_base32,
            enabled = excluded.enabled,
            updated_at = excluded.updated_at,
            last_verified_at = excluded.last_verified_at
        `
      )
      .run(
        normalizedGuildId,
        normalizedUserId,
        normalizedSecret,
        enabled ? 1 : 0,
        now,
        now,
        lastVerifiedAt == null ? null : String(lastVerifiedAt)
      );

    return this.getStaffTotpAuth(normalizedGuildId, normalizedUserId);
  }

  markStaffTotpVerified(guildId, userId, verifiedAt = null) {
    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedUserId = toSnowflakeText(userId);

    if (!normalizedGuildId || !normalizedUserId) {
      return null;
    }

    const when = verifiedAt == null ? nowIso() : String(verifiedAt);
    const now = nowIso();

    this.db
      .prepare(
        `
          UPDATE staff_totp
          SET enabled = 1, last_verified_at = ?, updated_at = ?
          WHERE guild_id = ? AND user_id = ?
        `
      )
      .run(when, now, normalizedGuildId, normalizedUserId);

    return this.getStaffTotpAuth(normalizedGuildId, normalizedUserId);
  }

  clearStaffTotpVerification(guildId, userId) {
    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedUserId = toSnowflakeText(userId);

    if (!normalizedGuildId || !normalizedUserId) {
      return null;
    }

    this.db
      .prepare(
        `
          UPDATE staff_totp
          SET last_verified_at = NULL, updated_at = ?
          WHERE guild_id = ? AND user_id = ?
        `
      )
      .run(nowIso(), normalizedGuildId, normalizedUserId);

    return this.getStaffTotpAuth(normalizedGuildId, normalizedUserId);
  }

  getWarningCount(guildId, userId) {
    const row = this.db
      .prepare("SELECT warning_count FROM warnings WHERE guild_id = ? AND user_id = ?")
      .get(guildId, userId);

    return row ? toInteger(row.warning_count, 0) : 0;
  }

  incrementWarning(guildId, userId, reason, actorUserId = null) {
    const now = nowIso();
    const current = this.getWarningCount(guildId, userId);
    const next = current + 1;

    this.db
      .prepare(
        `
          INSERT INTO warnings (guild_id, user_id, warning_count, updated_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(guild_id, user_id)
          DO UPDATE SET
            warning_count = excluded.warning_count,
            updated_at = excluded.updated_at
        `
      )
      .run(guildId, userId, next, now);

    this.db
      .prepare(
        `
          INSERT INTO warning_events (guild_id, user_id, actor_user_id, reason, created_at)
          VALUES (?, ?, ?, ?, ?)
        `
      )
      .run(guildId, userId, actorUserId, reason || null, now);

    return next;
  }

  resetWarnings(guildId, userId) {
    this.db.prepare("DELETE FROM warnings WHERE guild_id = ? AND user_id = ?").run(guildId, userId);
  }

  logModerationAction({
    guildId,
    action,
    actorUserId = null,
    targetUserId = null,
    reason = null,
    channelId = null,
    messageId = null,
    metadata = null
  }) {
    this.db
      .prepare(
        `
          INSERT INTO moderation_logs (
            guild_id, actor_user_id, target_user_id, action, reason,
            channel_id, message_id, metadata, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        `
      )
      .run(
        guildId,
        actorUserId,
        targetUserId,
        action,
        reason,
        channelId,
        messageId,
        metadata == null ? null : JSON.stringify(metadata),
        nowIso()
      );
  }

  upsertVerificationMember({
    guildId,
    userId,
    status,
    riskScore,
    verificationUrl = null,
    reason,
    verifiedByUserId = null
  }) {
    const now = nowIso();

    this.db
      .prepare(
        `
          INSERT INTO verification_queue (
            guild_id, user_id, status, risk_score, verification_url, reason,
            created_at, updated_at, verified_by_user_id
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(guild_id, user_id)
          DO UPDATE SET
            status = excluded.status,
            risk_score = excluded.risk_score,
            verification_url = excluded.verification_url,
            reason = excluded.reason,
            updated_at = excluded.updated_at,
            verified_by_user_id = excluded.verified_by_user_id
        `
      )
      .run(guildId, userId, status, riskScore, verificationUrl, reason, now, now, verifiedByUserId);
  }

  getVerificationStatus(guildId, userId) {
    const row = this.db
      .prepare(
        `
          SELECT status, risk_score, verification_url, reason, created_at, updated_at, verified_by_user_id
          FROM verification_queue
          WHERE guild_id = ? AND user_id = ?
        `
      )
      .get(guildId, userId);

    if (!row) {
      return null;
    }

    return {
      status: String(row.status || "pending"),
      risk_score: toFloat(row.risk_score, 0),
      verification_url: row.verification_url == null ? null : String(row.verification_url),
      reason: String(row.reason || ""),
      created_at: String(row.created_at || nowIso()),
      updated_at: String(row.updated_at || nowIso()),
      verified_by_user_id: toSnowflakeText(row.verified_by_user_id)
    };
  }

  isMemberPendingVerification(guildId, userId) {
    const status = this.getVerificationStatus(guildId, userId);
    return status != null && status.status === "pending";
  }

  listPendingVerifications(guildId, limit = 20) {
    const clamped = Math.max(1, Math.min(toInteger(limit, 20), 50));
    const rows = this.db
      .prepare(
        `
          SELECT user_id, status, risk_score, verification_url, reason, created_at, updated_at, verified_by_user_id
          FROM verification_queue
          WHERE guild_id = ? AND status = 'pending'
          ORDER BY updated_at DESC
          LIMIT ?
        `
      )
      .all(guildId, clamped);

    return rows.map((row) => ({
      user_id: toSnowflakeText(row.user_id),
      status: String(row.status || "pending"),
      risk_score: toFloat(row.risk_score, 0),
      verification_url: row.verification_url == null ? null : String(row.verification_url),
      reason: String(row.reason || ""),
      created_at: String(row.created_at || nowIso()),
      updated_at: String(row.updated_at || nowIso()),
      verified_by_user_id: toSnowflakeText(row.verified_by_user_id)
    }));
  }

  setRaidGateState(guildId, gateActive, reason = null, gateUntil = null) {
    this.db
      .prepare(
        `
          INSERT INTO raid_state (guild_id, gate_active, gate_reason, gate_until, updated_at)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(guild_id)
          DO UPDATE SET
            gate_active = excluded.gate_active,
            gate_reason = excluded.gate_reason,
            gate_until = excluded.gate_until,
            updated_at = excluded.updated_at
        `
      )
      .run(guildId, gateActive ? 1 : 0, reason, gateUntil, nowIso());
  }

  getRaidGateState(guildId) {
    const row = this.db
      .prepare("SELECT gate_active, gate_reason, gate_until, updated_at FROM raid_state WHERE guild_id = ?")
      .get(guildId);

    if (!row) {
      return {
        gate_active: false,
        gate_reason: null,
        gate_until: null,
        updated_at: null
      };
    }

    return {
      gate_active: toBoolean(row.gate_active),
      gate_reason: row.gate_reason == null ? null : String(row.gate_reason),
      gate_until: row.gate_until == null ? null : String(row.gate_until),
      updated_at: row.updated_at == null ? null : String(row.updated_at)
    };
  }

  logJoinEvent({
    guildId,
    userId,
    accountAgeDays,
    hasAvatar,
    profileScore,
    joinRate,
    youngAccountRatio,
    riskScore,
    riskLevel,
    action,
    metadata
  }) {
    this.db
      .prepare(
        `
          INSERT INTO join_events (
            guild_id, user_id, account_age_days, has_avatar, profile_score,
            join_rate, young_account_ratio, risk_score, risk_level, action, metadata, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `
      )
      .run(
        guildId,
        userId,
        accountAgeDays,
        hasAvatar ? 1 : 0,
        profileScore,
        joinRate,
        youngAccountRatio,
        riskScore,
        riskLevel,
        action,
        metadata == null ? null : JSON.stringify(metadata),
        nowIso()
      );
  }

  listRecentJoinEvents(guildId, limit = 20) {
    const clamped = Math.max(1, Math.min(toInteger(limit, 20), 100));
    const rows = this.db
      .prepare(
        `
          SELECT user_id, risk_score, risk_level, action, metadata, created_at
          FROM join_events
          WHERE guild_id = ?
          ORDER BY id DESC
          LIMIT ?
        `
      )
      .all(guildId, clamped);

    return rows.map((row) => ({
      user_id: toSnowflakeText(row.user_id),
      risk_score: toFloat(row.risk_score, 0),
      risk_level: String(row.risk_level || "unknown"),
      action: String(row.action || "allow"),
      metadata: parseMetadata(row.metadata),
      created_at: String(row.created_at || nowIso())
    }));
  }

  ensureMemberLevelRow(guildId, userId) {
    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedUserId = toSnowflakeText(userId);
    if (!normalizedGuildId || !normalizedUserId) {
      throw new Error("invalid guild id or user id");
    }

    this.db
      .prepare(
        `
          INSERT OR IGNORE INTO member_levels (guild_id, user_id, updated_at)
          VALUES (?, ?, ?)
        `
      )
      .run(normalizedGuildId, normalizedUserId, nowIso());
  }

  getMemberLevel(guildId, userId) {
    this.ensureMemberLevelRow(guildId, userId);

    const row = this.db
      .prepare(
        `
          SELECT guild_id, user_id, xp, level, message_count, last_xp_at, updated_at
          FROM member_levels
          WHERE guild_id = ? AND user_id = ?
        `
      )
      .get(guildId, userId);

    const xp = Math.max(0, toInteger(row?.xp, 0));
    const level = Math.max(0, toInteger(row?.level, levelFromTotalXp(xp)));
    const progress = buildLevelProgress(xp, level);

    return {
      guild_id: toSnowflakeText(row?.guild_id || guildId),
      user_id: toSnowflakeText(row?.user_id || userId),
      xp,
      level,
      message_count: Math.max(0, toInteger(row?.message_count, 0)),
      last_xp_at: row?.last_xp_at == null ? null : String(row.last_xp_at),
      updated_at: row?.updated_at == null ? null : String(row.updated_at),
      current_level_xp: progress.currentLevelXp,
      next_level_xp: progress.nextLevelXp,
      progress_xp: progress.progressXp,
      progress_required: progress.progressRequired,
      progress_percent: progress.progressPercent
    };
  }

  addMemberXp({ guildId, userId, xpGain, cooldownSeconds = 45 }) {
    this.ensureMemberLevelRow(guildId, userId);

    const now = new Date();
    const nowText = now.toISOString();
    const row = this.db
      .prepare(
        `
          SELECT xp, level, message_count, last_xp_at
          FROM member_levels
          WHERE guild_id = ? AND user_id = ?
        `
      )
      .get(guildId, userId);

    const previousXp = Math.max(0, toInteger(row?.xp, 0));
    const previousLevel = Math.max(0, toInteger(row?.level, levelFromTotalXp(previousXp)));
    const previousMessageCount = Math.max(0, toInteger(row?.message_count, 0));
    const cooldownMs = Math.max(0, toInteger(cooldownSeconds, 45)) * 1000;
    const lastXpAt = row?.last_xp_at == null ? null : String(row.last_xp_at);
    const lastXpAtMs = lastXpAt ? Date.parse(lastXpAt) : NaN;
    const cooldownActive =
      cooldownMs > 0 && Number.isFinite(lastXpAtMs) && now.getTime() - lastXpAtMs < cooldownMs;

    const appliedXp = cooldownActive ? 0 : Math.max(0, toInteger(xpGain, 0));
    const totalXp = previousXp + appliedXp;
    const level = levelFromTotalXp(totalXp);
    const messageCount = previousMessageCount + 1;
    const leveledUp = level > previousLevel;
    const progress = buildLevelProgress(totalXp, level);
    const cooldownRemainingSeconds =
      cooldownActive && Number.isFinite(lastXpAtMs)
        ? Math.max(0, Math.ceil((cooldownMs - (now.getTime() - lastXpAtMs)) / 1000))
        : 0;

    this.db
      .prepare(
        `
          UPDATE member_levels
          SET xp = ?, level = ?, message_count = ?, last_xp_at = ?, updated_at = ?
          WHERE guild_id = ? AND user_id = ?
        `
      )
      .run(totalXp, level, messageCount, appliedXp > 0 ? nowText : lastXpAt, nowText, guildId, userId);

    return {
      guild_id: toSnowflakeText(guildId),
      user_id: toSnowflakeText(userId),
      xp: totalXp,
      level,
      previous_level: previousLevel,
      previous_xp: previousXp,
      applied_xp: appliedXp,
      message_count: messageCount,
      leveled_up: leveledUp,
      cooldown_active: cooldownActive,
      cooldown_remaining_seconds: cooldownRemainingSeconds,
      current_level_xp: progress.currentLevelXp,
      next_level_xp: progress.nextLevelXp,
      progress_xp: progress.progressXp,
      progress_required: progress.progressRequired,
      progress_percent: progress.progressPercent
    };
  }

  getMemberLevelRank(guildId, userId) {
    const level = this.getMemberLevel(guildId, userId);

    const row = this.db
      .prepare(
        `
          SELECT COUNT(*) AS ahead_count
          FROM member_levels
          WHERE guild_id = @guild_id
            AND (
              level > @level
              OR (level = @level AND xp > @xp)
              OR (level = @level AND xp = @xp AND message_count > @message_count)
              OR (level = @level AND xp = @xp AND message_count = @message_count AND user_id < @user_id)
            )
        `
      )
      .get({
        guild_id: guildId,
        level: level.level,
        xp: level.xp,
        message_count: level.message_count,
        user_id: level.user_id
      });

    return Math.max(1, toInteger(row?.ahead_count, 0) + 1);
  }

  listLevelLeaderboard(guildId, limit = 10, offset = 0) {
    const normalizedLimit = Math.max(1, Math.min(toInteger(limit, 10), 50));
    const normalizedOffset = Math.max(0, toInteger(offset, 0));

    const rows = this.db
      .prepare(
        `
          SELECT user_id, xp, level, message_count, last_xp_at, updated_at
          FROM member_levels
          WHERE guild_id = ?
          ORDER BY level DESC, xp DESC, message_count DESC, user_id ASC
          LIMIT ? OFFSET ?
        `
      )
      .all(guildId, normalizedLimit, normalizedOffset);

    return rows.map((row, index) => {
      const xp = Math.max(0, toInteger(row.xp, 0));
      const level = Math.max(0, toInteger(row.level, levelFromTotalXp(xp)));
      const progress = buildLevelProgress(xp, level);

      return {
        rank: normalizedOffset + index + 1,
        user_id: toSnowflakeText(row.user_id),
        xp,
        level,
        message_count: Math.max(0, toInteger(row.message_count, 0)),
        last_xp_at: row.last_xp_at == null ? null : String(row.last_xp_at),
        updated_at: row.updated_at == null ? null : String(row.updated_at),
        current_level_xp: progress.currentLevelXp,
        next_level_xp: progress.nextLevelXp,
        progress_xp: progress.progressXp,
        progress_required: progress.progressRequired,
        progress_percent: progress.progressPercent
      };
    });
  }

  addReactionRole({
    guildId,
    channelId,
    messageId,
    emojiKey,
    emojiDisplay,
    roleId,
    createdByUserId = null
  }) {
    const result = this.db
      .prepare(
        `
          INSERT OR IGNORE INTO reaction_roles (
            guild_id, channel_id, message_id, emoji_key, emoji_display,
            role_id, created_by_user_id, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `
      )
      .run(guildId, channelId, messageId, emojiKey, emojiDisplay, roleId, createdByUserId, nowIso());

    return result.changes > 0;
  }

  listReactionRoles(guildId, messageId = null) {
    let rows;
    if (messageId == null) {
      rows = this.db
        .prepare(
          `
            SELECT id, channel_id, message_id, emoji_key, emoji_display, role_id, created_by_user_id, created_at
            FROM reaction_roles
            WHERE guild_id = ?
            ORDER BY message_id DESC, emoji_display ASC, role_id ASC
          `
        )
        .all(guildId);
    } else {
      rows = this.db
        .prepare(
          `
            SELECT id, channel_id, message_id, emoji_key, emoji_display, role_id, created_by_user_id, created_at
            FROM reaction_roles
            WHERE guild_id = ? AND message_id = ?
            ORDER BY emoji_display ASC, role_id ASC
          `
        )
        .all(guildId, messageId);
    }

    return rows.map((row) => ({
      id: toInteger(row.id, 0),
      channel_id: toSnowflakeText(row.channel_id),
      message_id: toSnowflakeText(row.message_id),
      emoji_key: String(row.emoji_key || ""),
      emoji_display: String(row.emoji_display || ""),
      role_id: toSnowflakeText(row.role_id),
      created_by_user_id: toSnowflakeText(row.created_by_user_id),
      created_at: String(row.created_at || nowIso())
    }));
  }

  getReactionRoleIds(guildId, channelId, messageId, emojiKey) {
    const rows = this.db
      .prepare(
        `
          SELECT role_id
          FROM reaction_roles
          WHERE guild_id = ? AND channel_id = ? AND message_id = ? AND emoji_key = ?
          ORDER BY role_id ASC
        `
      )
      .all(guildId, channelId, messageId, emojiKey);

    return rows.map((row) => toSnowflakeText(row.role_id)).filter(Boolean);
  }

  removeReactionRole(guildId, channelId, messageId, emojiKey, roleId = null) {
    let result;
    if (roleId) {
      result = this.db
        .prepare(
          `
            DELETE FROM reaction_roles
            WHERE guild_id = ? AND channel_id = ? AND message_id = ? AND emoji_key = ? AND role_id = ?
          `
        )
        .run(guildId, channelId, messageId, emojiKey, roleId);
    } else {
      result = this.db
        .prepare(
          `
            DELETE FROM reaction_roles
            WHERE guild_id = ? AND channel_id = ? AND message_id = ? AND emoji_key = ?
          `
        )
        .run(guildId, channelId, messageId, emojiKey);
    }

    return result.changes;
  }

  clearReactionRolesForMessage(guildId, channelId, messageId) {
    const result = this.db
      .prepare(
        `
          DELETE FROM reaction_roles
          WHERE guild_id = ? AND channel_id = ? AND message_id = ?
        `
      )
      .run(guildId, channelId, messageId);

    return result.changes;
  }
}
