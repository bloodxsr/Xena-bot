import { Pool } from "pg";

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
  if (typeof value === "boolean") {
    return value;
  }

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

const DEFAULT_WELCOME_MESSAGE_TEMPLATE = "Welcome {user.mention} to {guild.name}.";
const DEFAULT_LEVELUP_MESSAGE_TEMPLATE =
  "Level Up: {user.mention} reached level {level}. Rank #{rank}.";
const DEFAULT_KICK_MESSAGE_TEMPLATE = "Kicked {user.mention}. Reason: {reason}.";
const DEFAULT_BAN_MESSAGE_TEMPLATE = "Banned {user.mention}. Reason: {reason}.";
const DEFAULT_MUTE_MESSAGE_TEMPLATE = "{user.mention} muted for {duration_minutes} minute(s). Reason: {reason}.";
const DEFAULT_LEVEL_CARD_FONT = "default";
const DEFAULT_LEVEL_CARD_PRIMARY_COLOR = "#66f2c4";
const DEFAULT_LEVEL_CARD_ACCENT_COLOR = "#6da8ff";
const DEFAULT_LEVEL_CARD_OVERLAY_OPACITY = 0.38;
const DEFAULT_WELCOME_CARD_TITLE_TEMPLATE = "Welcome to {guild.name}";
const DEFAULT_WELCOME_CARD_SUBTITLE_TEMPLATE = "You're member #{server.member_count}.";
const DEFAULT_WELCOME_CARD_FONT = "default";
const DEFAULT_WELCOME_CARD_PRIMARY_COLOR = "#f8fafc";
const DEFAULT_WELCOME_CARD_ACCENT_COLOR = "#6dd6ff";
const DEFAULT_WELCOME_CARD_OVERLAY_OPACITY = 0.48;
const DEFAULT_TICKET_TRIGGER_EMOJI = "\u{1F3AB}";
const DEFAULT_TICKET_WELCOME_TEMPLATE =
  "Hello {user.mention}, thanks for opening a ticket. Our team will be with you soon.";

function normalizeTemplateText(value, fallback) {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function normalizeColor(value, fallback) {
  const text = String(value ?? "").trim();
  if (/^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(text)) {
    return text.toLowerCase();
  }

  return fallback;
}

function normalizeCommandName(commandName) {
  return String(commandName || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, "");
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

export class PostgresBotDatabase {
  constructor(options = {}) {
    this.pool = new Pool({
      host: String(options.host || "").trim(),
      port: Number(options.port || 5432),
      user: String(options.user || "").trim(),
      password: String(options.password || ""),
      database: String(options.database || "").trim(),
      max: Math.max(1, Number(options.maxPoolSize || 10)),
      ssl: String(options.sslMode || "disabled").toLowerCase() === "required" ? { rejectUnauthorized: false } : undefined
    });

    this.initPromise = this.initialize();
  }

  async queryRows(sql, params = []) {
    const result = await this.pool.query(sql, params);
    return result.rows;
  }

  async queryOne(sql, params = []) {
    const rows = await this.queryRows(sql, params);
    return rows[0] || null;
  }

  async execute(sql, params = []) {
    await this.pool.query(sql, params);
  }

  async ensureInitialized() {
    await this.initPromise;
  }

  async initialize() {
    await this.execute(`
      CREATE TABLE IF NOT EXISTS warnings (
        guild_id VARCHAR(22) NOT NULL,
        user_id VARCHAR(22) NOT NULL,
        warning_count INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id)
      );

      CREATE TABLE IF NOT EXISTS warning_events (
        id BIGSERIAL PRIMARY KEY,
        guild_id VARCHAR(22) NOT NULL,
        user_id VARCHAR(22) NOT NULL,
        actor_user_id VARCHAR(22),
        reason TEXT,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS moderation_logs (
        id BIGSERIAL PRIMARY KEY,
        guild_id VARCHAR(22) NOT NULL,
        actor_user_id VARCHAR(22),
        target_user_id VARCHAR(22),
        action TEXT NOT NULL,
        reason TEXT,
        channel_id VARCHAR(22),
        message_id VARCHAR(22),
        metadata TEXT,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS guild_config (
        guild_id VARCHAR(22) PRIMARY KEY,
        log_channel_id VARCHAR(22),
        welcome_channel_id VARCHAR(22),
        rules_channel_id VARCHAR(22),
        chat_channel_id VARCHAR(22),
        help_channel_id VARCHAR(22),
        about_channel_id VARCHAR(22),
        perks_channel_id VARCHAR(22),
        leveling_channel_id VARCHAR(22),
        welcome_message_template TEXT,
        levelup_message_template TEXT,
        kick_message_template TEXT,
        ban_message_template TEXT,
        mute_message_template TEXT,
        level_card_font TEXT NOT NULL DEFAULT 'default',
        level_card_primary_color TEXT NOT NULL DEFAULT '#66f2c4',
        level_card_accent_color TEXT NOT NULL DEFAULT '#6da8ff',
        level_card_background_url TEXT,
        level_card_overlay_opacity DOUBLE PRECISION NOT NULL DEFAULT 0.38,
        welcome_card_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        welcome_card_title_template TEXT,
        welcome_card_subtitle_template TEXT,
        welcome_card_font TEXT NOT NULL DEFAULT 'default',
        welcome_card_primary_color TEXT NOT NULL DEFAULT '#f8fafc',
        welcome_card_accent_color TEXT NOT NULL DEFAULT '#6dd6ff',
        welcome_card_background_url TEXT,
        welcome_card_overlay_opacity DOUBLE PRECISION NOT NULL DEFAULT 0.48,
        ticket_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        ticket_trigger_channel_id VARCHAR(22),
        ticket_trigger_message_id VARCHAR(22),
        ticket_trigger_emoji TEXT NOT NULL DEFAULT '🎫',
        ticket_category_channel_id VARCHAR(22),
        ticket_support_role_id VARCHAR(22),
        ticket_welcome_template TEXT,
        admin_role_name TEXT NOT NULL DEFAULT 'Admin',
        mod_role_name TEXT NOT NULL DEFAULT 'Moderator',
        sync_mode TEXT NOT NULL DEFAULT 'global',
        sync_guild_id VARCHAR(22),
        verification_url TEXT,
        leveling_enabled BOOLEAN NOT NULL DEFAULT TRUE,
        raid_detection_enabled BOOLEAN NOT NULL DEFAULT TRUE,
        raid_gate_threshold DOUBLE PRECISION NOT NULL DEFAULT 0.72,
        raid_monitor_window_seconds INTEGER NOT NULL DEFAULT 90,
        raid_join_rate_threshold INTEGER NOT NULL DEFAULT 8,
        gate_duration_seconds INTEGER NOT NULL DEFAULT 900,
        join_gate_mode TEXT NOT NULL DEFAULT 'timeout'
      );

      CREATE TABLE IF NOT EXISTS raid_state (
        guild_id VARCHAR(22) PRIMARY KEY,
        gate_active BOOLEAN NOT NULL DEFAULT FALSE,
        gate_reason TEXT,
        gate_until TEXT,
        updated_at TEXT
      );

      CREATE TABLE IF NOT EXISTS verification_queue (
        guild_id VARCHAR(22) NOT NULL,
        user_id VARCHAR(22) NOT NULL,
        status TEXT NOT NULL,
        risk_score DOUBLE PRECISION NOT NULL,
        verification_url TEXT,
        reason TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        verified_by_user_id VARCHAR(22),
        PRIMARY KEY (guild_id, user_id)
      );

      CREATE TABLE IF NOT EXISTS join_events (
        id BIGSERIAL PRIMARY KEY,
        guild_id VARCHAR(22) NOT NULL,
        user_id VARCHAR(22) NOT NULL,
        account_age_days DOUBLE PRECISION NOT NULL,
        has_avatar BOOLEAN NOT NULL,
        profile_score DOUBLE PRECISION NOT NULL,
        join_rate DOUBLE PRECISION NOT NULL,
        young_account_ratio DOUBLE PRECISION NOT NULL,
        risk_score DOUBLE PRECISION NOT NULL,
        risk_level TEXT NOT NULL,
        action TEXT NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS reaction_roles (
        id BIGSERIAL PRIMARY KEY,
        guild_id VARCHAR(22) NOT NULL,
        channel_id VARCHAR(22) NOT NULL,
        message_id VARCHAR(22) NOT NULL,
        emoji_key TEXT NOT NULL,
        emoji_display TEXT NOT NULL,
        role_id VARCHAR(22) NOT NULL,
        created_by_user_id VARCHAR(22),
        created_at TEXT NOT NULL,
        UNIQUE (guild_id, channel_id, message_id, emoji_key, role_id)
      );

      CREATE TABLE IF NOT EXISTS member_levels (
        guild_id VARCHAR(22) NOT NULL,
        user_id VARCHAR(22) NOT NULL,
        xp INTEGER NOT NULL DEFAULT 0,
        level INTEGER NOT NULL DEFAULT 0,
        message_count INTEGER NOT NULL DEFAULT 0,
        last_xp_at TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id)
      );

      CREATE TABLE IF NOT EXISTS staff_totp (
        guild_id VARCHAR(22) NOT NULL,
        user_id VARCHAR(22) NOT NULL,
        secret_base32 TEXT NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        last_verified_at TEXT,
        PRIMARY KEY (guild_id, user_id)
      );

      CREATE TABLE IF NOT EXISTS command_toggles (
        guild_id VARCHAR(22) NOT NULL,
        command_name TEXT NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, command_name)
      );

      CREATE TABLE IF NOT EXISTS ticket_threads (
        guild_id VARCHAR(22) NOT NULL,
        user_id VARCHAR(22) NOT NULL,
        channel_id VARCHAR(22) NOT NULL,
        trigger_channel_id VARCHAR(22),
        trigger_message_id VARCHAR(22),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id)
      );

      CREATE TABLE IF NOT EXISTS reaction_role_panels (
        guild_id VARCHAR(22) NOT NULL,
        channel_id VARCHAR(22) NOT NULL,
        message_id VARCHAR(22) NOT NULL,
        content TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, message_id)
      );

      CREATE INDEX IF NOT EXISTS idx_verification_queue_pending
      ON verification_queue (guild_id, status, updated_at);

      CREATE INDEX IF NOT EXISTS idx_join_events_lookup
      ON join_events (guild_id, id DESC);

      CREATE INDEX IF NOT EXISTS idx_reaction_roles_lookup
      ON reaction_roles (guild_id, channel_id, message_id, emoji_key);

      CREATE INDEX IF NOT EXISTS idx_member_levels_rank
      ON member_levels (guild_id, level DESC, xp DESC, message_count DESC, user_id ASC);

      CREATE INDEX IF NOT EXISTS idx_staff_totp_last_verified
      ON staff_totp (guild_id, last_verified_at);

      CREATE INDEX IF NOT EXISTS idx_command_toggles_lookup
      ON command_toggles (guild_id, command_name, enabled);

      CREATE INDEX IF NOT EXISTS idx_ticket_threads_channel_lookup
      ON ticket_threads (guild_id, channel_id);
    `);

    await this.execute("UPDATE guild_config SET leveling_enabled = TRUE WHERE leveling_enabled IS DISTINCT FROM TRUE");
    await this.execute("UPDATE guild_config SET raid_detection_enabled = TRUE WHERE raid_detection_enabled IS DISTINCT FROM TRUE");
    await this.execute(
      "UPDATE guild_config SET welcome_message_template = $1 WHERE welcome_message_template IS NULL OR TRIM(welcome_message_template) = ''",
      [DEFAULT_WELCOME_MESSAGE_TEMPLATE]
    );
    await this.execute(
      "UPDATE guild_config SET levelup_message_template = $1 WHERE levelup_message_template IS NULL OR TRIM(levelup_message_template) = ''",
      [DEFAULT_LEVELUP_MESSAGE_TEMPLATE]
    );
    await this.execute(
      "UPDATE guild_config SET kick_message_template = $1 WHERE kick_message_template IS NULL OR TRIM(kick_message_template) = ''",
      [DEFAULT_KICK_MESSAGE_TEMPLATE]
    );
    await this.execute(
      "UPDATE guild_config SET ban_message_template = $1 WHERE ban_message_template IS NULL OR TRIM(ban_message_template) = ''",
      [DEFAULT_BAN_MESSAGE_TEMPLATE]
    );
    await this.execute(
      "UPDATE guild_config SET mute_message_template = $1 WHERE mute_message_template IS NULL OR TRIM(mute_message_template) = ''",
      [DEFAULT_MUTE_MESSAGE_TEMPLATE]
    );
    await this.execute(
      "UPDATE guild_config SET welcome_card_title_template = $1 WHERE welcome_card_title_template IS NULL OR TRIM(welcome_card_title_template) = ''",
      [DEFAULT_WELCOME_CARD_TITLE_TEMPLATE]
    );
    await this.execute(
      "UPDATE guild_config SET welcome_card_subtitle_template = $1 WHERE welcome_card_subtitle_template IS NULL OR TRIM(welcome_card_subtitle_template) = ''",
      [DEFAULT_WELCOME_CARD_SUBTITLE_TEMPLATE]
    );
    await this.execute(
      "UPDATE guild_config SET ticket_trigger_emoji = $1 WHERE ticket_trigger_emoji IS NULL OR TRIM(ticket_trigger_emoji) = ''",
      [DEFAULT_TICKET_TRIGGER_EMOJI]
    );
    await this.execute(
      "UPDATE guild_config SET ticket_welcome_template = $1 WHERE ticket_welcome_template IS NULL OR TRIM(ticket_welcome_template) = ''",
      [DEFAULT_TICKET_WELCOME_TEMPLATE]
    );
  }

  async ensureGuildConfig(guildId) {
    await this.ensureInitialized();

    const id = toSnowflakeText(guildId);
    if (!id) {
      throw new Error("invalid guild id");
    }

    await this.execute("INSERT INTO guild_config (guild_id) VALUES ($1) ON CONFLICT (guild_id) DO NOTHING", [id]);
  }

  async getGuildConfig(guildId) {
    await this.ensureGuildConfig(guildId);

    const row = await this.queryOne("SELECT * FROM guild_config WHERE guild_id = $1", [guildId]);
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
      welcome_message_template: normalizeTemplateText(row.welcome_message_template, DEFAULT_WELCOME_MESSAGE_TEMPLATE),
      levelup_message_template: normalizeTemplateText(row.levelup_message_template, DEFAULT_LEVELUP_MESSAGE_TEMPLATE),
      kick_message_template: normalizeTemplateText(row.kick_message_template, DEFAULT_KICK_MESSAGE_TEMPLATE),
      ban_message_template: normalizeTemplateText(row.ban_message_template, DEFAULT_BAN_MESSAGE_TEMPLATE),
      mute_message_template: normalizeTemplateText(row.mute_message_template, DEFAULT_MUTE_MESSAGE_TEMPLATE),
      level_card_font: String(row.level_card_font || DEFAULT_LEVEL_CARD_FONT),
      level_card_primary_color: normalizeColor(row.level_card_primary_color, DEFAULT_LEVEL_CARD_PRIMARY_COLOR),
      level_card_accent_color: normalizeColor(row.level_card_accent_color, DEFAULT_LEVEL_CARD_ACCENT_COLOR),
      level_card_background_url: row.level_card_background_url == null ? null : String(row.level_card_background_url),
      level_card_overlay_opacity: Math.max(
        0,
        Math.min(toFloat(row.level_card_overlay_opacity, DEFAULT_LEVEL_CARD_OVERLAY_OPACITY), 1)
      ),
      welcome_card_enabled: toBoolean(row.welcome_card_enabled),
      welcome_card_title_template: normalizeTemplateText(
        row.welcome_card_title_template,
        DEFAULT_WELCOME_CARD_TITLE_TEMPLATE
      ),
      welcome_card_subtitle_template: normalizeTemplateText(
        row.welcome_card_subtitle_template,
        DEFAULT_WELCOME_CARD_SUBTITLE_TEMPLATE
      ),
      welcome_card_font: String(row.welcome_card_font || DEFAULT_WELCOME_CARD_FONT),
      welcome_card_primary_color: normalizeColor(
        row.welcome_card_primary_color,
        DEFAULT_WELCOME_CARD_PRIMARY_COLOR
      ),
      welcome_card_accent_color: normalizeColor(row.welcome_card_accent_color, DEFAULT_WELCOME_CARD_ACCENT_COLOR),
      welcome_card_background_url:
        row.welcome_card_background_url == null ? null : String(row.welcome_card_background_url),
      welcome_card_overlay_opacity: Math.max(
        0,
        Math.min(toFloat(row.welcome_card_overlay_opacity, DEFAULT_WELCOME_CARD_OVERLAY_OPACITY), 1)
      ),
      ticket_enabled: toBoolean(row.ticket_enabled),
      ticket_trigger_channel_id: toSnowflakeText(row.ticket_trigger_channel_id),
      ticket_trigger_message_id: toSnowflakeText(row.ticket_trigger_message_id),
      ticket_trigger_emoji: String(row.ticket_trigger_emoji || DEFAULT_TICKET_TRIGGER_EMOJI),
      ticket_category_channel_id: toSnowflakeText(row.ticket_category_channel_id),
      ticket_support_role_id: toSnowflakeText(row.ticket_support_role_id),
      ticket_welcome_template: normalizeTemplateText(row.ticket_welcome_template, DEFAULT_TICKET_WELCOME_TEMPLATE),
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

  async updateGuildConfig(guildId, updates) {
    const allowed = new Set([
      "log_channel_id",
      "welcome_channel_id",
      "rules_channel_id",
      "chat_channel_id",
      "help_channel_id",
      "about_channel_id",
      "perks_channel_id",
      "leveling_channel_id",
      "welcome_message_template",
      "levelup_message_template",
      "kick_message_template",
      "ban_message_template",
      "mute_message_template",
      "level_card_font",
      "level_card_primary_color",
      "level_card_accent_color",
      "level_card_background_url",
      "level_card_overlay_opacity",
      "welcome_card_enabled",
      "welcome_card_title_template",
      "welcome_card_subtitle_template",
      "welcome_card_font",
      "welcome_card_primary_color",
      "welcome_card_accent_color",
      "welcome_card_background_url",
      "welcome_card_overlay_opacity",
      "ticket_enabled",
      "ticket_trigger_channel_id",
      "ticket_trigger_message_id",
      "ticket_trigger_emoji",
      "ticket_category_channel_id",
      "ticket_support_role_id",
      "ticket_welcome_template",
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

    await this.ensureGuildConfig(guildId);

    const normalized = {};
    for (const [key, rawValue] of entries) {
      if (key.endsWith("_channel_id") || key === "sync_guild_id") {
        normalized[key] = toSnowflakeText(rawValue);
        continue;
      }

      if (key === "ticket_trigger_message_id" || key === "ticket_support_role_id") {
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

      if (key === "welcome_message_template") {
        normalized[key] = normalizeTemplateText(rawValue, DEFAULT_WELCOME_MESSAGE_TEMPLATE);
        continue;
      }

      if (key === "levelup_message_template") {
        normalized[key] = normalizeTemplateText(rawValue, DEFAULT_LEVELUP_MESSAGE_TEMPLATE);
        continue;
      }

      if (key === "kick_message_template") {
        normalized[key] = normalizeTemplateText(rawValue, DEFAULT_KICK_MESSAGE_TEMPLATE);
        continue;
      }

      if (key === "ban_message_template") {
        normalized[key] = normalizeTemplateText(rawValue, DEFAULT_BAN_MESSAGE_TEMPLATE);
        continue;
      }

      if (key === "mute_message_template") {
        normalized[key] = normalizeTemplateText(rawValue, DEFAULT_MUTE_MESSAGE_TEMPLATE);
        continue;
      }

      if (key === "welcome_card_title_template") {
        normalized[key] = normalizeTemplateText(rawValue, DEFAULT_WELCOME_CARD_TITLE_TEMPLATE);
        continue;
      }

      if (key === "welcome_card_subtitle_template") {
        normalized[key] = normalizeTemplateText(rawValue, DEFAULT_WELCOME_CARD_SUBTITLE_TEMPLATE);
        continue;
      }

      if (key === "ticket_welcome_template") {
        normalized[key] = normalizeTemplateText(rawValue, DEFAULT_TICKET_WELCOME_TEMPLATE);
        continue;
      }

      if (key === "level_card_font") {
        const text = String(rawValue ?? "").trim().toLowerCase();
        normalized[key] = text || DEFAULT_LEVEL_CARD_FONT;
        continue;
      }

      if (key === "welcome_card_font") {
        const text = String(rawValue ?? "").trim().toLowerCase();
        normalized[key] = text || DEFAULT_WELCOME_CARD_FONT;
        continue;
      }

      if (key === "level_card_primary_color") {
        normalized[key] = normalizeColor(rawValue, DEFAULT_LEVEL_CARD_PRIMARY_COLOR);
        continue;
      }

      if (key === "level_card_accent_color") {
        normalized[key] = normalizeColor(rawValue, DEFAULT_LEVEL_CARD_ACCENT_COLOR);
        continue;
      }

      if (key === "welcome_card_primary_color") {
        normalized[key] = normalizeColor(rawValue, DEFAULT_WELCOME_CARD_PRIMARY_COLOR);
        continue;
      }

      if (key === "welcome_card_accent_color") {
        normalized[key] = normalizeColor(rawValue, DEFAULT_WELCOME_CARD_ACCENT_COLOR);
        continue;
      }

      if (key === "level_card_background_url" || key === "welcome_card_background_url") {
        const value = String(rawValue ?? "").trim();
        normalized[key] = value === "" ? null : value;
        continue;
      }

      if (key === "level_card_overlay_opacity") {
        normalized[key] = Math.max(0, Math.min(toFloat(rawValue, DEFAULT_LEVEL_CARD_OVERLAY_OPACITY), 1));
        continue;
      }

      if (key === "welcome_card_overlay_opacity") {
        normalized[key] = Math.max(0, Math.min(toFloat(rawValue, DEFAULT_WELCOME_CARD_OVERLAY_OPACITY), 1));
        continue;
      }

      if (key === "welcome_card_enabled" || key === "ticket_enabled") {
        normalized[key] = toBoolean(rawValue);
        continue;
      }

      if (key === "ticket_trigger_emoji") {
        const value = String(rawValue ?? "").trim();
        normalized[key] = value || DEFAULT_TICKET_TRIGGER_EMOJI;
        continue;
      }

      normalized[key] = rawValue;
    }

    const fields = Object.keys(normalized);
    const setClause = fields.map((key, index) => `${key} = $${index + 1}`).join(", ");
    const values = [...fields.map((key) => normalized[key]), guildId];
    await this.execute(`UPDATE guild_config SET ${setClause} WHERE guild_id = $${fields.length + 1}`, values);

    return this.getGuildConfig(guildId);
  }

  async getStaffTotpAuth(guildId, userId) {
    await this.ensureInitialized();

    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedUserId = toSnowflakeText(userId);

    if (!normalizedGuildId || !normalizedUserId) {
      return null;
    }

    const row = await this.queryOne(
      `
        SELECT guild_id, user_id, secret_base32, enabled, created_at, updated_at, last_verified_at
        FROM staff_totp
        WHERE guild_id = $1 AND user_id = $2
      `,
      [normalizedGuildId, normalizedUserId]
    );

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

  async upsertStaffTotpAuth({ guildId, userId, secretBase32, enabled = true, lastVerifiedAt = null }) {
    await this.ensureInitialized();

    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedUserId = toSnowflakeText(userId);
    const normalizedSecret = String(secretBase32 || "")
      .trim()
      .toUpperCase();

    if (!normalizedGuildId || !normalizedUserId || !normalizedSecret) {
      throw new Error("invalid totp enrollment payload");
    }

    const now = nowIso();
    await this.execute(
      `
        INSERT INTO staff_totp (
          guild_id, user_id, secret_base32, enabled, created_at, updated_at, last_verified_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET
          secret_base32 = excluded.secret_base32,
          enabled = excluded.enabled,
          updated_at = excluded.updated_at,
          last_verified_at = excluded.last_verified_at
      `,
      [
        normalizedGuildId,
        normalizedUserId,
        normalizedSecret,
        enabled ? true : false,
        now,
        now,
        lastVerifiedAt == null ? null : String(lastVerifiedAt)
      ]
    );

    return this.getStaffTotpAuth(normalizedGuildId, normalizedUserId);
  }

  async markStaffTotpVerified(guildId, userId, verifiedAt = null) {
    await this.ensureInitialized();

    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedUserId = toSnowflakeText(userId);

    if (!normalizedGuildId || !normalizedUserId) {
      return null;
    }

    const when = verifiedAt == null ? nowIso() : String(verifiedAt);
    const now = nowIso();

    await this.execute(
      `
        UPDATE staff_totp
        SET enabled = TRUE, last_verified_at = $1, updated_at = $2
        WHERE guild_id = $3 AND user_id = $4
      `,
      [when, now, normalizedGuildId, normalizedUserId]
    );

    return this.getStaffTotpAuth(normalizedGuildId, normalizedUserId);
  }

  async clearStaffTotpVerification(guildId, userId) {
    await this.ensureInitialized();

    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedUserId = toSnowflakeText(userId);

    if (!normalizedGuildId || !normalizedUserId) {
      return null;
    }

    await this.execute(
      `
        UPDATE staff_totp
        SET last_verified_at = NULL, updated_at = $1
        WHERE guild_id = $2 AND user_id = $3
      `,
      [nowIso(), normalizedGuildId, normalizedUserId]
    );

    return this.getStaffTotpAuth(normalizedGuildId, normalizedUserId);
  }

  async getWarningCount(guildId, userId) {
    await this.ensureInitialized();

    const row = await this.queryOne(
      "SELECT warning_count FROM warnings WHERE guild_id = $1 AND user_id = $2",
      [guildId, userId]
    );

    return row ? toInteger(row.warning_count, 0) : 0;
  }

  async incrementWarning(guildId, userId, reason, actorUserId = null) {
    await this.ensureInitialized();

    const now = nowIso();
    const current = await this.getWarningCount(guildId, userId);
    const next = current + 1;

    await this.execute(
      `
        INSERT INTO warnings (guild_id, user_id, warning_count, updated_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET
          warning_count = excluded.warning_count,
          updated_at = excluded.updated_at
      `,
      [guildId, userId, next, now]
    );

    await this.execute(
      `
        INSERT INTO warning_events (guild_id, user_id, actor_user_id, reason, created_at)
        VALUES ($1, $2, $3, $4, $5)
      `,
      [guildId, userId, actorUserId, reason || null, now]
    );

    return next;
  }

  async resetWarnings(guildId, userId) {
    await this.ensureInitialized();
    await this.execute("DELETE FROM warnings WHERE guild_id = $1 AND user_id = $2", [guildId, userId]);
  }

  async listWarningCounts(guildId, limit = 50) {
    await this.ensureInitialized();

    const normalizedLimit = Math.max(1, Math.min(toInteger(limit, 50), 200));
    const rows = await this.queryRows(
      `
        SELECT user_id, warning_count, updated_at
        FROM warnings
        WHERE guild_id = $1
        ORDER BY warning_count DESC, updated_at DESC
        LIMIT $2
      `,
      [guildId, normalizedLimit]
    );

    return rows.map((row) => ({
      user_id: toSnowflakeText(row.user_id),
      warning_count: Math.max(0, toInteger(row.warning_count, 0)),
      updated_at: String(row.updated_at || nowIso())
    }));
  }

  async listKnownGuildIds(limit = 1000) {
    await this.ensureInitialized();

    const normalizedLimit = Math.max(1, Math.min(toInteger(limit, 1000), 10000));
    const rows = await this.queryRows(
      `
        SELECT guild_id
        FROM guild_config
        ORDER BY guild_id ASC
        LIMIT $1
      `,
      [normalizedLimit]
    );

    return rows.map((row) => toSnowflakeText(row.guild_id)).filter(Boolean);
  }

  async isCommandEnabled(guildId, commandName) {
    await this.ensureInitialized();

    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedCommand = normalizeCommandName(commandName);
    if (!normalizedGuildId || !normalizedCommand) {
      return true;
    }

    const row = await this.queryOne(
      `
        SELECT enabled
        FROM command_toggles
        WHERE guild_id = $1 AND command_name = $2
      `,
      [normalizedGuildId, normalizedCommand]
    );

    if (!row) {
      return true;
    }

    return toBoolean(row.enabled);
  }

  async setCommandEnabled(guildId, commandName, enabled) {
    await this.ensureInitialized();

    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedCommand = normalizeCommandName(commandName);
    if (!normalizedGuildId || !normalizedCommand) {
      throw new Error("invalid guild id or command name");
    }

    const now = nowIso();
    await this.execute(
      `
        INSERT INTO command_toggles (guild_id, command_name, enabled, updated_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT(guild_id, command_name)
        DO UPDATE SET
          enabled = excluded.enabled,
          updated_at = excluded.updated_at
      `,
      [normalizedGuildId, normalizedCommand, enabled ? true : false, now]
    );

    return this.isCommandEnabled(normalizedGuildId, normalizedCommand);
  }

  async listCommandToggles(guildId) {
    await this.ensureInitialized();

    const normalizedGuildId = toSnowflakeText(guildId);
    if (!normalizedGuildId) {
      return [];
    }

    const rows = await this.queryRows(
      `
        SELECT command_name, enabled, updated_at
        FROM command_toggles
        WHERE guild_id = $1
        ORDER BY command_name ASC
      `,
      [normalizedGuildId]
    );

    return rows.map((row) => ({
      command_name: String(row.command_name || ""),
      enabled: toBoolean(row.enabled),
      updated_at: String(row.updated_at || nowIso())
    }));
  }

  async logModerationAction({
    guildId,
    action,
    actorUserId = null,
    targetUserId = null,
    reason = null,
    channelId = null,
    messageId = null,
    metadata = null
  }) {
    await this.ensureInitialized();

    await this.execute(
      `
        INSERT INTO moderation_logs (
          guild_id, actor_user_id, target_user_id, action, reason,
          channel_id, message_id, metadata, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `,
      [
        guildId,
        actorUserId,
        targetUserId,
        action,
        reason,
        channelId,
        messageId,
        metadata == null ? null : JSON.stringify(metadata),
        nowIso()
      ]
    );
  }

  async upsertVerificationMember({
    guildId,
    userId,
    status,
    riskScore,
    verificationUrl = null,
    reason,
    verifiedByUserId = null
  }) {
    await this.ensureInitialized();

    const now = nowIso();

    await this.execute(
      `
        INSERT INTO verification_queue (
          guild_id, user_id, status, risk_score, verification_url, reason,
          created_at, updated_at, verified_by_user_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET
          status = excluded.status,
          risk_score = excluded.risk_score,
          verification_url = excluded.verification_url,
          reason = excluded.reason,
          updated_at = excluded.updated_at,
          verified_by_user_id = excluded.verified_by_user_id
      `,
      [guildId, userId, status, riskScore, verificationUrl, reason, now, now, verifiedByUserId]
    );
  }

  async getVerificationStatus(guildId, userId) {
    await this.ensureInitialized();

    const row = await this.queryOne(
      `
        SELECT status, risk_score, verification_url, reason, created_at, updated_at, verified_by_user_id
        FROM verification_queue
        WHERE guild_id = $1 AND user_id = $2
      `,
      [guildId, userId]
    );

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

  async isMemberPendingVerification(guildId, userId) {
    const status = await this.getVerificationStatus(guildId, userId);
    return status != null && status.status === "pending";
  }

  async listPendingVerifications(guildId, limit = 20) {
    await this.ensureInitialized();

    const clamped = Math.max(1, Math.min(toInteger(limit, 20), 50));
    const rows = await this.queryRows(
      `
        SELECT user_id, status, risk_score, verification_url, reason, created_at, updated_at, verified_by_user_id
        FROM verification_queue
        WHERE guild_id = $1 AND status = 'pending'
        ORDER BY updated_at DESC
        LIMIT $2
      `,
      [guildId, clamped]
    );

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

  async setRaidGateState(guildId, gateActive, reason = null, gateUntil = null) {
    await this.ensureInitialized();

    await this.execute(
      `
        INSERT INTO raid_state (guild_id, gate_active, gate_reason, gate_until, updated_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT(guild_id)
        DO UPDATE SET
          gate_active = excluded.gate_active,
          gate_reason = excluded.gate_reason,
          gate_until = excluded.gate_until,
          updated_at = excluded.updated_at
      `,
      [guildId, gateActive ? true : false, reason, gateUntil, nowIso()]
    );
  }

  async getRaidGateState(guildId) {
    await this.ensureInitialized();

    const row = await this.queryOne(
      "SELECT gate_active, gate_reason, gate_until, updated_at FROM raid_state WHERE guild_id = $1",
      [guildId]
    );

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

  async logJoinEvent({
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
    await this.ensureInitialized();

    await this.execute(
      `
        INSERT INTO join_events (
          guild_id, user_id, account_age_days, has_avatar, profile_score,
          join_rate, young_account_ratio, risk_score, risk_level, action, metadata, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
      `,
      [
        guildId,
        userId,
        accountAgeDays,
        hasAvatar ? true : false,
        profileScore,
        joinRate,
        youngAccountRatio,
        riskScore,
        riskLevel,
        action,
        metadata == null ? null : JSON.stringify(metadata),
        nowIso()
      ]
    );
  }

  async listRecentJoinEvents(guildId, limit = 20) {
    await this.ensureInitialized();

    const clamped = Math.max(1, Math.min(toInteger(limit, 20), 100));
    const rows = await this.queryRows(
      `
        SELECT user_id, risk_score, risk_level, action, metadata, created_at
        FROM join_events
        WHERE guild_id = $1
        ORDER BY id DESC
        LIMIT $2
      `,
      [guildId, clamped]
    );

    return rows.map((row) => ({
      user_id: toSnowflakeText(row.user_id),
      risk_score: toFloat(row.risk_score, 0),
      risk_level: String(row.risk_level || "unknown"),
      action: String(row.action || "allow"),
      metadata: parseMetadata(row.metadata),
      created_at: String(row.created_at || nowIso())
    }));
  }

  async ensureMemberLevelRow(guildId, userId) {
    await this.ensureInitialized();

    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedUserId = toSnowflakeText(userId);
    if (!normalizedGuildId || !normalizedUserId) {
      throw new Error("invalid guild id or user id");
    }

    await this.execute(
      `
        INSERT INTO member_levels (guild_id, user_id, updated_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (guild_id, user_id) DO NOTHING
      `,
      [normalizedGuildId, normalizedUserId, nowIso()]
    );
  }

  async getMemberLevel(guildId, userId) {
    await this.ensureMemberLevelRow(guildId, userId);

    const row = await this.queryOne(
      `
        SELECT guild_id, user_id, xp, level, message_count, last_xp_at, updated_at
        FROM member_levels
        WHERE guild_id = $1 AND user_id = $2
      `,
      [guildId, userId]
    );

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

  async addMemberXp({ guildId, userId, xpGain, cooldownSeconds = 45 }) {
    await this.ensureMemberLevelRow(guildId, userId);

    const now = new Date();
    const nowText = now.toISOString();
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");
      const result = await client.query(
        `
          SELECT xp, level, message_count, last_xp_at
          FROM member_levels
          WHERE guild_id = $1 AND user_id = $2
          FOR UPDATE
        `,
        [guildId, userId]
      );

      const row = result.rows[0] || null;
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

      await client.query(
        `
          UPDATE member_levels
          SET xp = $1, level = $2, message_count = $3, last_xp_at = $4, updated_at = $5
          WHERE guild_id = $6 AND user_id = $7
        `,
        [totalXp, level, messageCount, appliedXp > 0 ? nowText : lastXpAt, nowText, guildId, userId]
      );

      await client.query("COMMIT");

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
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async getMemberLevelRank(guildId, userId) {
    const level = await this.getMemberLevel(guildId, userId);

    const row = await this.queryOne(
      `
        SELECT COUNT(*)::int AS ahead_count
        FROM member_levels
        WHERE guild_id = $1
          AND (
            level > $2
            OR (level = $2 AND xp > $3)
            OR (level = $2 AND xp = $3 AND message_count > $4)
            OR (level = $2 AND xp = $3 AND message_count = $4 AND user_id < $5)
          )
      `,
      [guildId, level.level, level.xp, level.message_count, level.user_id]
    );

    return Math.max(1, toInteger(row?.ahead_count, 0) + 1);
  }

  async getLevelMemberCount(guildId) {
    await this.ensureInitialized();

    const row = await this.queryOne(
      `
        SELECT COUNT(*)::int AS total_count
        FROM member_levels
        WHERE guild_id = $1
      `,
      [guildId]
    );

    return Math.max(0, toInteger(row?.total_count, 0));
  }

  async listLevelLeaderboard(guildId, limit = 10, offset = 0) {
    await this.ensureInitialized();

    const normalizedLimit = Math.max(1, Math.min(toInteger(limit, 10), 50));
    const normalizedOffset = Math.max(0, toInteger(offset, 0));

    const rows = await this.queryRows(
      `
        SELECT user_id, xp, level, message_count, last_xp_at, updated_at
        FROM member_levels
        WHERE guild_id = $1
        ORDER BY level DESC, xp DESC, message_count DESC, user_id ASC
        LIMIT $2 OFFSET $3
      `,
      [guildId, normalizedLimit, normalizedOffset]
    );

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

  async getOpenTicketForUser(guildId, userId) {
    await this.ensureInitialized();

    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedUserId = toSnowflakeText(userId);
    if (!normalizedGuildId || !normalizedUserId) {
      return null;
    }

    const row = await this.queryOne(
      `
        SELECT guild_id, user_id, channel_id, trigger_channel_id, trigger_message_id, created_at, updated_at
        FROM ticket_threads
        WHERE guild_id = $1 AND user_id = $2
      `,
      [normalizedGuildId, normalizedUserId]
    );

    if (!row) {
      return null;
    }

    return {
      guild_id: toSnowflakeText(row.guild_id),
      user_id: toSnowflakeText(row.user_id),
      channel_id: toSnowflakeText(row.channel_id),
      trigger_channel_id: toSnowflakeText(row.trigger_channel_id),
      trigger_message_id: toSnowflakeText(row.trigger_message_id),
      created_at: String(row.created_at || nowIso()),
      updated_at: String(row.updated_at || nowIso())
    };
  }

  async getOpenTicketForChannel(guildId, channelId) {
    await this.ensureInitialized();

    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedChannelId = toSnowflakeText(channelId);
    if (!normalizedGuildId || !normalizedChannelId) {
      return null;
    }

    const row = await this.queryOne(
      `
        SELECT guild_id, user_id, channel_id, trigger_channel_id, trigger_message_id, created_at, updated_at
        FROM ticket_threads
        WHERE guild_id = $1 AND channel_id = $2
      `,
      [normalizedGuildId, normalizedChannelId]
    );

    if (!row) {
      return null;
    }

    return {
      guild_id: toSnowflakeText(row.guild_id),
      user_id: toSnowflakeText(row.user_id),
      channel_id: toSnowflakeText(row.channel_id),
      trigger_channel_id: toSnowflakeText(row.trigger_channel_id),
      trigger_message_id: toSnowflakeText(row.trigger_message_id),
      created_at: String(row.created_at || nowIso()),
      updated_at: String(row.updated_at || nowIso())
    };
  }

  async setOpenTicket({ guildId, userId, channelId, triggerChannelId = null, triggerMessageId = null }) {
    await this.ensureInitialized();

    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedUserId = toSnowflakeText(userId);
    const normalizedChannelId = toSnowflakeText(channelId);
    const normalizedTriggerChannelId = toSnowflakeText(triggerChannelId);
    const normalizedTriggerMessageId = toSnowflakeText(triggerMessageId);

    if (!normalizedGuildId || !normalizedUserId || !normalizedChannelId) {
      throw new Error("invalid ticket thread payload");
    }

    const now = nowIso();
    await this.execute(
      `
        INSERT INTO ticket_threads (
          guild_id, user_id, channel_id, trigger_channel_id, trigger_message_id, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $6)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET
          channel_id = excluded.channel_id,
          trigger_channel_id = excluded.trigger_channel_id,
          trigger_message_id = excluded.trigger_message_id,
          updated_at = excluded.updated_at
      `,
      [
        normalizedGuildId,
        normalizedUserId,
        normalizedChannelId,
        normalizedTriggerChannelId,
        normalizedTriggerMessageId,
        now
      ]
    );

    return this.getOpenTicketForUser(normalizedGuildId, normalizedUserId);
  }

  async clearOpenTicketForUser(guildId, userId) {
    await this.ensureInitialized();

    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedUserId = toSnowflakeText(userId);
    if (!normalizedGuildId || !normalizedUserId) {
      return 0;
    }

    const result = await this.pool.query(
      `
        DELETE FROM ticket_threads
        WHERE guild_id = $1 AND user_id = $2
      `,
      [normalizedGuildId, normalizedUserId]
    );

    return Number(result.rowCount || 0);
  }

  async clearOpenTicketForChannel(guildId, channelId) {
    await this.ensureInitialized();

    const normalizedGuildId = toSnowflakeText(guildId);
    const normalizedChannelId = toSnowflakeText(channelId);
    if (!normalizedGuildId || !normalizedChannelId) {
      return 0;
    }

    const result = await this.pool.query(
      `
        DELETE FROM ticket_threads
        WHERE guild_id = $1 AND channel_id = $2
      `,
      [normalizedGuildId, normalizedChannelId]
    );

    return Number(result.rowCount || 0);
  }

  async addReactionRole({
    guildId,
    channelId,
    messageId,
    emojiKey,
    emojiDisplay,
    roleId,
    createdByUserId = null
  }) {
    await this.ensureInitialized();

    const result = await this.pool.query(
      `
        INSERT INTO reaction_roles (
          guild_id, channel_id, message_id, emoji_key, emoji_display,
          role_id, created_by_user_id, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT(guild_id, channel_id, message_id, emoji_key, role_id)
        DO NOTHING
      `,
      [guildId, channelId, messageId, emojiKey, emojiDisplay, roleId, createdByUserId, nowIso()]
    );

    return Number(result.rowCount || 0) > 0;
  }

  async listReactionRoles(guildId, messageId = null) {
    await this.ensureInitialized();

    let rows;
    if (messageId == null) {
      rows = await this.queryRows(
        `
          SELECT id, channel_id, message_id, emoji_key, emoji_display, role_id, created_by_user_id, created_at
          FROM reaction_roles
          WHERE guild_id = $1
          ORDER BY message_id DESC, emoji_display ASC, role_id ASC
        `,
        [guildId]
      );
    } else {
      rows = await this.queryRows(
        `
          SELECT id, channel_id, message_id, emoji_key, emoji_display, role_id, created_by_user_id, created_at
          FROM reaction_roles
          WHERE guild_id = $1 AND message_id = $2
          ORDER BY emoji_display ASC, role_id ASC
        `,
        [guildId, messageId]
      );
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

  async getReactionRoleIds(guildId, channelId, messageId, emojiKey) {
    await this.ensureInitialized();

    const rows = await this.queryRows(
      `
        SELECT role_id
        FROM reaction_roles
        WHERE guild_id = $1 AND channel_id = $2 AND message_id = $3 AND emoji_key = $4
        ORDER BY role_id ASC
      `,
      [guildId, channelId, messageId, emojiKey]
    );

    return rows.map((row) => toSnowflakeText(row.role_id)).filter(Boolean);
  }

  async removeReactionRole(guildId, channelId, messageId, emojiKey, roleId = null) {
    await this.ensureInitialized();

    let result;
    if (roleId) {
      result = await this.pool.query(
        `
          DELETE FROM reaction_roles
          WHERE guild_id = $1 AND channel_id = $2 AND message_id = $3 AND emoji_key = $4 AND role_id = $5
        `,
        [guildId, channelId, messageId, emojiKey, roleId]
      );
    } else {
      result = await this.pool.query(
        `
          DELETE FROM reaction_roles
          WHERE guild_id = $1 AND channel_id = $2 AND message_id = $3 AND emoji_key = $4
        `,
        [guildId, channelId, messageId, emojiKey]
      );
    }

    return Number(result.rowCount || 0);
  }

  async clearReactionRolesForMessage(guildId, channelId, messageId) {
    await this.ensureInitialized();

    const result = await this.pool.query(
      `
        DELETE FROM reaction_roles
        WHERE guild_id = $1 AND channel_id = $2 AND message_id = $3
      `,
      [guildId, channelId, messageId]
    );

    return Number(result.rowCount || 0);
  }
}
