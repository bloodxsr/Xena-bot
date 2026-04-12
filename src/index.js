import http from "node:http";

import { Client, Events, PermissionFlags, parseUserMention } from "@fluxerjs/core";

import { loadConfig } from "./utilities/config.js";
import { BotDatabase } from "./admin/database.js";
import { createAdminCommandHandlers } from "./admin/commands.js";
import {
  normalizeEmojiInput,
  emojiKeyCandidatesFromGatewayEmoji,
  emojiRouteTokenFromNormalized
} from "./utilities/emoji.js";
import { createModerationCommandHandlers } from "./moderation/commands.js";
import { RaidMlClient } from "./moderation/raidMlClient.js";
import { RaidRiskEngine, SpamRiskEngine, snowflakeToDate } from "./moderation/riskSignals.js";
import { createUtilityCommandHandlers } from "./utilities/commands.js";
import { WordStore } from "./moderation/words.js";

const config = loadConfig();
const db = new BotDatabase(config.databasePath);
const riskEngine = new RaidRiskEngine(config.raidMl);
const spamEngine = new SpamRiskEngine(config.automod);
const wordStore = new WordStore(config.wordsJsonPath);
const aiLastUsedByUser = new Map();

wordStore.load();

const client = new Client({ intents: 0 });

function logInfo(...args) {
  console.log(new Date().toISOString(), "|", ...args);
}

function logError(...args) {
  console.error(new Date().toISOString(), "|", ...args);
}

const raidMlClient = new RaidMlClient(config.raidMl, logError);
const raidMlHealthState = {
  online: null,
  monitorHandle: null
};

function logRaidMlStartupState(online) {
  if (online) {
    logInfo(`raid ml sidecar online at ${config.raidMl.serviceUrl}`);
    return;
  }

  logError(`raid ml sidecar unavailable at ${config.raidMl.serviceUrl}; using local JS fallback.`);
}

function logRaidMlTransition(previousOnline, currentOnline) {
  if (previousOnline === currentOnline) {
    return;
  }

  if (previousOnline === true && currentOnline === false) {
    logError(`raid ml sidecar disconnected; using local JS fallback.`);
    return;
  }

  if (previousOnline === false && currentOnline === true) {
    logInfo(`raid ml sidecar reconnected at ${config.raidMl.serviceUrl}`);
  }
}

async function updateRaidMlHealthState({ startup = false } = {}) {
  if (!raidMlClient.enabled) {
    return false;
  }

  const previousOnline = raidMlHealthState.online;
  const currentOnline = await raidMlClient.checkHealth();
  raidMlHealthState.online = currentOnline;

  if (startup || previousOnline == null) {
    logRaidMlStartupState(currentOnline);
    return currentOnline;
  }

  logRaidMlTransition(previousOnline, currentOnline);
  return currentOnline;
}

function startRaidMlHealthMonitor() {
  if (!raidMlClient.enabled || raidMlHealthState.monitorHandle) {
    return;
  }

  const intervalMs = Math.max(2000, Number(config.raidMl.healthCheckIntervalMs || 10000));
  raidMlHealthState.monitorHandle = setInterval(() => {
    updateRaidMlHealthState().catch((error) => {
      logError("raid ml health monitor failed", error);
    });
  }, intervalMs);
}

function formatUserMention(userId) {
  return `<@${userId}>`;
}

function parseSnowflake(value) {
  const text = String(value ?? "").trim();
  return /^\d{5,22}$/.test(text) ? text : null;
}

function parseUserIdArg(arg) {
  const text = String(arg ?? "").trim();
  if (!text) {
    return null;
  }

  const fromMention = parseUserMention(text);
  if (fromMention) {
    return fromMention;
  }

  const cleaned = text.replace(/[<@!>]/g, "");
  return parseSnowflake(cleaned);
}

function toIsoSeconds(date) {
  const rounded = new Date(Math.floor(date.getTime() / 1000) * 1000);
  return rounded.toISOString().replace(".000Z", "Z");
}

function sanitizeReason(reason, maxLength = 180) {
  const cleaned = String(reason ?? "")
    .replace(/\s+/g, " ")
    .trim();

  if (cleaned.length <= maxLength) {
    return cleaned;
  }

  return `${cleaned.slice(0, maxLength - 3).trimEnd()}...`;
}

function formatWarningCounter(warningCount, maxWarnings) {
  const current = Math.max(0, Number(warningCount || 0));
  const max = Math.max(1, Number(maxWarnings || 1));

  if (current <= max) {
    return `warning ${current}/${max}.`;
  }

  return `warning threshold exceeded (${current} total, limit ${max}).`;
}

function formatWarningThresholdDetail(warningCount, maxWarnings) {
  const current = Math.max(0, Number(warningCount || 0));
  const max = Math.max(1, Number(maxWarnings || 1));

  if (current <= max) {
    return `${current}/${max}`;
  }

  return `${current} total, limit ${max}`;
}

function parsePrefixedCommand(messageContent) {
  const content = String(messageContent ?? "");
  const prefix = config.prefixes.find((entry) => content.startsWith(entry));
  if (!prefix) {
    return null;
  }

  const body = content.slice(prefix.length).trim();
  if (!body) {
    return null;
  }

  const parts = body.split(/\s+/);
  const command = String(parts.shift() || "").toLowerCase();

  return {
    prefix,
    body,
    command,
    args: parts
  };
}

function randomIntegerInRange(min, max) {
  const lower = Math.floor(Math.min(min, max));
  const upper = Math.floor(Math.max(min, max));
  if (lower === upper) {
    return lower;
  }

  return lower + Math.floor(Math.random() * (upper - lower + 1));
}

const EMBED_COLORS = {
  info: 0x1f6feb,
  success: 0x2ea043,
  warning: 0xd29922,
  error: 0xf85149
};

function truncateText(value, maxLength) {
  const text = String(value ?? "");
  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, Math.max(0, maxLength - 3)).trimEnd()}...`;
}

function buildEmbedPayload(description, options = {}) {
  const title = options.title ? truncateText(options.title, 256) : undefined;
  const kind = options.kind && EMBED_COLORS[options.kind] ? options.kind : "info";

  return {
    embeds: [
      {
        title,
        description: truncateText(description || "No content.", 4096),
        color: EMBED_COLORS[kind],
        timestamp: new Date().toISOString()
      }
    ]
  };
}

function normalizeRoleMentionIds(options = {}) {
  const ids = [];
  const seen = new Set();

  const push = (value) => {
    const id = parseSnowflake(value);
    if (!id || seen.has(id)) {
      return;
    }

    seen.add(id);
    ids.push(id);
  };

  push(options.roleId);

  if (Array.isArray(options.roleIds)) {
    for (const roleId of options.roleIds) {
      push(roleId);
    }
  }

  return ids;
}

function buildReplyContextLines(message, options = {}) {
  if (options.includeContext === false) {
    return [];
  }

  const lines = [];
  const userId = parseSnowflake(options.userId || message?.author?.id || message?.user?.id);
  const channelId = parseSnowflake(options.channelId || message?.channelId || message?.channel?.id);
  const guildId = parseSnowflake(options.guildId || message?.guildId || message?.guild?.id);
  const messageId = parseSnowflake(options.messageId || message?.id);
  const roleIds = normalizeRoleMentionIds(options);

  if (userId) {
    lines.push(`user: <@${userId}>`);
  }

  if (channelId) {
    lines.push(`channel: <#${channelId}>`);
  }

  if (guildId && channelId && messageId) {
    lines.push(`message: https://discord.com/channels/${guildId}/${channelId}/${messageId}`);
  } else if (messageId) {
    lines.push(`message_id: ${messageId}`);
  }

  if (roleIds.length > 0) {
    lines.push(`role: ${roleIds.map((id) => `<@&${id}>`).join(", ")}`);
  }

  return lines;
}

function appendContextToEmbedPayload(payload, message, options = {}) {
  if (!payload || typeof payload !== "object" || !Array.isArray(payload.embeds) || payload.embeds.length === 0) {
    return payload;
  }

  const contextLines = buildReplyContextLines(message, options);
  if (contextLines.length === 0) {
    return payload;
  }

  const firstEmbed = payload.embeds[0] || {};
  const baseDescription = String(firstEmbed.description || "No content.").trim();
  const contextBlock = contextLines.join("\n");

  payload.embeds[0] = {
    ...firstEmbed,
    description: truncateText(`${baseDescription}\n\n${contextBlock}`, 4096)
  };

  return payload;
}

function toReplyPayload(content, options = {}) {
  if (content && typeof content === "object") {
    return content;
  }

  return buildEmbedPayload(String(content ?? ""), options);
}

function isUnknownMessageError(error) {
  if (!error || typeof error !== "object") {
    return false;
  }

  if (String(error.code || "").toUpperCase() === "UNKNOWN_MESSAGE") {
    return true;
  }

  const statusCode = Number(error.statusCode || 0);
  const message = String(error.message || "").toLowerCase();
  return statusCode === 404 && message.includes("message wasn't found");
}

async function resolveReplyChannel(message) {
  if (message?.channel && typeof message.channel.send === "function") {
    return message.channel;
  }

  if (message?.channelId) {
    try {
      const channel = await client.channels.resolve(message.channelId);
      if (channel && typeof channel.send === "function") {
        return channel;
      }
    } catch {
      // Ignore channel resolve errors and continue with best effort fallbacks.
    }
  }

  if (typeof message?.send === "function") {
    return message;
  }

  return null;
}

async function safeReply(message, content, options = {}) {
  const payload = appendContextToEmbedPayload(toReplyPayload(content, options), message, options);

  try {
    await message.reply(payload);
    return;
  } catch (error) {
    const unknownMessage = isUnknownMessageError(error);
    if (!unknownMessage) {
      logError("embed reply failed", error);
    }

    const channel = await resolveReplyChannel(message);
    if (channel && typeof channel.send === "function") {
      try {
        await channel.send(payload);
        return;
      } catch (sendError) {
        logError("channel send fallback failed", sendError);
      }
    }

    if (!unknownMessage && typeof content === "string") {
      try {
        await message.reply(truncateText(content, 1900));
      } catch (fallbackError) {
        logError("reply fallback failed", fallbackError);
      }
    }
  }
}

async function resolveGuildFromMessage(message) {
  if (message.guild) {
    return message.guild;
  }

  if (!message.guildId) {
    return null;
  }

  try {
    return await client.guilds.resolve(message.guildId);
  } catch {
    return null;
  }
}

async function resolveGuildMember(guild, userId) {
  if (!guild || !userId) {
    return null;
  }

  try {
    return guild.members.get(userId) || (await guild.fetchMember(userId));
  } catch {
    return null;
  }
}

function memberRoleNames(member) {
  const names = new Set();
  const cache = member?.roles?.cache;
  if (!cache) {
    return names;
  }

  for (const role of cache.values()) {
    if (role?.name) {
      names.add(String(role.name).trim().toLowerCase());
    }
  }

  return names;
}

async function isStaffMember(message) {
  const guild = await resolveGuildFromMessage(message);
  if (!guild) {
    return false;
  }

  const member = await resolveGuildMember(guild, message.author?.id);
  if (!member) {
    return false;
  }

  if (member.permissions?.has(PermissionFlags.Administrator)) {
    return true;
  }

  const guildConfig = db.getGuildConfig(guild.id);
  const roleNames = memberRoleNames(member);
  const adminRoleName = String(guildConfig.admin_role_name || "").trim().toLowerCase();
  const modRoleName = String(guildConfig.mod_role_name || "").trim().toLowerCase();

  return (adminRoleName && roleNames.has(adminRoleName)) || (modRoleName && roleNames.has(modRoleName));
}

const TOTP_PROTECTED_PERMISSIONS = new Set(
  [
    PermissionFlags.Administrator,
    PermissionFlags.ManageGuild,
    PermissionFlags.ManageRoles,
    PermissionFlags.ManageMessages,
    PermissionFlags.ModerateMembers,
    PermissionFlags.KickMembers,
    PermissionFlags.BanMembers
  ].filter((value) => value != null)
);

function isTotpProtectedPermission(permissionFlag) {
  return TOTP_PROTECTED_PERMISSIONS.has(permissionFlag);
}

function resolveTotpAuthorization(record, authWindowDays) {
  const result = {
    enrolled: Boolean(record?.enabled && record?.secret_base32),
    authorized: false,
    expiresAt: null
  };

  if (!result.enrolled || !record?.last_verified_at) {
    return result;
  }

  const lastVerifiedAtMs = Date.parse(String(record.last_verified_at));
  if (!Number.isFinite(lastVerifiedAtMs)) {
    return result;
  }

  const ttlMs = Math.max(1, Number(authWindowDays || 30)) * 24 * 60 * 60 * 1000;
  const expiresAtMs = lastVerifiedAtMs + ttlMs;
  result.expiresAt = new Date(expiresAtMs).toISOString();
  result.authorized = Date.now() < expiresAtMs;

  return result;
}

async function hasPermission(message, permissionFlag) {
  const guild = await resolveGuildFromMessage(message);
  if (!guild) {
    return false;
  }

  const member = await resolveGuildMember(guild, message.author?.id);
  if (!member) {
    return false;
  }

  return (
    member.permissions?.has(permissionFlag) ||
    member.permissions?.has(PermissionFlags.Administrator) ||
    (await isStaffMember(message))
  );
}

async function requirePermission(
  message,
  permissionFlag,
  deniedText = "You do not have permission for this command.",
  options = {}
) {
  const allowed = await hasPermission(message, permissionFlag);
  if (!allowed) {
    await safeReply(message, deniedText);
    return false;
  }

  const skipTotp = options && options.skipTotp === true;
  if (skipTotp || !config.totp.enabled || !isTotpProtectedPermission(permissionFlag)) {
    return true;
  }

  const guildId = parseSnowflake(message?.guildId || message?.guild?.id);
  const userId = parseSnowflake(message?.author?.id);
  if (!guildId || !userId) {
    await safeReply(message, "TOTP verification requires a guild context and a resolvable user.", {
      title: "TOTP",
      kind: "warning"
    });
    return false;
  }

  const record = db.getStaffTotpAuth(guildId, userId);
  const totp = resolveTotpAuthorization(record, config.totp.authWindowDays);

  if (!totp.enrolled) {
    await safeReply(
      message,
      [
        "TOTP setup is required for protected staff commands.",
        "Run: totpsetup",
        "Then verify with: totpauth <6-digit-code>"
      ].join("\n"),
      {
        title: "TOTP Required",
        kind: "warning"
      }
    );
    return false;
  }

  if (!totp.authorized) {
    await safeReply(
      message,
      [
        `Your TOTP authorization expired or is missing (window: ${config.totp.authWindowDays} days).`,
        "Run: totpauth <6-digit-code>"
      ].join("\n"),
      {
        title: "TOTP Reverification Required",
        kind: "warning"
      }
    );
    return false;
  }

  return true;
}

async function generateAiText(prompt) {
  if (!config.ai.apiKey) {
    throw new Error("Google API key is not configured");
  }

  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    config.ai.modelName
  )}:generateContent?key=${encodeURIComponent(config.ai.apiKey)}`;

  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      contents: [
        {
          role: "user",
          parts: [{ text: prompt }]
        }
      ]
    })
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`AI request failed (${response.status}): ${text.slice(0, 300)}`);
  }

  const data = await response.json();
  const candidate = data?.candidates?.[0];
  const part = candidate?.content?.parts?.find((item) => typeof item?.text === "string");
  const text = String(part?.text ?? "").trim();

  if (!text) {
    throw new Error("AI response was empty");
  }

  return text;
}

function estimateProfileScore(member) {
  const hasAvatar = Boolean(member?.user?.avatar || member?.user?.avatarUrl);
  const displayName = String(member?.displayName || member?.nick || member?.user?.username || "").trim();

  let score = 0;
  if (hasAvatar) score += 0.55;
  if (displayName.length >= 3) score += 0.25;
  if (displayName && !displayName.toLowerCase().startsWith("user") && !displayName.toLowerCase().startsWith("member")) {
    score += 0.2;
  }

  return {
    hasAvatar,
    profileScore: Math.max(0, Math.min(score, 1))
  };
}

function buildTimeoutCandidates(durationSeconds) {
  const until = new Date(Date.now() + durationSeconds * 1000);
  const secondsIso = toIsoSeconds(until);
  const fullIso = until.toISOString();

  return [...new Set([secondsIso, fullIso.replace(".000Z", "Z"), fullIso])];
}

async function tryApplyTimeout(member, reason, durationSeconds) {
  const timeoutReason = sanitizeReason(reason);
  const attempted = [];

  for (const candidate of buildTimeoutCandidates(durationSeconds)) {
    attempted.push(candidate);
    try {
      await member.edit({
        communication_disabled_until: candidate,
        timeout_reason: timeoutReason
      });
      return {
        ok: true,
        until: candidate,
        attempted,
        error: null
      };
    } catch (error) {
      if (!String(error).includes("INVALID_FORM_BODY")) {
        return {
          ok: false,
          until: null,
          attempted,
          error: String(error)
        };
      }
    }
  }

  return {
    ok: false,
    until: null,
    attempted,
    error: "INVALID_FORM_BODY"
  };
}

async function sendVerificationDm(member, verificationUrl, reason) {
  const details = verificationUrl
    ? [
        "Your join is currently gated while staff review activity patterns.",
        `Reason: ${reason}`,
        `Complete verification here: ${verificationUrl}`
      ]
    : [
        "Your join is currently gated while staff review activity patterns.",
        `Reason: ${reason}`,
        "No verification website is configured right now.",
        "A staff member will review and approve or reject your join manually."
      ];

  try {
    if (member?.user && typeof member.user.send === "function") {
      await member.user.send(
        buildEmbedPayload(details.join("\n"), {
          title: "Join Verification",
          kind: "warning"
        })
      );
      return true;
    }
  } catch (error) {
    logError("failed to DM gated member", error);
  }

  return false;
}

function getEffectiveGateState(guildId) {
  const state = db.getRaidGateState(guildId);
  if (!state.gate_active || !state.gate_until) {
    return state;
  }

  const gateUntilMs = Date.parse(state.gate_until);
  if (!Number.isFinite(gateUntilMs)) {
    return state;
  }

  if (Date.now() >= gateUntilMs) {
    db.setRaidGateState(guildId, false, "Gate expired", null);
    return {
      gate_active: false,
      gate_reason: "Gate expired",
      gate_until: null,
      updated_at: new Date().toISOString()
    };
  }

  return state;
}

async function gateMember(member, signal, reason, guildConfig) {
  const guildId = member?.guild?.id || member?.guildId;
  const userId = member?.user?.id || member?.id;

  if (!guildId || !userId) {
    return "gated_failed";
  }

  db.upsertVerificationMember({
    guildId,
    userId,
    status: "pending",
    riskScore: signal.riskScore,
    verificationUrl: guildConfig.verification_url,
    reason
  });

  const dmSent = await sendVerificationDm(member, guildConfig.verification_url, reason);
  const metadata = {
    risk_score: Number(signal.riskScore.toFixed(4)),
    risk_level: signal.riskLevel,
    join_rate_per_minute: Number(signal.joinRatePerMinute.toFixed(3)),
    young_account_ratio: Number(signal.youngAccountRatio.toFixed(3)),
    dm_sent: dmSent,
    gate_mode: guildConfig.join_gate_mode
  };

  if (guildConfig.join_gate_mode === "kick") {
    try {
      await member.guild.kick(userId, { reason: sanitizeReason(`Join gated: ${reason}`) });
      db.logModerationAction({
        guildId,
        action: "join_gate_kick",
        actorUserId: client.user?.id ?? null,
        targetUserId: userId,
        reason,
        metadata
      });
      return "gated_kick";
    } catch (error) {
      db.logModerationAction({
        guildId,
        action: "join_gate_kick_failed",
        actorUserId: client.user?.id ?? null,
        targetUserId: userId,
        reason,
        metadata: {
          ...metadata,
          kick_error: String(error)
        }
      });
      return "gated_kick_failed";
    }
  }

  const timeoutResult = await tryApplyTimeout(member, `Join gated: ${reason}`, guildConfig.gate_duration_seconds);
  if (timeoutResult.ok) {
    db.logModerationAction({
      guildId,
      action: "join_gate_timeout",
      actorUserId: client.user?.id ?? null,
      targetUserId: userId,
      reason,
      metadata: {
        ...metadata,
        gate_until: new Date(Date.now() + guildConfig.gate_duration_seconds * 1000).toISOString(),
        timeout_until: timeoutResult.until,
        timeout_attempted_values: timeoutResult.attempted
      }
    });
    return "gated_timeout";
  }

  db.logModerationAction({
    guildId,
    action: "join_gate_timeout_failed",
    actorUserId: client.user?.id ?? null,
    targetUserId: userId,
    reason,
    metadata: {
      ...metadata,
      timeout_attempted_values: timeoutResult.attempted,
      timeout_error: timeoutResult.error
    }
  });
  return "gated_timeout_failed";
}

async function sendWelcomeForMember(member) {
  const guildId = member?.guild?.id || member?.guildId;
  const userId = member?.user?.id || member?.id;
  if (!guildId || !userId) {
    return;
  }

  const gateState = getEffectiveGateState(guildId);
  if (gateState.gate_active) {
    return;
  }

  if (db.isMemberPendingVerification(guildId, userId)) {
    return;
  }

  const guildConfig = db.getGuildConfig(guildId);
  const channelId = guildConfig.welcome_channel_id;
  if (!channelId) {
    return;
  }

  const lines = [
    `Welcome ${formatUserMention(userId)}.`,
    "Please review the server resources below."
  ];

  if (guildConfig.rules_channel_id) lines.push(`Rules: <#${guildConfig.rules_channel_id}>`);
  if (guildConfig.chat_channel_id) lines.push(`Chat: <#${guildConfig.chat_channel_id}>`);
  if (guildConfig.help_channel_id) lines.push(`Help: <#${guildConfig.help_channel_id}>`);
  if (guildConfig.about_channel_id) lines.push(`About: <#${guildConfig.about_channel_id}>`);
  if (guildConfig.perks_channel_id) lines.push(`Perks: <#${guildConfig.perks_channel_id}>`);

  try {
    const channel = await client.channels.resolve(channelId);
    if (channel && typeof channel.send === "function") {
      await channel.send(
        buildEmbedPayload(lines.join("\n"), {
          title: "Welcome",
          kind: "success"
        })
      );
    }
  } catch (error) {
    logError("failed to send welcome message", error);
  }
}

async function handleMemberJoin(member) {
  const guildId = member?.guild?.id || member?.guildId;
  const userId = member?.user?.id || member?.id;

  if (!guildId || !userId) {
    return;
  }

  if (member?.user?.bot) {
    return;
  }

  const guildConfig = db.getGuildConfig(guildId);
  const gateState = getEffectiveGateState(guildId);

  const createdAt = snowflakeToDate(userId);
  const accountAgeDays = Math.max(0, (Date.now() - createdAt.getTime()) / 86400000);
  const profile = estimateProfileScore(member);

  const joinInput = {
    guildId,
    accountAgeDays,
    hasAvatar: profile.hasAvatar,
    profileScore: profile.profileScore,
    windowSeconds: guildConfig.raid_monitor_window_seconds,
    joinRateThreshold: guildConfig.raid_join_rate_threshold
  };

  let signal = null;
  if (raidMlClient.enabled) {
    const sidecarSignal = await raidMlClient.evaluateJoin(joinInput);
    if (sidecarSignal && Number.isFinite(Number(sidecarSignal.riskScore))) {
      signal = sidecarSignal;
    }
  }

  if (!signal) {
    signal = riskEngine.evaluateJoin(joinInput);
  }

  const riskScore = Number(signal.riskScore || 0);
  const modelConfidence = Number(signal.modelConfidence ?? 0.5);
  const heuristicScore = Number(signal.heuristicScore ?? riskScore);
  const adaptiveScore = Number(signal.adaptiveScore ?? riskScore);
  const riskLevel =
    typeof signal.riskLevel === "string"
      ? signal.riskLevel
      : riskScore >= 0.82
        ? "high"
        : riskScore >= 0.6
          ? "medium"
          : "low";

  const threshold = Number(guildConfig.raid_gate_threshold);
  const suspicious = riskScore >= threshold;
  const cautious = riskScore >= Math.max(0.5, threshold - 0.1);

  let gateActive = Boolean(gateState.gate_active);
  let gateReason = String(gateState.gate_reason || "");
  let action = "allow";

  if (suspicious && !gateActive) {
    const gateUntil = new Date(Date.now() + guildConfig.gate_duration_seconds * 1000);
    gateReason =
      `Automated anti-raid trigger in closed testing. ${signal.explanation}. ` +
      `risk=${riskScore.toFixed(3)} confidence=${modelConfidence.toFixed(3)}`;
    db.setRaidGateState(guildId, true, gateReason, gateUntil.toISOString());
    gateActive = true;
  }

  const shouldGate = gateActive || cautious;
  if (shouldGate) {
    if (!gateReason) {
      gateReason =
        `Precautionary join gating while staff review suspicious pattern ` +
        `(risk=${riskScore.toFixed(3)}, confidence=${modelConfidence.toFixed(3)}, ${signal.explanation}).`;
    }

    action = await gateMember(member, signal, gateReason, guildConfig);
  }

  db.logJoinEvent({
    guildId,
    userId,
    accountAgeDays: Number(signal.accountAgeDays ?? accountAgeDays),
    hasAvatar: Boolean(signal.hasAvatar ?? profile.hasAvatar),
    profileScore: Number(signal.profileScore ?? profile.profileScore),
    joinRate: Number(signal.joinRatePerMinute || 0),
    youngAccountRatio: Number(signal.youngAccountRatio || 0),
    riskScore,
    riskLevel,
    action,
    metadata: {
      explanation: String(signal.explanation || "signals unavailable"),
      gate_active: gateActive,
      model_confidence: Number(modelConfidence.toFixed(4)),
      heuristic_score: Number(heuristicScore.toFixed(4)),
      adaptive_score: Number(adaptiveScore.toFixed(4)),
      anomaly: signal.anomaly || null,
      suspicious_activity: signal.suspiciousActivity || null,
      model_state: signal.modelState || null,
      backend: raidMlClient.enabled ? "rust-sidecar" : "local-js"
    }
  });

  if (!action.startsWith("gated")) {
    await sendWelcomeForMember(member);
  }
}

async function applyReactionRole(reaction, emoji, userId, channelId, messageId, remove) {
  const guildId = parseSnowflake(reaction?.guildId || reaction?.guild_id || reaction?.message?.guildId || reaction?.message?.guild_id);
  const resolvedUserId = parseSnowflake(userId || reaction?.userId || reaction?.user_id || reaction?.user?.id);
  const resolvedChannelId = parseSnowflake(
    channelId || reaction?.channelId || reaction?.channel_id || reaction?.message?.channelId || reaction?.message?.channel_id
  );
  const resolvedMessageId = parseSnowflake(
    messageId || reaction?.messageId || reaction?.message_id || reaction?.message?.id
  );

  if (!guildId || !resolvedUserId || !resolvedChannelId || !resolvedMessageId) {
    return;
  }

  if (client.user?.id && resolvedUserId === client.user.id) {
    return;
  }

  const emojiKeys = emojiKeyCandidatesFromGatewayEmoji(emoji || reaction?.emoji);
  if (emojiKeys.length === 0) {
    return;
  }

  let roleIds = [];
  let matchedEmojiKey = "";
  let fallbackMappingUsed = false;

  for (const key of emojiKeys) {
    const candidateRoleIds = db.getReactionRoleIds(guildId, resolvedChannelId, resolvedMessageId, key);
    if (candidateRoleIds.length > 0) {
      roleIds = candidateRoleIds;
      matchedEmojiKey = key;
      break;
    }
  }

  if (roleIds.length === 0) {
    const messageMappings = db
      .listReactionRoles(guildId, resolvedMessageId)
      .filter((entry) => entry.channel_id === resolvedChannelId);

    if (messageMappings.length === 1 && messageMappings[0].role_id) {
      roleIds = [messageMappings[0].role_id];
      matchedEmojiKey = String(messageMappings[0].emoji_key || "");
      fallbackMappingUsed = true;
    }
  }

  if (roleIds.length === 0) {
    return;
  }

  try {
    const guild = client.guilds.get(guildId) || (await client.guilds.resolve(guildId));
    const member = await resolveGuildMember(guild, resolvedUserId);
    if (!member || member.user?.bot) {
      return;
    }

    const appliedRoleIds = [];
    const failedRoleIds = [];

    for (const roleId of roleIds) {
      try {
        if (remove) {
          await member.roles.remove(roleId);
        } else {
          await member.roles.add(roleId);
        }
        appliedRoleIds.push(roleId);
      } catch {
        failedRoleIds.push(roleId);
      }
    }

    db.logModerationAction({
      guildId,
      action: remove ? "reaction_role_revoke" : "reaction_role_grant",
      actorUserId: client.user?.id ?? null,
      targetUserId: resolvedUserId,
      reason: `emoji=${matchedEmojiKey || emojiKeys[0]} message=${resolvedMessageId}`,
      channelId: resolvedChannelId,
      messageId: resolvedMessageId,
      metadata: {
        emoji_key: matchedEmojiKey || emojiKeys[0],
        emoji_keys_seen: emojiKeys,
        fallback_mapping_used: fallbackMappingUsed,
        applied_role_ids: appliedRoleIds,
        failed_role_ids: failedRoleIds
      }
    });
  } catch (error) {
    logError("failed applying reaction role", error);
  }
}

async function handleSpamModeration(message) {
  if (!config.automod.spamDetectionEnabled) {
    return false;
  }

  if (!message.guildId || !message.content || message.author?.bot) {
    return false;
  }

  const guild = await resolveGuildFromMessage(message);
  if (!guild) {
    return false;
  }

  if (await isStaffMember(message)) {
    return false;
  }

  const actorId = message.author.id;
  const signal = spamEngine.evaluateMessage({
    guildId: guild.id,
    userId: actorId,
    content: message.content,
    createdAtMs: Date.now()
  });

  if (!signal.isSpam || signal.score < config.automod.warningOnlyThreshold) {
    return false;
  }

  const reasonsText = signal.reasons.length > 0 ? signal.reasons.join(", ") : "spam-like activity";

  try {
    if (typeof message.delete === "function") {
      await message.delete();
    }
  } catch {
    // Best effort.
  }

  const warningCount = db.incrementWarning(
    guild.id,
    actorId,
    `Spam detected: ${reasonsText}`,
    client.user?.id ?? null
  );

  let timeoutApplied = false;
  let timeoutError = null;
  let timeoutSeconds = 0;
  let timeoutAttemptedValues = [];

  const shouldTimeout =
    signal.score >= config.automod.spamScoreMuteThreshold || warningCount >= config.maxWarnings;

  if (shouldTimeout) {
    const member = await resolveGuildMember(guild, actorId);
    if (member) {
      timeoutSeconds = signal.severity === "high" ? config.automod.severeTimeoutSeconds : config.automod.timeoutSeconds;
      const timeoutResult = await tryApplyTimeout(member, `Automod spam: ${reasonsText}`, timeoutSeconds);
      timeoutApplied = timeoutResult.ok;
      timeoutError = timeoutResult.ok ? null : timeoutResult.error;
      timeoutAttemptedValues = timeoutResult.attempted;
    }
  }

  const guildConfig = db.getGuildConfig(guild.id);
  let raidEscalated = false;
  let escalation = null;

  if (signal.score >= config.automod.spamScoreMuteThreshold) {
    const escalationInput = {
      guildId: guild.id,
      userId: actorId,
      windowSeconds: config.automod.raidEscalationWindowSeconds,
      score: signal.score
    };

    if (raidMlClient.enabled) {
      escalation = await raidMlClient.recordSuspiciousActivity(escalationInput);
    }

    if (!escalation) {
      escalation = riskEngine.recordSuspiciousActivity(escalationInput);
    }

    const escalationScore = Number(escalation.suspiciousScore || 0);
    const escalationRate = Number(escalation.eventRatePerMinute || 0);

    const shouldEscalate =
      (escalation.eventCount >= config.automod.raidEscalationEventThreshold &&
        escalation.uniqueUsers >= config.automod.raidEscalationUserThreshold) ||
      escalationScore >= 1.2 ||
      (escalation.uniqueUsers >= config.automod.raidEscalationUserThreshold &&
        escalationRate >= Math.max(6, config.automod.raidEscalationEventThreshold));

    if (shouldEscalate) {
      const state = getEffectiveGateState(guild.id);
      if (!state.gate_active) {
        const gateReason =
          `Automatic raid gate from spam surge: ${escalation.eventCount} suspicious events from ` +
          `${escalation.uniqueUsers} users in ${escalation.windowSeconds}s (score=${escalationScore.toFixed(3)}).`;
        const gateUntil = new Date(Date.now() + guildConfig.gate_duration_seconds * 1000).toISOString();
        db.setRaidGateState(guild.id, true, gateReason, gateUntil);
        raidEscalated = true;
      }
    }
  }

  db.logModerationAction({
    guildId: guild.id,
    action: timeoutApplied ? "automod_spam_timeout" : "automod_spam_warn",
    actorUserId: client.user?.id ?? null,
    targetUserId: actorId,
    reason: `Spam detected: ${reasonsText}`,
    channelId: message.channelId,
    messageId: message.id,
    metadata: {
      score: Number(signal.score.toFixed(4)),
      severity: signal.severity,
      reasons: signal.reasons,
      metrics: signal.metrics,
      warning_count: warningCount,
      max_warnings: config.maxWarnings,
      timeout_applied: timeoutApplied,
      timeout_seconds: timeoutSeconds,
      timeout_error: timeoutError,
      timeout_attempted_values: timeoutAttemptedValues,
      raid_escalated: raidEscalated,
      raid_escalation_state: escalation
    }
  });

  const responseLines = [
    `${formatUserMention(actorId)} flagged for spam (${reasonsText}).`,
    formatWarningCounter(warningCount, config.maxWarnings)
  ];

  if (timeoutApplied) {
    responseLines.push(`Auto-timeout applied for ${Math.max(1, Math.round(timeoutSeconds / 60))} minute(s).`);
  } else if (shouldTimeout && timeoutError) {
    responseLines.push(`Timeout failed: ${timeoutError}`);
  }

  if (raidEscalated) {
    responseLines.push("Raid gate enabled automatically due to coordinated spam.");
  }

  await safeReply(message, responseLines.join("\n"), {
    title: "Auto Moderation",
    kind: timeoutApplied || raidEscalated ? "warning" : "info"
  });

  return true;
}

async function handleWordModeration(message) {
  if (!message.guildId || !message.content || message.author?.bot) {
    return false;
  }

  const blockedWord = wordStore.findBlockedWord(message.content);
  if (!blockedWord) {
    return false;
  }

  const guild = await resolveGuildFromMessage(message);
  if (!guild) {
    return false;
  }

  const actorId = message.author.id;
  const warningCount = db.incrementWarning(guild.id, actorId, `Blocked word: ${blockedWord}`, client.user?.id ?? null);

  try {
    if (typeof message.delete === "function") {
      await message.delete();
    }
  } catch {
    // Best effort.
  }

  db.logModerationAction({
    guildId: guild.id,
    action: "automod_warn",
    actorUserId: client.user?.id ?? null,
    targetUserId: actorId,
    reason: `Blocked word detected: ${blockedWord}`,
    channelId: message.channelId,
    messageId: message.id,
    metadata: {
      warning_count: warningCount,
      max_warnings: config.maxWarnings
    }
  });

  if (warningCount >= config.maxWarnings) {
    const thresholdDetail = formatWarningThresholdDetail(warningCount, config.maxWarnings);
    const member = await resolveGuildMember(guild, actorId);
    if (member) {
      const timeoutSeconds = config.automod.timeoutSeconds;
      const timeoutResult = await tryApplyTimeout(
        member,
        `Blocked word threshold reached (${thresholdDetail})`,
        timeoutSeconds
      );

      if (timeoutResult.ok) {
        db.logModerationAction({
          guildId: guild.id,
          action: "automod_word_timeout",
          actorUserId: client.user?.id ?? null,
          targetUserId: actorId,
          reason: `Blocked word threshold reached (${thresholdDetail})`,
          metadata: {
            blocked_word: blockedWord,
            timeout_seconds: timeoutSeconds,
            timeout_until: timeoutResult.until,
            timeout_attempted_values: timeoutResult.attempted
          }
        });

        await safeReply(
          message,
          `${formatUserMention(actorId)} was auto-muted after repeated blocked words.`,
          { title: "Auto Moderation", kind: "warning" }
        );
        return true;
      }
    }

    try {
      await guild.kick(actorId, { reason: `Exceeded warnings (${warningCount})` });
      db.resetWarnings(guild.id, actorId);
      db.logModerationAction({
        guildId: guild.id,
        action: "automod_kick",
        actorUserId: client.user?.id ?? null,
        targetUserId: actorId,
        reason: `Exceeded warnings (${warningCount})`
      });
      await safeReply(message, `${formatUserMention(actorId)} was removed after exceeding warning limit.`, {
        title: "Auto Moderation",
        kind: "error"
      });
      return true;
    } catch (error) {
      await safeReply(message, `Warning recorded for ${formatUserMention(actorId)} but kick failed: ${String(error)}`, {
        title: "Auto Moderation",
        kind: "warning"
      });
      return true;
    }
  }

  await safeReply(
    message,
    `${formatUserMention(actorId)} ${formatWarningCounter(warningCount, config.maxWarnings)} Blocked word detected.`,
    {
      title: "Auto Moderation",
      kind: "warning"
    }
  );

  return true;
}

async function sendLevelUpAnnouncement(message, guildConfig, levelSnapshot) {
  if (!config.leveling.announceLevelUp) {
    return;
  }

  const channelId = guildConfig.leveling_channel_id || message.channelId;
  if (!channelId) {
    return;
  }

  let channel = null;
  try {
    channel = await client.channels.resolve(channelId);
  } catch {
    channel = null;
  }

  if (!channel || typeof channel.send !== "function") {
    channel = await resolveReplyChannel(message);
  }

  if (!channel || typeof channel.send !== "function") {
    return;
  }

  try {
    await channel.send(
      buildEmbedPayload(
        [
          `${formatUserMention(levelSnapshot.user_id)} reached level ${levelSnapshot.level}.`,
          `xp: ${levelSnapshot.xp}`,
          `messages: ${levelSnapshot.message_count}`
        ].join("\n"),
        {
          title: "Level Up",
          kind: "success"
        }
      )
    );
  } catch (error) {
    logError("failed to send level up announcement", error);
  }
}

async function handleLevelingMessage(message, parsedCommand = null) {
  if (!message.guildId || message.author?.bot) {
    return;
  }

  const guild = await resolveGuildFromMessage(message);
  if (!guild) {
    return;
  }

  const guildConfig = db.getGuildConfig(guild.id);

  if (config.leveling.ignoreCommandMessages) {
    const parsed = parsedCommand || parsePrefixedCommand(message.content);
    if (parsed) {
      return;
    }
  }

  const content = String(message.content || "").trim();
  if (content.length < config.leveling.minMessageLength) {
    return;
  }

  const xpGain = randomIntegerInRange(config.leveling.minXpPerMessage, config.leveling.maxXpPerMessage);
  const levelSnapshot = db.addMemberXp({
    guildId: guild.id,
    userId: message.author.id,
    xpGain,
    cooldownSeconds: config.leveling.cooldownSeconds
  });

  if (!levelSnapshot.leveled_up || levelSnapshot.applied_xp <= 0 || levelSnapshot.cooldown_active) {
    return;
  }

  await sendLevelUpAnnouncement(message, guildConfig, levelSnapshot);
}

const commandHandlers = {
  ...createUtilityCommandHandlers({
    safeReply,
    config,
    aiLastUsedByUser,
    generateAiText,
    db,
    resolveGuildFromMessage,
    parseUserIdArg,
    formatUserMention
  }),
  ...createAdminCommandHandlers({
    PermissionFlags,
    requirePermission,
    resolveGuildFromMessage,
    safeReply,
    db,
    parseSnowflake,
    config
  }),
  ...createModerationCommandHandlers({
    PermissionFlags,
    requirePermission,
    resolveGuildFromMessage,
    safeReply,
    wordStore,
    db,
    parseUserIdArg,
    formatUserMention,
    parseSnowflake,
    client,
    sanitizeReason,
    resolveGuildMember,
    toIsoSeconds,
    getEffectiveGateState,
    sendWelcomeForMember,
    normalizeEmojiInput,
    emojiRouteTokenFromNormalized
  })
};
async function executeCommand(parsed, message) {
  const handler = commandHandlers[parsed.command];
  if (!handler) {
    return;
  }

  try {
    await handler({
      message,
      args: parsed.args,
      body: parsed.body,
      command: parsed.command
    });
  } catch (error) {
    logError(`command failed: ${parsed.command}`, error);
    await safeReply(message, `Command failed: ${String(error)}`);
  }
}

function startUptimeServer() {
  if (!config.uptime.enabled) {
    return;
  }

  const server = http.createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("ok\n");
  });

  server.listen(config.uptime.port, config.uptime.host, () => {
    logInfo(`uptime server listening on ${config.uptime.host}:${config.uptime.port}`);
  });
}

client.on(Events.Ready, () => {
  logInfo(`ready as ${client.user?.username || client.user?.id || "bot"}`);
});

client.on(Events.Error, (error) => {
  logError("client error", error);
});

client.on(Events.GuildMemberAdd, async (member) => {
  try {
    await handleMemberJoin(member);
  } catch (error) {
    logError("guild member add handler failed", error);
  }
});

client.on(Events.MessageReactionAdd, async (reaction, user, messageId, channelId, emoji, userId) => {
  try {
    const resolvedUserId = userId || user?.id || null;
    await applyReactionRole(reaction, emoji, resolvedUserId, channelId, messageId, false);
  } catch (error) {
    logError("reaction add handler failed", error);
  }
});

client.on(Events.MessageReactionRemove, async (reaction, user, messageId, channelId, emoji, userId) => {
  try {
    const resolvedUserId = userId || user?.id || null;
    await applyReactionRole(reaction, emoji, resolvedUserId, channelId, messageId, true);
  } catch (error) {
    logError("reaction remove handler failed", error);
  }
});

client.on(Events.MessageCreate, async (message) => {
  try {
    if (message.author?.bot) {
      return;
    }

    const spamBlocked = await handleSpamModeration(message);
    if (spamBlocked) {
      return;
    }

    const blocked = await handleWordModeration(message);
    if (blocked) {
      return;
    }

    const parsed = parsePrefixedCommand(message.content);
    await handleLevelingMessage(message, parsed);

    if (!parsed) {
      return;
    }

    await executeCommand(parsed, message);
  } catch (error) {
    logError("message handler failed", error);
  }
});

async function start() {
  startUptimeServer();

  if (raidMlClient.enabled) {
    await updateRaidMlHealthState({ startup: true });
    startRaidMlHealthMonitor();
  }

  await client.login(config.botToken);
}

start().catch((error) => {
  logError("bot startup failed", error);
  process.exitCode = 1;
});

