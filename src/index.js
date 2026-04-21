import http from "node:http";

import { Client, Events, PermissionFlags, parseUserMention } from "@fluxerjs/core";

import { loadConfig } from "./utilities/config.js";
import { BotDatabase } from "./admin/database.js";
import { PostgresBotDatabase } from "./admin/database-postgres.js";
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
import { createMusicRuntime } from "./utilities/music.js";
import { renderWelcomeCardImage } from "./utilities/welcome-card-image.js";
import { WordStore } from "./moderation/words.js";

const config = loadConfig();
const db =
  config.databaseDriver === "postgres"
    ? new PostgresBotDatabase(config.postgres)
    : new BotDatabase(config.databasePath);
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

const musicRuntime = createMusicRuntime({
  client,
  logError
});

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

function formatInteger(value) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) {
    return "0";
  }

  return Math.trunc(numeric).toLocaleString("en-US");
}

function buildProgressBar(current, total, size = 16) {
  const normalizedTotal = Math.max(1, Number(total || 1));
  const normalizedCurrent = Math.max(0, Math.min(Number(current || 0), normalizedTotal));
  const filled = Math.round((normalizedCurrent / normalizedTotal) * size);
  const clampedFilled = Math.max(0, Math.min(filled, size));
  return `${"=".repeat(clampedFilled)}${".".repeat(size - clampedFilled)}`;
}

const NON_TOGGLEABLE_COMMANDS = new Set(["help"]);
const WELCOME_CARD_IMAGE_FILE = "welcome-card.png";

const TICKET_PERMISSION_BITS = {
  viewChannel: 1024n,
  sendMessages: 2048n,
  embedLinks: 16384n,
  attachFiles: 32768n,
  readMessageHistory: 65536n
};

const TICKET_ALLOW_MASK =
  TICKET_PERMISSION_BITS.viewChannel |
  TICKET_PERMISSION_BITS.sendMessages |
  TICKET_PERMISSION_BITS.embedLinks |
  TICKET_PERMISSION_BITS.attachFiles |
  TICKET_PERMISSION_BITS.readMessageHistory;

function resolveAvatarUrl(userLike) {
  if (!userLike || typeof userLike !== "object") {
    return null;
  }

  try {
    if (typeof userLike.displayAvatarURL === "function") {
      return String(userLike.displayAvatarURL({ extension: "png", size: 256 }));
    }
  } catch {
    // Best effort.
  }

  try {
    if (typeof userLike.avatarURL === "function") {
      return String(userLike.avatarURL({ extension: "png", size: 256 }));
    }
  } catch {
    // Best effort.
  }

  if (typeof userLike.avatarUrl === "string" && userLike.avatarUrl.trim()) {
    return userLike.avatarUrl.trim();
  }

  const userId = String(userLike.id || "").trim();
  const avatarHash = String(userLike.avatar || "").trim();
  if (userId && avatarHash) {
    const extension = avatarHash.startsWith("a_") ? "gif" : "png";
    return `https://cdn.discordapp.com/avatars/${userId}/${avatarHash}.${extension}?size=256`;
  }

  return null;
}

function sanitizeChannelNameFragment(value) {
  const text = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-\s_]/g, "")
    .replace(/[\s_]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");

  if (!text) {
    return "member";
  }

  return text.slice(0, 30);
}

function buildTicketChannelName(member, userId) {
  const sourceName =
    member?.displayName ||
    member?.nick ||
    member?.user?.globalName ||
    member?.user?.displayName ||
    member?.user?.username ||
    `user-${String(userId || "").slice(-4)}`;

  const clean = sanitizeChannelNameFragment(sourceName);
  const suffix = String(userId || "").slice(-4);
  return suffix ? `ticket-${clean}-${suffix}` : `ticket-${clean}`;
}

function buildTicketPermissionOverwrites({ guildId, userId, supportRoleId }) {
  const overwrites = [
    {
      id: guildId,
      type: 0,
      allow: "0",
      deny: String(TICKET_PERMISSION_BITS.viewChannel)
    },
    {
      id: userId,
      type: 1,
      allow: String(TICKET_ALLOW_MASK),
      deny: "0"
    }
  ];

  const roleId = parseSnowflake(supportRoleId);
  if (roleId) {
    overwrites.push({
      id: roleId,
      type: 0,
      allow: String(TICKET_ALLOW_MASK),
      deny: "0"
    });
  }

  return overwrites;
}

function emojiMatchesStoredTrigger(storedEmoji, gatewayEmoji) {
  const configured = String(storedEmoji || "").trim();
  if (!configured) {
    return false;
  }

  let normalized = null;
  try {
    normalized = normalizeEmojiInput(configured);
  } catch {
    return false;
  }

  const candidates = emojiKeyCandidatesFromGatewayEmoji(gatewayEmoji);
  if (candidates.length === 0) {
    return false;
  }

  const expected = new Set([String(normalized.key || "").trim(), String(normalized.display || "").trim()]);
  if (Array.isArray(normalized.aliases)) {
    for (const alias of normalized.aliases) {
      expected.add(String(alias || "").trim());
    }
  }

  for (const candidate of candidates) {
    if (expected.has(String(candidate || "").trim())) {
      return true;
    }
  }

  return false;
}

async function createTicketChannel({ guild, guildId, name, parentId, topic, permissionOverwrites }) {
  if (guild?.channels && typeof guild.channels.create === "function") {
    try {
      const created = await guild.channels.create({
        name,
        type: 0,
        parentId: parentId || undefined,
        topic,
        permissionOverwrites
      });

      if (created?.id) {
        return created;
      }
    } catch {
      // Fall back to direct REST create.
    }
  }

  const body = {
    name,
    type: 0,
    topic,
    permission_overwrites: Array.isArray(permissionOverwrites) ? permissionOverwrites : undefined,
    parent_id: parentId || undefined
  };

  return client.rest.post(`/guilds/${guildId}/channels`, { auth: true, body });
}

function renderMessageTemplate(template, values = {}) {
  const source = String(template || "");
  if (!source.trim()) {
    return "";
  }

  return source.replace(/\{([a-z0-9_.-]+)\}/gi, (full, token) => {
    const key = String(token || "").toLowerCase();
    if (!Object.prototype.hasOwnProperty.call(values, key)) {
      return full;
    }

    const replacement = values[key];
    return replacement == null ? "" : String(replacement);
  });
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
  if (options.includeContext !== true) {
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
    lines.push(`message: ${config.web.baseUrl}/channels/${guildId}/${channelId}/${messageId}`);
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

function scheduleMessageDeletion(sentMessage, deleteAfterMs) {
  const delayMs = Math.floor(Number(deleteAfterMs || 0));
  if (!sentMessage || typeof sentMessage.delete !== "function" || !Number.isFinite(delayMs) || delayMs <= 0) {
    return;
  }

  const timer = setTimeout(() => {
    Promise.resolve(sentMessage.delete()).catch(() => {
      // Best effort auto-delete.
    });
  }, delayMs);

  if (timer && typeof timer.unref === "function") {
    timer.unref();
  }
}

function waitFor(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, Math.max(0, Math.floor(Number(ms || 0))));
  });
}

function getHttpStatusCode(error) {
  const statusCode = Number(error?.statusCode || error?.status || 0);
  return Number.isFinite(statusCode) ? statusCode : 0;
}

function normalizeRetryAfterMs(value) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return 0;
  }

  // Retry-After is normally seconds; some clients expose milliseconds.
  return numeric <= 60 ? Math.floor(numeric * 1000) : Math.floor(numeric);
}

function getRetryAfterMs(error) {
  const fromField = normalizeRetryAfterMs(error?.retryAfter ?? error?.retry_after);
  if (fromField > 0) {
    return fromField;
  }

  const headers = error?.headers || error?.response?.headers;
  if (!headers || typeof headers !== "object") {
    return 0;
  }

  return normalizeRetryAfterMs(headers["retry-after"] ?? headers["Retry-After"]);
}

function isTransientSendError(error) {
  const statusCode = getHttpStatusCode(error);
  return statusCode === 429 || (statusCode >= 500 && statusCode <= 599);
}

function computeRetryDelayMs(error, attemptNumber) {
  const exponentialBase = 300 * Math.pow(2, Math.max(0, Number(attemptNumber || 1) - 1));
  const retryAfterMs = getRetryAfterMs(error);
  const jitterMs = Math.floor(Math.random() * 120);
  return Math.min(2500, Math.max(exponentialBase, retryAfterMs) + jitterMs);
}

async function sendWithRetry(sendFn, { label = "message send", maxAttempts = 2 } = {}) {
  let attempt = 0;

  while (attempt < maxAttempts) {
    attempt += 1;

    try {
      return await sendFn();
    } catch (error) {
      if (!isTransientSendError(error) || attempt >= maxAttempts) {
        throw error;
      }

      const delayMs = computeRetryDelayMs(error, attempt);
      const statusCode = getHttpStatusCode(error);
      logInfo(`${label} transient failure (${statusCode || "unknown"}); retrying in ${delayMs}ms (${attempt + 1}/${maxAttempts})`);
      await waitFor(delayMs);
    }
  }

  return null;
}

async function safeReply(message, content, options = {}) {
  const payload = appendContextToEmbedPayload(toReplyPayload(content, options), message, options);
  const autoDeleteMs = options?.deleteAfterMs;

  try {
    const sentMessage = await sendWithRetry(() => message.reply(payload), {
      label: "message.reply",
      maxAttempts: 2
    });
    scheduleMessageDeletion(sentMessage, autoDeleteMs);
    return sentMessage;
  } catch (error) {
    const unknownMessage = isUnknownMessageError(error);
    if (!unknownMessage) {
      logError("embed reply failed", error);
    }

    const channel = await resolveReplyChannel(message);
    if (channel && typeof channel.send === "function") {
      try {
        const sentMessage = await sendWithRetry(() => channel.send(payload), {
          label: "channel.send",
          maxAttempts: 2
        });
        scheduleMessageDeletion(sentMessage, autoDeleteMs);
        return sentMessage;
      } catch (sendError) {
        logError("channel send fallback failed", sendError);
      }
    }

    if (!unknownMessage && typeof content === "string") {
      try {
        const sentMessage = await sendWithRetry(() => message.reply(truncateText(content, 1900)), {
          label: "message.reply plain fallback",
          maxAttempts: 2
        });
        scheduleMessageDeletion(sentMessage, autoDeleteMs);
        return sentMessage;
      } catch (fallbackError) {
        logError("reply fallback failed", fallbackError);
      }
    }
  }

  return null;
}

const PAGINATION_REACTION_ORDER = Object.freeze(["⏪", "◀️", "▶️", "⏩"]);
const PAGINATION_ACTION_BY_EMOJI = new Map([
  ["⏪", "first"],
  ["⏮", "first"],
  ["◀", "previous"],
  ["▶", "next"],
  ["⏩", "last"],
  ["⏭", "last"]
]);
const PAGINATION_SESSION_TTL_MS = 15 * 60 * 1000;
const paginationSessions = new Map();

function normalizeEmojiSymbol(value) {
  return String(value || "")
    .trim()
    .replace(/\uFE0F/g, "");
}

function resolvePaginationAction(emoji) {
  const rawCandidates = [
    ...(Array.isArray(emojiKeyCandidatesFromGatewayEmoji(emoji)) ? emojiKeyCandidatesFromGatewayEmoji(emoji) : []),
    emoji?.name,
    emoji
  ];

  for (const candidate of rawCandidates) {
    const normalized = normalizeEmojiSymbol(candidate);
    if (!normalized) {
      continue;
    }

    const action = PAGINATION_ACTION_BY_EMOJI.get(normalized);
    if (action) {
      return action;
    }
  }

  return null;
}

function resolveReactionRouteToken(emoji) {
  const rawCandidates = Array.isArray(emojiKeyCandidatesFromGatewayEmoji(emoji))
    ? emojiKeyCandidatesFromGatewayEmoji(emoji)
    : [];

  for (const candidate of rawCandidates) {
    const text = String(candidate || "").trim();
    if (!text || /^\d{5,22}$/.test(text) || /^[^:]+:\d{5,22}$/.test(text)) {
      continue;
    }

    return text;
  }

  return String(emoji?.name || emoji || "").trim();
}

function clearPaginationSessionTimer(session) {
  if (session?.timer) {
    clearTimeout(session.timer);
    session.timer = null;
  }
}

function armPaginationSession(session) {
  clearPaginationSessionTimer(session);
  session.expiresAt = Date.now() + PAGINATION_SESSION_TTL_MS;
  session.timer = setTimeout(() => {
    paginationSessions.delete(session.messageId);
  }, PAGINATION_SESSION_TTL_MS);

  if (session.timer && typeof session.timer.unref === "function") {
    session.timer.unref();
  }
}

async function addBotReactionToMessage(channelId, messageId, emoji) {
  try {
    await client.rest.put(
      `/channels/${channelId}/messages/${messageId}/reactions/${encodeURIComponent(emoji)}/@me`,
      { auth: true }
    );
  } catch {
    // Best effort pagination controls.
  }
}

async function removeUserReactionFromMessage(channelId, messageId, emoji, userId) {
  const routeToken = resolveReactionRouteToken(emoji);
  if (!channelId || !messageId || !userId || !routeToken) {
    return;
  }

  try {
    await client.rest.delete(
      `/channels/${channelId}/messages/${messageId}/reactions/${encodeURIComponent(routeToken)}/${userId}`,
      { auth: true }
    );
  } catch {
    // Best effort cleanup of pagination reactions.
  }
}

async function ensurePaginationReactions(channelId, messageId, totalPages) {
  if (!channelId || !messageId || Math.max(1, Number(totalPages || 1)) <= 1) {
    return;
  }

  for (const emoji of PAGINATION_REACTION_ORDER) {
    await addBotReactionToMessage(channelId, messageId, emoji);
  }
}

async function editPaginatedMessage(session, payload) {
  let updatedMessage = null;

  if (typeof session?.message?.edit === "function") {
    try {
      updatedMessage = await session.message.edit(payload);
    } catch {
      updatedMessage = null;
    }
  }

  if (!updatedMessage) {
    try {
      const channel = await client.channels.resolve(session.channelId);
      const fetchedMessage =
        typeof channel?.messages?.fetch === "function"
          ? await channel.messages.fetch(session.messageId)
          : null;

      if (fetchedMessage && typeof fetchedMessage.edit === "function") {
        updatedMessage = await fetchedMessage.edit(payload);
      }
    } catch {
      updatedMessage = null;
    }
  }

  if (!updatedMessage && client.rest && typeof client.rest.patch === "function") {
    updatedMessage = await client.rest.patch(
      `/channels/${session.channelId}/messages/${session.messageId}`,
      { auth: true, body: payload }
    );
  }

  if (updatedMessage) {
    session.message = updatedMessage;
  }

  return updatedMessage;
}

async function registerPaginatedMessage({ sentMessage, currentPage, totalPages, getPagePayload }) {
  const messageId = parseSnowflake(sentMessage?.id);
  const channelId = parseSnowflake(sentMessage?.channelId || sentMessage?.channel?.id);
  if (!messageId || !channelId || typeof getPagePayload !== "function") {
    return false;
  }

  const existing = paginationSessions.get(messageId);
  if (existing) {
    clearPaginationSessionTimer(existing);
  }

  const session = {
    messageId,
    channelId,
    message: sentMessage,
    currentPage: Math.max(1, Number(currentPage || 1)),
    totalPages: Math.max(1, Number(totalPages || 1)),
    getPagePayload,
    expiresAt: Date.now() + PAGINATION_SESSION_TTL_MS,
    timer: null
  };

  armPaginationSession(session);
  paginationSessions.set(messageId, session);
  Promise.resolve(ensurePaginationReactions(channelId, messageId, session.totalPages)).catch(() => {
    // Best effort pagination controls.
  });
  return true;
}

async function handlePaginatedEmbedReaction(reaction, emoji, userId, channelId, messageId) {
  const resolvedUserId = parseSnowflake(userId || reaction?.userId || reaction?.user_id || reaction?.user?.id);
  const resolvedChannelId = parseSnowflake(
    channelId || reaction?.channelId || reaction?.channel_id || reaction?.message?.channelId || reaction?.message?.channel_id
  );
  const resolvedMessageId = parseSnowflake(
    messageId || reaction?.messageId || reaction?.message_id || reaction?.message?.id
  );

  if (!resolvedUserId || !resolvedChannelId || !resolvedMessageId) {
    return false;
  }

  if (client.user?.id && resolvedUserId === client.user.id) {
    return true;
  }

  const session = paginationSessions.get(resolvedMessageId);
  if (!session || session.channelId !== resolvedChannelId) {
    return false;
  }

  const action = resolvePaginationAction(emoji || reaction?.emoji);
  if (!action) {
    return false;
  }

  const nowMs = Date.now();
  if (session.expiresAt <= nowMs) {
    clearPaginationSessionTimer(session);
    paginationSessions.delete(resolvedMessageId);
    await removeUserReactionFromMessage(resolvedChannelId, resolvedMessageId, emoji || reaction?.emoji, resolvedUserId);
    return true;
  }

  armPaginationSession(session);

  try {
    let requestedPage = Math.max(1, Number(session.currentPage || 1));
    const knownTotalPages = Math.max(1, Number(session.totalPages || 1));

    if (action === "first") {
      requestedPage = 1;
    } else if (action === "previous") {
      requestedPage = Math.max(1, requestedPage - 1);
    } else if (action === "next") {
      requestedPage = Math.min(knownTotalPages, requestedPage + 1);
    } else if (action === "last") {
      requestedPage = knownTotalPages;
    }

    const nextState = await session.getPagePayload(requestedPage);
    if (!nextState || !nextState.payload) {
      return true;
    }

    const nextTotalPages = Math.max(1, Number(nextState.totalPages || 1));
    const nextPage = Math.max(1, Math.min(Number(nextState.page || requestedPage), nextTotalPages));

    if (nextPage !== session.currentPage || nextTotalPages !== session.totalPages) {
      await editPaginatedMessage(session, nextState.payload);
    }

    session.currentPage = nextPage;
    session.totalPages = nextTotalPages;
  } catch (error) {
    logError("pagination reaction update failed", error);
  } finally {
    await removeUserReactionFromMessage(resolvedChannelId, resolvedMessageId, emoji || reaction?.emoji, resolvedUserId);
  }

  return true;
}

const paginationRuntime = {
  registerPaginatedMessage
};

const messageGuildCache = new WeakMap();
const messageAuthorMemberCache = new WeakMap();
const messageStaffCache = new WeakMap();

function canCacheMessageObject(message) {
  return Boolean(message && typeof message === "object");
}

async function resolveGuildFromMessage(message) {
  if (canCacheMessageObject(message) && messageGuildCache.has(message)) {
    return messageGuildCache.get(message);
  }

  const promise = (async () => {
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
  })();

  if (canCacheMessageObject(message)) {
    messageGuildCache.set(message, promise);
  }

  return promise;
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

async function resolveAuthorMemberFromMessage(message, guild = null) {
  if (canCacheMessageObject(message) && messageAuthorMemberCache.has(message)) {
    return messageAuthorMemberCache.get(message);
  }

  const promise = (async () => {
    const resolvedGuild = guild || (await resolveGuildFromMessage(message));
    const authorId = parseSnowflake(message?.author?.id);
    if (!resolvedGuild || !authorId) {
      return null;
    }

    return resolveGuildMember(resolvedGuild, authorId);
  })();

  if (canCacheMessageObject(message)) {
    messageAuthorMemberCache.set(message, promise);
  }

  return promise;
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
  if (canCacheMessageObject(message) && messageStaffCache.has(message)) {
    return messageStaffCache.get(message);
  }

  const promise = (async () => {
    const member = await resolveAuthorMemberFromMessage(message);
    return Boolean(member?.permissions?.has(PermissionFlags.Administrator));
  })();

  if (canCacheMessageObject(message)) {
    messageStaffCache.set(message, promise);
  }

  return promise;
}

async function hasPermission(message, permissionFlag) {
  const member = await resolveAuthorMemberFromMessage(message);
  if (!member) {
    return false;
  }

  return Boolean(
    member.permissions?.has(permissionFlag) ||
    member.permissions?.has(PermissionFlags.Administrator)
  );
}

async function requirePermission(
  message,
  permissionFlag,
  deniedText = "You do not have permission for this command."
) {
  const allowed = await hasPermission(message, permissionFlag);
  if (!allowed) {
    await safeReply(message, deniedText);
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

async function getEffectiveGateState(guildId) {
  const state = await db.getRaidGateState(guildId);
  if (!state.gate_active || !state.gate_until) {
    return state;
  }

  const gateUntilMs = Date.parse(state.gate_until);
  if (!Number.isFinite(gateUntilMs)) {
    return state;
  }

  if (Date.now() >= gateUntilMs) {
    await db.setRaidGateState(guildId, false, "Gate expired", null);
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

  await db.upsertVerificationMember({
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
      await db.logModerationAction({
        guildId,
        action: "join_gate_kick",
        actorUserId: client.user?.id ?? null,
        targetUserId: userId,
        reason,
        metadata
      });
      return "gated_kick";
    } catch (error) {
      await db.logModerationAction({
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
    await db.logModerationAction({
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

  await db.logModerationAction({
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

  const gateState = await getEffectiveGateState(guildId);
  if (gateState.gate_active) {
    return;
  }

  if (await db.isMemberPendingVerification(guildId, userId)) {
    return;
  }

  const guildConfig = await db.getGuildConfig(guildId);
  const channelId = guildConfig.welcome_channel_id;
  if (!channelId) {
    return;
  }

  const memberCountCandidate = [
    Number(member?.guild?.memberCount),
    Number(member?.guild?.member_count),
    Number(member?.guild?.approximate_member_count)
  ].find((entry) => Number.isFinite(entry) && entry > 0);
  const memberCount = Math.max(0, Number.isFinite(memberCountCandidate) ? memberCountCandidate : 0);
  const guildName = String(member?.guild?.name || guildId);

  const templateValues = {
    "user.mention": formatUserMention(userId),
    "user.id": userId,
    "user.name": String(member?.user?.username || member?.displayName || userId),
    "guild.id": guildId,
    "guild.name": guildName,
    "server.member_count": formatInteger(memberCount),
    "channels.rules": guildConfig.rules_channel_id ? `<#${guildConfig.rules_channel_id}>` : "-",
    "channels.chat": guildConfig.chat_channel_id ? `<#${guildConfig.chat_channel_id}>` : "-",
    "channels.help": guildConfig.help_channel_id ? `<#${guildConfig.help_channel_id}>` : "-",
    "channels.about": guildConfig.about_channel_id ? `<#${guildConfig.about_channel_id}>` : "-",
    "channels.perks": guildConfig.perks_channel_id ? `<#${guildConfig.perks_channel_id}>` : "-"
  };

  const resourceLines = [];
  if (guildConfig.rules_channel_id) resourceLines.push(`Rules: <#${guildConfig.rules_channel_id}>`);
  if (guildConfig.chat_channel_id) resourceLines.push(`Chat: <#${guildConfig.chat_channel_id}>`);
  if (guildConfig.help_channel_id) resourceLines.push(`Help: <#${guildConfig.help_channel_id}>`);
  if (guildConfig.about_channel_id) resourceLines.push(`About: <#${guildConfig.about_channel_id}>`);
  if (guildConfig.perks_channel_id) resourceLines.push(`Perks: <#${guildConfig.perks_channel_id}>`);

  templateValues["channels.resources"] = resourceLines.join("\n");

  const templateSource = String(guildConfig.welcome_message_template || "");
  const renderedWelcome = renderMessageTemplate(templateSource, templateValues).trim();

  const lines = [renderedWelcome || `Welcome ${formatUserMention(userId)} to ${guildName}.`];
  if (!/\{channels\./i.test(templateSource) && resourceLines.length > 0) {
    lines.push(...resourceLines);
  }

  let welcomePayload = lines.join("\n");

  if (guildConfig.welcome_card_enabled) {
    const titleTemplate = String(guildConfig.welcome_card_title_template || "");
    const subtitleTemplate = String(guildConfig.welcome_card_subtitle_template || "");
    const titleText = renderMessageTemplate(titleTemplate, templateValues).trim() || `Welcome to ${guildName}`;
    const subtitleText =
      renderMessageTemplate(subtitleTemplate, templateValues).trim() ||
      `${String(member?.user?.username || member?.displayName || "Member")} joined the server.`;

    try {
      const imageData = await renderWelcomeCardImage({
        guildName,
        displayName: String(member?.displayName || member?.user?.username || "Member"),
        avatarUrl: resolveAvatarUrl(member?.user || member),
        memberCount,
        titleText,
        subtitleText,
        primaryColor: guildConfig.welcome_card_primary_color,
        accentColor: guildConfig.welcome_card_accent_color,
        overlayOpacity: guildConfig.welcome_card_overlay_opacity,
        backgroundUrl: guildConfig.welcome_card_background_url,
        fontStyle: guildConfig.welcome_card_font
      });

      welcomePayload = {
        content: lines.join("\n"),
        files: [
          {
            name: WELCOME_CARD_IMAGE_FILE,
            data: imageData
          }
        ]
      };
    } catch (error) {
      logError("failed to render welcome card image", error);
    }
  }

  try {
    const channel = await client.channels.resolve(channelId);
    if (channel && typeof channel.send === "function") {
      await channel.send(welcomePayload);
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

  const guildConfig = await db.getGuildConfig(guildId);
  const gateState = await getEffectiveGateState(guildId);

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
    await db.setRaidGateState(guildId, true, gateReason, gateUntil.toISOString());
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

  await db.logJoinEvent({
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

async function sendTicketTriggerNotice(channelId, messageText) {
  if (!channelId || !messageText) {
    return;
  }

  try {
    const channel = await client.channels.resolve(channelId);
    if (channel && typeof channel.send === "function") {
      await channel.send(String(messageText));
    }
  } catch {
    // Best effort notification.
  }
}

async function handleReactionTicketCreate(reaction, user, messageId, channelId, emoji, userId) {
  const guildId = parseSnowflake(reaction?.guildId || reaction?.guild_id || reaction?.message?.guildId || reaction?.message?.guild_id);
  const resolvedUserId = parseSnowflake(userId || user?.id || reaction?.userId || reaction?.user_id || reaction?.user?.id);
  const resolvedChannelId = parseSnowflake(
    channelId || reaction?.channelId || reaction?.channel_id || reaction?.message?.channelId || reaction?.message?.channel_id
  );
  const resolvedMessageId = parseSnowflake(messageId || reaction?.messageId || reaction?.message_id || reaction?.message?.id);

  if (!guildId || !resolvedUserId || !resolvedChannelId || !resolvedMessageId) {
    return;
  }

  if (client.user?.id && resolvedUserId === client.user.id) {
    return;
  }

  const guildConfig = await db.getGuildConfig(guildId);
  if (!guildConfig.ticket_enabled) {
    return;
  }

  if (!guildConfig.ticket_trigger_channel_id || !guildConfig.ticket_trigger_message_id) {
    return;
  }

  if (guildConfig.ticket_trigger_channel_id !== resolvedChannelId) {
    return;
  }

  if (guildConfig.ticket_trigger_message_id !== resolvedMessageId) {
    return;
  }

  if (!emojiMatchesStoredTrigger(guildConfig.ticket_trigger_emoji, emoji || reaction?.emoji)) {
    return;
  }

  let guild = null;
  try {
    guild = client.guilds.get(guildId) || (await client.guilds.resolve(guildId));
  } catch {
    guild = null;
  }

  if (!guild) {
    return;
  }

  const member = await resolveGuildMember(guild, resolvedUserId);
  if (!member || member.user?.bot) {
    return;
  }

  const existingTicket = await db.getOpenTicketForUser(guildId, resolvedUserId);
  if (existingTicket?.channel_id) {
    let existingChannel = null;
    try {
      existingChannel = await client.channels.resolve(existingTicket.channel_id);
    } catch {
      existingChannel = null;
    }

    if (existingChannel) {
      await sendTicketTriggerNotice(
        resolvedChannelId,
        `${formatUserMention(resolvedUserId)} you already have an open ticket: <#${existingTicket.channel_id}>.`
      );
      return;
    }

    await db.clearOpenTicketForUser(guildId, resolvedUserId);
  }

  const ticketChannelName = buildTicketChannelName(member, resolvedUserId);
  const supportRoleId = parseSnowflake(guildConfig.ticket_support_role_id);
  const parentId = parseSnowflake(guildConfig.ticket_category_channel_id);
  const permissionOverwrites = buildTicketPermissionOverwrites({
    guildId,
    userId: resolvedUserId,
    supportRoleId
  });
  const topic = sanitizeReason(
    `Support ticket for ${resolvedUserId}. Trigger message ${resolvedMessageId}.`,
    250
  );

  try {
    const created = await createTicketChannel({
      guild,
      guildId,
      name: ticketChannelName,
      parentId,
      topic,
      permissionOverwrites
    });

    const createdChannelId = parseSnowflake(created?.id);
    if (!createdChannelId) {
      throw new Error("ticket channel creation returned no channel id");
    }

    await db.setOpenTicket({
      guildId,
      userId: resolvedUserId,
      channelId: createdChannelId,
      triggerChannelId: resolvedChannelId,
      triggerMessageId: resolvedMessageId
    });

    const ticketTemplateValues = {
      "user.mention": formatUserMention(resolvedUserId),
      "user.id": resolvedUserId,
      "user.name": String(member.displayName || member.user?.username || resolvedUserId),
      "guild.id": guildId,
      "guild.name": String(guild.name || guildId),
      "ticket.channel": `<#${createdChannelId}>`,
      "ticket.support_role": supportRoleId ? `<@&${supportRoleId}>` : ""
    };

    const ticketMessage =
      renderMessageTemplate(guildConfig.ticket_welcome_template, ticketTemplateValues).trim() ||
      `Hello ${formatUserMention(resolvedUserId)}. Thanks for opening a ticket. Our team will be with you soon.`;

    let ticketChannel = null;
    try {
      ticketChannel = await client.channels.resolve(createdChannelId);
    } catch {
      ticketChannel = null;
    }

    if (ticketChannel && typeof ticketChannel.send === "function") {
      await ticketChannel.send(ticketMessage);
    }

    await sendTicketTriggerNotice(
      resolvedChannelId,
      `${formatUserMention(resolvedUserId)} ticket created: <#${createdChannelId}>.`
    );

    await db.logModerationAction({
      guildId,
      action: "ticket_create",
      actorUserId: client.user?.id ?? null,
      targetUserId: resolvedUserId,
      reason: `trigger=${resolvedMessageId} emoji=${guildConfig.ticket_trigger_emoji}`,
      channelId: createdChannelId,
      messageId: resolvedMessageId,
      metadata: {
        trigger_channel_id: resolvedChannelId,
        ticket_channel_id: createdChannelId,
        support_role_id: supportRoleId,
        category_channel_id: parentId
      }
    });
  } catch (error) {
    await db.logModerationAction({
      guildId,
      action: "ticket_create_failed",
      actorUserId: client.user?.id ?? null,
      targetUserId: resolvedUserId,
      reason: `trigger=${resolvedMessageId}`,
      channelId: resolvedChannelId,
      messageId: resolvedMessageId,
      metadata: {
        error: String(error),
        ticket_trigger_emoji: guildConfig.ticket_trigger_emoji
      }
    });

    await sendTicketTriggerNotice(
      resolvedChannelId,
      `${formatUserMention(resolvedUserId)} ticket creation failed. Please contact staff manually.`
    );
    logError("failed creating ticket channel", error);
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
    const candidateRoleIds = await db.getReactionRoleIds(guildId, resolvedChannelId, resolvedMessageId, key);
    if (candidateRoleIds.length > 0) {
      roleIds = candidateRoleIds;
      matchedEmojiKey = key;
      break;
    }
  }

  if (roleIds.length === 0) {
    const messageMappings = (await db
      .listReactionRoles(guildId, resolvedMessageId)
      ).filter((entry) => entry.channel_id === resolvedChannelId);

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

    await db.logModerationAction({
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

async function handleSpamModeration(message, options = {}) {
  if (!config.automod.spamDetectionEnabled) {
    return false;
  }

  if (!message.guildId || !message.content || message.author?.bot) {
    return false;
  }

  const guild = options.guild || (await resolveGuildFromMessage(message));
  if (!guild) {
    return false;
  }

  const authorIsStaff =
    typeof options.authorIsStaff === "boolean" ? options.authorIsStaff : await isStaffMember(message);

  if (authorIsStaff) {
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

  const warningCount = await db.incrementWarning(
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

  const guildConfig = await db.getGuildConfig(guild.id);
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
      const state = await getEffectiveGateState(guild.id);
      if (!state.gate_active) {
        const gateReason =
          `Automatic raid gate from spam surge: ${escalation.eventCount} suspicious events from ` +
          `${escalation.uniqueUsers} users in ${escalation.windowSeconds}s (score=${escalationScore.toFixed(3)}).`;
        const gateUntil = new Date(Date.now() + guildConfig.gate_duration_seconds * 1000).toISOString();
        await db.setRaidGateState(guild.id, true, gateReason, gateUntil);
        raidEscalated = true;
      }
    }
  }

  await db.logModerationAction({
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

async function handleWordModeration(message, options = {}) {
  if (!message.guildId || !message.content || message.author?.bot) {
    return false;
  }

  const blockedWord = wordStore.findBlockedWord(message.content);
  if (!blockedWord) {
    return false;
  }

  const guild = options.guild || (await resolveGuildFromMessage(message));
  if (!guild) {
    return false;
  }

  const actorId = message.author.id;
  const warningCount = await db.incrementWarning(guild.id, actorId, `Blocked word: ${blockedWord}`, client.user?.id ?? null);

  try {
    if (typeof message.delete === "function") {
      await message.delete();
    }
  } catch {
    // Best effort.
  }

  await db.logModerationAction({
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
        await db.logModerationAction({
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
      await db.resetWarnings(guild.id, actorId);
      await db.logModerationAction({
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

  const rank = await db.getMemberLevelRank(levelSnapshot.guild_id || message.guildId, levelSnapshot.user_id);
  const progressBar = buildProgressBar(levelSnapshot.progress_xp, levelSnapshot.progress_required, 18);
  const xpToNext = Math.max(0, Number(levelSnapshot.progress_required || 0) - Number(levelSnapshot.progress_xp || 0));

  const levelupText = renderMessageTemplate(guildConfig.levelup_message_template, {
    "user.mention": formatUserMention(levelSnapshot.user_id),
    "user.id": levelSnapshot.user_id,
    "user.name": String(message.author?.username || levelSnapshot.user_id),
    "guild.id": String(message.guildId || levelSnapshot.guild_id || ""),
    "guild.name": String(message.guild?.name || ""),
    level: String(levelSnapshot.level),
    rank: String(rank),
    "messages.count": formatInteger(levelSnapshot.message_count),
    "xp.total": formatInteger(levelSnapshot.xp),
    "xp.current": formatInteger(levelSnapshot.progress_xp),
    "xp.required": formatInteger(levelSnapshot.progress_required),
    "xp.to_next": formatInteger(xpToNext),
    "progress.percent": String(levelSnapshot.progress_percent),
    "progress.bar": progressBar
  }).trim();

  try {
    await channel.send(levelupText || `Level Up: ${formatUserMention(levelSnapshot.user_id)} reached level ${levelSnapshot.level}. Rank #${rank}.`);
  } catch (error) {
    logError("failed to send level up announcement", error);
  }
}

async function handleLevelingMessage(message, parsedCommand = null, options = {}) {
  if (!message.guildId || message.author?.bot) {
    return;
  }

  const guild = options.guild || (await resolveGuildFromMessage(message));
  if (!guild) {
    return;
  }

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
  const levelSnapshot = await db.addMemberXp({
    guildId: guild.id,
    userId: message.author.id,
    xpGain,
    cooldownSeconds: config.leveling.cooldownSeconds
  });

  if (!levelSnapshot.leveled_up || levelSnapshot.applied_xp <= 0 || levelSnapshot.cooldown_active) {
    return;
  }

  const guildConfig = await db.getGuildConfig(guild.id);
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
    formatUserMention,
    musicRuntime,
    paginationRuntime
  }),
  ...createAdminCommandHandlers({
    PermissionFlags,
    requirePermission,
    resolveGuildFromMessage,
    safeReply,
    db,
    parseSnowflake
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
    renderMessageTemplate,
    normalizeEmojiInput,
    emojiRouteTokenFromNormalized,
    messageBaseUrl: config.web.baseUrl
  })
};
async function executeCommand(parsed, message) {
  const handler = commandHandlers[parsed.command];
  if (!handler) {
    return;
  }

  const guildId = parseSnowflake(message?.guildId || message?.guild?.id);
  if (guildId && !NON_TOGGLEABLE_COMMANDS.has(parsed.command)) {
    const enabled = await db.isCommandEnabled(guildId, parsed.command);
    if (!enabled) {
      await safeReply(message, `Command is disabled in this server: ${parsed.command}`, {
        title: "Command Disabled",
        kind: "warning"
      });
      return;
    }
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
    await safeReply(message, "Command failed. Please try again later.");
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

client.on(Events.GuildCreate, async (guild) => {
  try {
    if (!guild || !guild.id) return;
    logInfo(`Joined new guild: ${guild.name || guild.id} (${guild.id})`);
    
    // Dynamically initialize configuration
    await db.getGuildConfig(guild.id);

    // Warm up caches for seamless dynamic performance
    await Promise.allSettled([
      typeof guild.fetchChannels === "function" ? guild.fetchChannels() : null,
      typeof guild.fetchRoles === "function" ? guild.fetchRoles() : null,
      typeof guild.fetchEmojis === "function" ? guild.fetchEmojis() : null
    ]);
  } catch (error) {
    logError("guild create handler failed", error);
  }
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
    const paginationHandled = await handlePaginatedEmbedReaction(reaction, emoji, resolvedUserId, channelId, messageId);
    if (paginationHandled) {
      return;
    }

    await Promise.allSettled([
      applyReactionRole(reaction, emoji, resolvedUserId, channelId, messageId, false),
      handleReactionTicketCreate(reaction, user, messageId, channelId, emoji, resolvedUserId)
    ]);
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

const activeVoiceSessions = new Map();

client.on(Events.VoiceStateUpdate, async (oldState, newState) => {
  try {
    const guildId = newState?.guildId || oldState?.guildId;
    const userId = newState?.userId || oldState?.userId || newState?.id || oldState?.id;
    if (!guildId || !userId) return;

    if (newState?.member?.user?.bot || oldState?.member?.user?.bot) {
      return;
    }

    const oldChannelId = oldState?.channelId;
    const newChannelId = newState?.channelId;
    const sessionKey = `${guildId}_${userId}`;

    if (!oldChannelId && newChannelId) {
      activeVoiceSessions.set(sessionKey, Date.now());
    } else if (oldChannelId && !newChannelId) {
      const joinedAt = activeVoiceSessions.get(sessionKey);
      if (joinedAt) {
        const durationSecs = Math.floor((Date.now() - joinedAt) / 1000);
        activeVoiceSessions.delete(sessionKey);
        await db.addVoiceTime(guildId, userId, durationSecs);
      }
    }
  } catch (error) {
    logError("voice state update handler failed", error);
  }
});

client.on(Events.MessageCreate, async (message) => {
  try {
    if (message.author?.bot) {
      return;
    }

    const guild = message.guildId ? await resolveGuildFromMessage(message) : null;
    const authorIsStaff = guild && config.automod.spamDetectionEnabled ? await isStaffMember(message) : false;
    const messageContext = {
      guild,
      authorIsStaff
    };

    const spamBlocked = await handleSpamModeration(message, messageContext);
    if (spamBlocked) {
      return;
    }

    const blocked = await handleWordModeration(message, messageContext);
    if (blocked) {
      return;
    }

    const parsed = parsePrefixedCommand(message.content);
    await handleLevelingMessage(message, parsed, messageContext);

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

