function toReactionRouteToken(rawValue) {
  const raw = String(rawValue ?? "").trim();
  if (!raw) {
    return "";
  }

  const mentionMatch = raw.match(/^<a?:([^:>]+):(\d{5,22})>$/);
  if (mentionMatch) {
    const [, name, id] = mentionMatch;
    return `${name}:${id}`;
  }

  if (/^\d{5,22}$/.test(raw)) {
    return "";
  }

  return raw;
}

function parseChannelIdInput(rawValue, parseSnowflake) {
  const text = String(rawValue ?? "").trim();
  const direct = parseSnowflake(text);
  if (direct) {
    return direct;
  }

  const mentionMatch = text.match(/^<#(\d{5,22})>$/);
  if (mentionMatch) {
    return mentionMatch[1];
  }

  return null;
}

function parseMessageTargetInput(rawValue, parseSnowflake) {
  const text = String(rawValue ?? "").trim();
  const directMessageId = parseSnowflake(text);

  if (directMessageId) {
    return {
      guildId: null,
      channelId: null,
      messageId: directMessageId
    };
  }

  const linkMatch = text.match(
    /^https?:\/\/(?:www\.)?[^/]+\/channels\/(\d{5,22})\/(\d{5,22})\/(\d{5,22})(?:[/?#].*)?$/i
  );

  if (!linkMatch) {
    return null;
  }

  return {
    guildId: linkMatch[1],
    channelId: linkMatch[2],
    messageId: linkMatch[3]
  };
}

function buildReactionCandidates(normalized, emojiRouteTokenFromNormalized) {
  const candidates = [];
  const seenStrings = new Set();

  const pushString = (value) => {
    const text = String(value ?? "").trim();
    if (!text || seenStrings.has(text)) {
      return;
    }

    seenStrings.add(text);
    candidates.push(text);
  };

  if (normalized?.reactionValue && typeof normalized.reactionValue === "object") {
    candidates.push(normalized.reactionValue);
  } else {
    pushString(normalized?.reactionValue);
  }

  pushString(normalized?.display);
  pushString(normalized?.key);

  if (Array.isArray(normalized?.aliases)) {
    for (const alias of normalized.aliases) {
      pushString(alias);
    }
  }

  if (typeof emojiRouteTokenFromNormalized === "function") {
    pushString(toReactionRouteToken(emojiRouteTokenFromNormalized(normalized)));
  }

  return candidates;
}

function buildReactionRouteTokens(normalized, emojiRouteTokenFromNormalized) {
  const routeTokens = [];
  const seen = new Set();

  const push = (value) => {
    const token = toReactionRouteToken(value);
    if (!token || seen.has(token)) {
      return;
    }

    seen.add(token);
    routeTokens.push(token);
  };

  if (typeof emojiRouteTokenFromNormalized === "function") {
    push(emojiRouteTokenFromNormalized(normalized));
  }

  if (normalized?.customName && normalized?.customId) {
    push(`${normalized.customName}:${normalized.customId}`);
  }

  if (typeof normalized?.reactionValue === "string") {
    push(normalized.reactionValue);
  }

  if (Array.isArray(normalized?.aliases)) {
    for (const alias of normalized.aliases) {
      push(alias);
    }
  }

  push(normalized?.display);
  push(normalized?.key);
  return routeTokens;
}

const PURGE_FEEDBACK_AUTO_DELETE_MS = 5000;

async function ensureReactionOnMessage({
  client,
  channelId,
  messageId,
  targetMessage,
  normalized,
  emojiRouteTokenFromNormalized
}) {
  let firstError = null;

  for (const candidate of buildReactionCandidates(normalized, emojiRouteTokenFromNormalized)) {
    try {
      await targetMessage.react(candidate);
      return {
        ok: true,
        method: "message.react",
        candidate: typeof candidate === "string" ? candidate : JSON.stringify(candidate)
      };
    } catch (error) {
      if (!firstError) {
        firstError = error;
      }
    }
  }

  for (const routeToken of buildReactionRouteTokens(normalized, emojiRouteTokenFromNormalized)) {
    try {
      await client.rest.put(
        `/channels/${channelId}/messages/${messageId}/reactions/${encodeURIComponent(routeToken)}/@me`,
        { auth: true }
      );

      return {
        ok: true,
        method: "rest.put",
        candidate: routeToken
      };
    } catch (error) {
      if (!firstError) {
        firstError = error;
      }
    }
  }

  return {
    ok: false,
    error: firstError ? String(firstError) : "Failed to add reaction."
  };
}

async function ensureReactionRemovedFromMessage({
  client,
  channelId,
  messageId,
  targetMessage,
  normalized,
  emojiRouteTokenFromNormalized
}) {
  let firstError = null;

  for (const candidate of buildReactionCandidates(normalized, emojiRouteTokenFromNormalized)) {
    if (typeof targetMessage?.removeReaction !== "function") {
      break;
    }

    try {
      await targetMessage.removeReaction(candidate);
      return {
        ok: true,
        method: "message.removeReaction",
        candidate: typeof candidate === "string" ? candidate : JSON.stringify(candidate)
      };
    } catch (error) {
      if (!firstError) {
        firstError = error;
      }
    }
  }

  for (const routeToken of buildReactionRouteTokens(normalized, emojiRouteTokenFromNormalized)) {
    try {
      await client.rest.delete(
        `/channels/${channelId}/messages/${messageId}/reactions/${encodeURIComponent(routeToken)}/@me`,
        { auth: true }
      );

      return {
        ok: true,
        method: "rest.delete",
        candidate: routeToken
      };
    } catch (error) {
      if (!firstError) {
        firstError = error;
      }
    }
  }

  return {
    ok: false,
    error: firstError ? String(firstError) : "Failed to remove reaction."
  };
}

export function createModerationCommandHandlers({
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
  messageBaseUrl
}) {
  const normalizedMessageBaseUrl = String(messageBaseUrl || "https://fluxer.app")
    .trim()
    .replace(/\/+$/, "");

  const buildGuildMessageLink = (guildId, channelId, messageId) => {
    return `${normalizedMessageBaseUrl}/channels/${guildId}/${channelId}/${messageId}`;
  };

  return {
    async addbadword({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.ManageGuild))) {
        return;
      }

      const word = args.join(" ").trim();
      if (!word) {
        await safeReply(message, "Usage: addbadword <word>");
        return;
      }

      const created = wordStore.add(word);
      await safeReply(message, created ? `Blocked word added: ${word}` : `Blocked word already exists: ${word}`);
    },

    async removebadword({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.ManageGuild))) {
        return;
      }

      const word = args.join(" ").trim();
      if (!word) {
        await safeReply(message, "Usage: removebadword <word>");
        return;
      }

      const deleted = wordStore.remove(word);
      await safeReply(message, deleted ? `Blocked word removed: ${word}` : `Blocked word not found: ${word}`);
    },

    async viewbadwords({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.ManageGuild))) {
        return;
      }

      const words = wordStore.list();
      if (words.length === 0) {
        await safeReply(message, "No blocked words configured.");
        return;
      }

      const page = Math.max(1, Number(args[0] || 1));
      const pageSize = 20;
      const totalPages = Math.ceil(words.length / pageSize);
      const currentPage = Math.min(page, totalPages);
      const start = (currentPage - 1) * pageSize;
      const chunk = words.slice(start, start + pageSize);

      await safeReply(
        message,
        [`Blocked words page ${currentPage}/${totalPages}:`, ...chunk.map((word) => `- ${word}`)].join("\n")
      );
    },

    async reloadwords({ message }) {
      if (!(await requirePermission(message, PermissionFlags.ManageGuild))) {
        return;
      }

      wordStore.reload();
      await safeReply(message, `Word list reloaded (${wordStore.list().length} entries).`);
    },

    async warnings({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.ModerateMembers))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        await safeReply(message, "This command only works in a server.");
        return;
      }

      const targetUserId = parseUserIdArg(args[0]) || message.author.id;
      const count = await db.getWarningCount(guild.id, targetUserId);
      await safeReply(message, `${formatUserMention(targetUserId)} has ${count} warning(s).`);
    },

    async purge({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.ManageMessages, undefined, { skipTotp: true }))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        await safeReply(message, "This command only works in a server.");
        return;
      }

      const parsedAmount = Number.parseInt(String(args[0] || ""), 10);
      if (!Number.isFinite(parsedAmount) || parsedAmount < 1) {
        await safeReply(message, "Usage: purge <count 1-100> [channel_id]");
        return;
      }

      const amount = Math.max(1, Math.min(parsedAmount, 100));
      const targetChannelId = parseSnowflake(args[1]) || message.channelId;
      if (!targetChannelId) {
        await safeReply(message, "Could not resolve a channel to purge.");
        return;
      }

      let channel = null;
      try {
        channel = await client.channels.resolve(targetChannelId);
      } catch {
        // Best effort.
      }

      if (!channel) {
        await safeReply(message, "Channel not found.");
        return;
      }

      const fetchLimit = Math.max(1, Math.min(amount + 20, 100));
      let rawMessages;

      try {
        rawMessages = await client.rest.get(`/channels/${targetChannelId}/messages?limit=${fetchLimit}`, { auth: true });
      } catch (error) {
        await safeReply(message, "Failed to read channel messages.", {
          title: "Moderation",
          kind: "error"
        });

        if (typeof console !== "undefined" && typeof console.error === "function") {
          console.error("purge read channel messages failed", error);
        }

        return;
      }

      const targetMessageIds = Array.isArray(rawMessages)
        ? rawMessages
            .map((entry) => parseSnowflake(entry?.id))
            .filter((id) => id && id !== message.id)
            .slice(0, amount)
        : [];

      if (targetMessageIds.length === 0) {
        await safeReply(message, "No messages available to delete in that channel.", {
          title: "Moderation",
          kind: "info"
        });
        return;
      }

      let deletedCount = 0;
      let bulkError = null;

      if (targetMessageIds.length >= 2 && typeof channel.bulkDeleteMessages === "function") {
        try {
          await channel.bulkDeleteMessages(targetMessageIds);
          deletedCount = targetMessageIds.length;
        } catch (error) {
          bulkError = String(error);
        }
      }

      if (deletedCount < targetMessageIds.length) {
        for (const targetMessageId of targetMessageIds.slice(deletedCount)) {
          try {
            const targetMessage = await channel?.messages?.fetch(targetMessageId);
            if (targetMessage && typeof targetMessage.delete === "function") {
              await targetMessage.delete();
              deletedCount += 1;
            }
          } catch {
            // Continue deleting what we can.
          }
        }
      }

      await db.logModerationAction({
        guildId: guild.id,
        action: deletedCount > 0 ? "purge" : "purge_failed",
        actorUserId: message.author.id,
        reason: `purge requested (${amount})`,
        channelId: targetChannelId,
        messageId: message.id,
        metadata: {
          requested: amount,
          attempted: targetMessageIds.length,
          deleted: deletedCount,
          bulk_error: bulkError
        }
      });

      if (deletedCount === 0) {
        await safeReply(message, "Unable to delete messages in that channel.", {
          title: "Moderation",
          kind: "error"
        });
        return;
      }

      const responseLines = [
        `Purge completed in <#${targetChannelId}>.`,
        `deleted ${deletedCount}/${targetMessageIds.length} message(s).`
      ];

      if (bulkError && deletedCount > 0) {
        responseLines.push("Bulk delete failed, fallback single-message delete was used.");
      }

      if (deletedCount < targetMessageIds.length) {
        responseLines.push(`${targetMessageIds.length - deletedCount} message(s) could not be removed.`);
      }

      await safeReply(message, responseLines.join("\n"), {
        title: "Moderation",
        kind: deletedCount < targetMessageIds.length ? "warning" : "success",
        deleteAfterMs: PURGE_FEEDBACK_AUTO_DELETE_MS
      });

      if (typeof message.delete === "function") {
        try {
          await message.delete();
        } catch {
          // Best effort command cleanup.
        }
      }
    },

    async kick({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.KickMembers))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) return;

      const userId = parseUserIdArg(args[0]);
      if (!userId) {
        await safeReply(message, "Usage: kick <user> [reason]");
        return;
      }

      const reason = sanitizeReason(args.slice(1).join(" ") || "No reason provided");
      await guild.kick(userId, { reason });

      const guildConfig = await db.getGuildConfig(guild.id);
      const kickText = renderMessageTemplate(guildConfig.kick_message_template, {
        "user.mention": formatUserMention(userId),
        "user.id": userId,
        "guild.id": guild.id,
        "guild.name": String(guild.name || guild.id),
        reason
      }).trim();

      await db.logModerationAction({
        guildId: guild.id,
        action: "kick",
        actorUserId: message.author.id,
        targetUserId: userId,
        reason,
        channelId: message.channelId,
        messageId: message.id
      });

      await safeReply(message, kickText || `Kicked ${formatUserMention(userId)}.`);
    },

    async ban({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.BanMembers))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) return;

      const userId = parseUserIdArg(args[0]);
      if (!userId) {
        await safeReply(message, "Usage: ban <user> [reason]");
        return;
      }

      const reason = sanitizeReason(args.slice(1).join(" ") || "No reason provided");
      await guild.ban(userId, { reason });

      const guildConfig = await db.getGuildConfig(guild.id);
      const banText = renderMessageTemplate(guildConfig.ban_message_template, {
        "user.mention": formatUserMention(userId),
        "user.id": userId,
        "guild.id": guild.id,
        "guild.name": String(guild.name || guild.id),
        reason
      }).trim();

      await db.logModerationAction({
        guildId: guild.id,
        action: "ban",
        actorUserId: message.author.id,
        targetUserId: userId,
        reason,
        channelId: message.channelId,
        messageId: message.id
      });

      await safeReply(message, banText || `Banned ${formatUserMention(userId)}.`);
    },

    async unban({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.BanMembers))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) return;

      const userId = parseUserIdArg(args[0]);
      if (!userId) {
        await safeReply(message, "Usage: unban <user>");
        return;
      }

      await guild.unban(userId);

      await db.logModerationAction({
        guildId: guild.id,
        action: "unban",
        actorUserId: message.author.id,
        targetUserId: userId,
        reason: "Unbanned"
      });

      await safeReply(message, `Unbanned ${formatUserMention(userId)}.`);
    },

    async mute({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.ModerateMembers))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) return;

      const userId = parseUserIdArg(args[0]);
      if (!userId) {
        await safeReply(message, "Usage: mute <user> [duration_minutes] [reason]");
        return;
      }

      let durationMinutes = 10;
      let reasonStartIndex = 1;
      if (args[1] && /^\d+$/.test(args[1])) {
        durationMinutes = Math.max(1, Math.min(Number(args[1]), 10080));
        reasonStartIndex = 2;
      }

      const reason = sanitizeReason(args.slice(reasonStartIndex).join(" ") || "No reason provided");
      const until = toIsoSeconds(new Date(Date.now() + durationMinutes * 60 * 1000));

      const member = await resolveGuildMember(guild, userId);
      if (!member) {
        await safeReply(message, "Member not found in this guild.");
        return;
      }

      await member.edit({
        communication_disabled_until: until,
        timeout_reason: reason
      });

      const guildConfig = await db.getGuildConfig(guild.id);
      const muteText = renderMessageTemplate(guildConfig.mute_message_template, {
        "user.mention": formatUserMention(userId),
        "user.id": userId,
        "guild.id": guild.id,
        "guild.name": String(guild.name || guild.id),
        reason,
        until,
        duration_minutes: String(durationMinutes)
      }).trim();

      await db.logModerationAction({
        guildId: guild.id,
        action: "mute",
        actorUserId: message.author.id,
        targetUserId: userId,
        reason,
        channelId: message.channelId,
        messageId: message.id,
        metadata: {
          duration_minutes: durationMinutes,
          until
        }
      });

      await safeReply(message, muteText || `${formatUserMention(userId)} muted for ${durationMinutes} minute(s).`);
    },

    async unmute({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.ModerateMembers))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) return;

      const userId = parseUserIdArg(args[0]);
      if (!userId) {
        await safeReply(message, "Usage: unmute <user> [reason]");
        return;
      }

      const reason = sanitizeReason(args.slice(1).join(" ") || "No reason provided");
      const member = await resolveGuildMember(guild, userId);
      if (!member) {
        await safeReply(message, "Member not found in this guild.");
        return;
      }

      await member.edit({
        communication_disabled_until: null,
        timeout_reason: reason
      });

      await db.logModerationAction({
        guildId: guild.id,
        action: "unmute",
        actorUserId: message.author.id,
        targetUserId: userId,
        reason,
        channelId: message.channelId,
        messageId: message.id
      });

      await safeReply(message, `${formatUserMention(userId)} unmuted.`);
    },

    async addrole({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.ManageRoles))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) return;

      const userId = parseUserIdArg(args[0]);
      const roleText = args.slice(1).join(" ").trim();
      if (!userId || !roleText) {
        await safeReply(message, "Usage: addrole <user> <role>");
        return;
      }

      const roleId = await guild.resolveRoleId(roleText);
      if (!roleId) {
        await safeReply(message, "Role not found.");
        return;
      }

      const member = await resolveGuildMember(guild, userId);
      if (!member) {
        await safeReply(message, "Member not found.");
        return;
      }

      await member.roles.add(roleId);

      await db.logModerationAction({
        guildId: guild.id,
        action: "add_role",
        actorUserId: message.author.id,
        targetUserId: userId,
        reason: `role_id=${roleId}`,
        metadata: { role_id: roleId }
      });

      await safeReply(message, `Added role <@&${roleId}> to ${formatUserMention(userId)}.`, {
        title: "Moderation",
        kind: "success",
        roleId
      });
    },

    async removerole({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.ManageRoles))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) return;

      const userId = parseUserIdArg(args[0]);
      const roleText = args.slice(1).join(" ").trim();
      if (!userId || !roleText) {
        await safeReply(message, "Usage: removerole <user> <role>");
        return;
      }

      const roleId = await guild.resolveRoleId(roleText);
      if (!roleId) {
        await safeReply(message, "Role not found.");
        return;
      }

      const member = await resolveGuildMember(guild, userId);
      if (!member) {
        await safeReply(message, "Member not found.");
        return;
      }

      await member.roles.remove(roleId);

      await db.logModerationAction({
        guildId: guild.id,
        action: "remove_role",
        actorUserId: message.author.id,
        targetUserId: userId,
        reason: `role_id=${roleId}`,
        metadata: { role_id: roleId }
      });

      await safeReply(message, `Removed role <@&${roleId}> from ${formatUserMention(userId)}.`, {
        title: "Moderation",
        kind: "success",
        roleId
      });
    },

    async raidgate({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.ModerateMembers))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) return;

      const mode = String(args[0] || "status").trim().toLowerCase();
      if (mode === "status") {
        const state = await getEffectiveGateState(guild.id);
        await safeReply(
          message,
          [
            `gate_active: ${state.gate_active}`,
            `gate_reason: ${state.gate_reason || "-"}`,
            `gate_until: ${state.gate_until || "-"}`
          ].join("\n")
        );
        return;
      }

      if (mode === "off") {
        await db.setRaidGateState(guild.id, false, "Manual disable by staff", null);
        await safeReply(message, "Raid gate disabled.");
        return;
      }

      if (mode === "on") {
        const cfg = await db.getGuildConfig(guild.id);
        const duration = Math.max(60, Math.min(Number(args[1] || cfg.gate_duration_seconds), 86400));
        const gateUntil = new Date(Date.now() + duration * 1000).toISOString();
        await db.setRaidGateState(guild.id, true, `Manual gate enabled by ${message.author.id}`, gateUntil);
        await safeReply(message, `Raid gate enabled until ${gateUntil}.`);
        return;
      }

      await safeReply(message, "Usage: raidgate <on|off|status> [duration_seconds]");
    },

    async pendingverifications({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.ModerateMembers))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) return;

      const limit = Math.max(1, Math.min(Number(args[0] || 10), 50));
      const rows = await db.listPendingVerifications(guild.id, limit);

      if (rows.length === 0) {
        await safeReply(message, "No pending verifications.");
        return;
      }

      const lines = rows.map(
        (row) => `- ${formatUserMention(row.user_id)} | risk=${row.risk_score.toFixed(3)} | reason=${row.reason}`
      );

      await safeReply(message, ["Pending verifications:", ...lines].join("\n"));
    },

    async verifyjoin({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.ModerateMembers))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) return;

      const userId = parseUserIdArg(args[0]);
      if (!userId) {
        await safeReply(message, "Usage: verifyjoin <user>");
        return;
      }

      const current = await db.getVerificationStatus(guild.id, userId);
      if (!current || current.status !== "pending") {
        await safeReply(message, "That member is not in the pending verification queue.");
        return;
      }

      await db.upsertVerificationMember({
        guildId: guild.id,
        userId,
        status: "verified",
        riskScore: current.risk_score,
        verificationUrl: current.verification_url,
        reason: "Manually verified by staff",
        verifiedByUserId: message.author.id
      });

      let timeoutCleared = false;
      let timeoutError = null;
      let member = null;

      try {
        member = await resolveGuildMember(guild, userId);
        if (member) {
          await member.edit({
            communication_disabled_until: null,
            timeout_reason: sanitizeReason(`Verification approved by ${message.author.id}`)
          });
          timeoutCleared = true;
        }
      } catch (error) {
        timeoutError = String(error);
      }

      await db.logModerationAction({
        guildId: guild.id,
        action: "verification_approved",
        actorUserId: message.author.id,
        targetUserId: userId,
        reason: "Join verification approved",
        channelId: message.channelId,
        messageId: message.id,
        metadata: {
          timeout_cleared: timeoutCleared,
          timeout_error: timeoutError
        }
      });

      if (member) {
        await sendWelcomeForMember(member);
      }

      if (timeoutError) {
        await safeReply(message, `${formatUserMention(userId)} marked approved, but timeout clear failed: ${timeoutError}`);
        return;
      }

      await safeReply(message, `${formatUserMention(userId)} approved.`);
    },

    async rejectjoin({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.KickMembers))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) return;

      const userId = parseUserIdArg(args[0]);
      if (!userId) {
        await safeReply(message, "Usage: rejectjoin <user> [reason]");
        return;
      }

      const reason = sanitizeReason(args.slice(1).join(" ") || "No reason provided");
      const current = await db.getVerificationStatus(guild.id, userId);

      await db.upsertVerificationMember({
        guildId: guild.id,
        userId,
        status: "rejected",
        riskScore: current?.risk_score ?? 0,
        verificationUrl: current?.verification_url ?? null,
        reason,
        verifiedByUserId: message.author.id
      });

      let kickApplied = false;
      let kickError = null;

      try {
        await guild.kick(userId, { reason });
        kickApplied = true;
      } catch (error) {
        kickError = String(error);
      }

      await db.logModerationAction({
        guildId: guild.id,
        action: "verification_rejected",
        actorUserId: message.author.id,
        targetUserId: userId,
        reason,
        channelId: message.channelId,
        messageId: message.id,
        metadata: {
          kick_applied: kickApplied,
          kick_error: kickError
        }
      });

      if (kickApplied) {
        await safeReply(message, `${formatUserMention(userId)} rejected and removed.`);
      } else {
        await safeReply(message, `${formatUserMention(userId)} marked rejected, but kick failed: ${kickError || "unknown"}`);
      }
    },

    async raidsnapshot({ message, args }) {
      if (!(await requirePermission(message, PermissionFlags.ModerateMembers))) {
        return;
      }

      const guild = await resolveGuildFromMessage(message);
      if (!guild) return;

      const limit = Math.max(1, Math.min(Number(args[0] || 10), 50));
      const events = await db.listRecentJoinEvents(guild.id, limit);

      if (events.length === 0) {
        await safeReply(message, "No join events recorded yet.");
        return;
      }

      const lines = events.map(
        (entry) =>
          `- ${formatUserMention(entry.user_id)} | action=${entry.action} | risk=${entry.risk_score.toFixed(3)} | level=${entry.risk_level}`
      );
      await safeReply(message, ["Recent join events:", ...lines].join("\n"));
    },

    async reactionroleadd({ message }) {
      if (!(await requirePermission(message, PermissionFlags.ManageRoles))) {
        return;
      }

      await safeReply(
        message,
        "Reaction role mappings are now managed from the dashboard panel builder. Legacy chat commands are disabled.",
        {
          title: "Reaction Roles",
          kind: "info"
        }
      );
    },

    async reactionroleremove({ message }) {
      if (!(await requirePermission(message, PermissionFlags.ManageRoles))) {
        return;
      }

      await safeReply(
        message,
        "Reaction role mappings are now managed from the dashboard panel builder. Legacy chat commands are disabled.",
        {
          title: "Reaction Roles",
          kind: "info"
        }
      );
    },

    async reactionroleclear({ message }) {
      if (!(await requirePermission(message, PermissionFlags.ManageRoles))) {
        return;
      }

      await safeReply(
        message,
        "Reaction role mappings are now managed from the dashboard panel builder. Legacy chat commands are disabled.",
        {
          title: "Reaction Roles",
          kind: "info"
        }
      );
    },

    async reactionrolelist({ message }) {
      if (!(await requirePermission(message, PermissionFlags.ManageRoles))) {
        return;
      }

      await safeReply(
        message,
        "Reaction role mappings are now managed from the dashboard panel builder. Legacy chat commands are disabled.",
        {
          title: "Reaction Roles",
          kind: "info"
        }
      );
    }
  };
}
