function progressBar(current, total, size = 12) {
  const normalizedTotal = Math.max(1, Number(total || 1));
  const normalizedCurrent = Math.max(0, Math.min(Number(current || 0), normalizedTotal));
  const filled = Math.round((normalizedCurrent / normalizedTotal) * size);
  const clampedFilled = Math.max(0, Math.min(filled, size));
  return `${"#".repeat(clampedFilled)}${"-".repeat(size - clampedFilled)}`;
}

export function createUtilityCommandHandlers({
  safeReply,
  config,
  aiLastUsedByUser,
  generateAiText,
  db,
  resolveGuildFromMessage,
  parseUserIdArg,
  formatUserMention
}) {
  const handlers = {
    async help({ message }) {
      await safeReply(
        message,
        [
          "Available commands:",
          "- help, helpmenu",
          "- rank [user], level [user], leaderboard [page]",
          "- warnings [user], purge, kick, ban, unban, mute, unmute",
          "- addrole, removerole",
          "- addbadword, removebadword, viewbadwords, reloadwords",
          "- setlogchannel, setwelcomechannel, setresourcechannels, setroles, serverconfig",
          "- setlevelingchannel",
          "- totpsetup [rotate], totpauth <code>, totpstatus, totplogout",
          "- setverificationurl, setraidsettings, raidgate",
          "- pendingverifications, verifyjoin, rejectjoin, raidsnapshot",
          "- reactionroleadd, reactionroleremove, reactionroleclear, reactionrolelist",
          "- ask, joke"
        ].join("\n")
      );
    },

    async helpmenu(ctx) {
      return handlers.help(ctx);
    },

    async aboutserver({ message }) {
      await safeReply(message, String(process.env.ABOUT_TEXT || "No about text configured."));
    },

    async perks({ message }) {
      await safeReply(message, String(process.env.PERKS_TEXT || "No perks text configured."));
    },

    async rank({ message, args }) {
      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        await safeReply(message, "This command only works in a server.");
        return;
      }

      const targetUserId = parseUserIdArg(args[0]) || message.author.id;
      const snapshot = db.getMemberLevel(guild.id, targetUserId);
      const rank = db.getMemberLevelRank(guild.id, targetUserId);

      const bar = progressBar(snapshot.progress_xp, snapshot.progress_required, 14);
      await safeReply(
        message,
        [
          `Level stats for ${formatUserMention(targetUserId)}:`,
          `rank: #${rank}`,
          `level: ${snapshot.level}`,
          `xp: ${snapshot.progress_xp}/${snapshot.progress_required} (${snapshot.progress_percent}%)`,
          `progress: [${bar}]`,
          `total_xp: ${snapshot.xp}`,
          `messages: ${snapshot.message_count}`
        ].join("\n")
      );
    },

    async level(ctx) {
      return handlers.rank(ctx);
    },

    async leaderboard({ message, args }) {
      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        await safeReply(message, "This command only works in a server.");
        return;
      }

      const pageSize = 10;
      const requestedPage = Number.parseInt(String(args[0] || "1"), 10);
      const page = Number.isFinite(requestedPage) && requestedPage > 0 ? requestedPage : 1;
      const offset = (page - 1) * pageSize;

      const rows = db.listLevelLeaderboard(guild.id, pageSize, offset);
      if (rows.length === 0) {
        await safeReply(message, page === 1 ? "No leveling data yet." : "No entries on that page.");
        return;
      }

      const lines = rows.map(
        (entry) =>
          `#${entry.rank} ${formatUserMention(entry.user_id)} - lvl ${entry.level} - ${entry.xp} XP - ${entry.message_count} msg`
      );

      await safeReply(message, [`Level leaderboard page ${page}:`, ...lines].join("\n"));
    },

    async ask({ message, args }) {
      const question = args.join(" ").trim();
      if (!question) {
        await safeReply(message, "Usage: ask <question>");
        return;
      }

      if (question.length > config.ai.maxQuestionLength) {
        await safeReply(message, `Question too long. Max ${config.ai.maxQuestionLength} characters.`);
        return;
      }

      if (!config.ai.apiKey) {
        await safeReply(message, "AI is not configured (missing GOOGLE_API_KEY).\n");
        return;
      }

      const now = Date.now();
      const lastUsed = aiLastUsedByUser.get(message.author.id) || 0;
      const cooldownMs = config.ai.rateLimitSeconds * 1000;
      if (now - lastUsed < cooldownMs) {
        const retryIn = Math.ceil((cooldownMs - (now - lastUsed)) / 1000);
        await safeReply(message, `AI rate limit active. Retry in ${retryIn}s.`);
        return;
      }

      aiLastUsedByUser.set(message.author.id, now);

      try {
        const answer = await generateAiText(question);
        const trimmed = answer.slice(0, config.ai.maxResponseLength);
        await safeReply(message, trimmed);
      } catch (error) {
        await safeReply(message, `AI request failed: ${String(error)}`);
      }
    },

    async joke({ message }) {
      return handlers.ask({
        message,
        args: ["Tell a short clean joke for a community server."]
      });
    }
  };

  return handlers;
}
