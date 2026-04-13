import { renderLevelCardImage } from "./level-card-image.js";
import { renderServerStatsCardImage } from "./server-stats-image.js";

function formatInteger(value) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) {
    return "0";
  }

  return Math.trunc(numeric).toLocaleString("en-US");
}

function progressBar(current, total, size = 16) {
  const normalizedTotal = Math.max(1, Number(total || 1));
  const normalizedCurrent = Math.max(0, Math.min(Number(current || 0), normalizedTotal));
  const filled = Math.round((normalizedCurrent / normalizedTotal) * size);
  const clampedFilled = Math.max(0, Math.min(filled, size));
  return `${"=".repeat(clampedFilled)}${".".repeat(size - clampedFilled)}`;
}

const LEVEL_CARD_IMAGE_FILE = "level-card.png";
const SERVER_STATS_IMAGE_FILE = "server-stats.png";
const DISCORD_EPOCH_MS = 1420070400000n;

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

function resolveGuildIconUrl(guild) {
  if (!guild || typeof guild !== "object") {
    return null;
  }

  try {
    if (typeof guild.iconURL === "function") {
      const iconUrl = guild.iconURL({ size: 256 });
      if (typeof iconUrl === "string" && iconUrl.trim()) {
        return iconUrl.trim();
      }
    }
  } catch {
    // Best effort.
  }

  if (typeof guild.icon_url === "string" && guild.icon_url.trim()) {
    return guild.icon_url.trim();
  }

  return null;
}

function dateFromSnowflake(snowflake) {
  try {
    const value = BigInt(String(snowflake));
    const timestampMs = (value >> 22n) + DISCORD_EPOCH_MS;
    return new Date(Number(timestampMs));
  } catch {
    return null;
  }
}

function resolveGuildCreatedAt(guild) {
  const direct = guild?.createdAt || guild?.created_at;
  if (direct instanceof Date && Number.isFinite(direct.getTime())) {
    return direct;
  }

  if (typeof direct === "string" && direct.trim()) {
    const parsed = new Date(direct);
    if (Number.isFinite(parsed.getTime())) {
      return parsed;
    }
  }

  if (typeof direct === "number" && Number.isFinite(direct)) {
    const parsed = new Date(direct);
    if (Number.isFinite(parsed.getTime())) {
      return parsed;
    }
  }

  return dateFromSnowflake(guild?.id);
}

function formatDateTimeUtc(value) {
  if (!(value instanceof Date) || !Number.isFinite(value.getTime())) {
    return "unknown";
  }

  return value.toISOString().replace("T", " ").replace(/\.\d{3}Z$/, " UTC");
}

function buildLevelCardPayload({
  mention,
  level,
  rank,
  trackedMembers,
  progressXp,
  progressRequired,
  progressPercent,
  totalXp,
  messageCount
}) {
  const xpToNext = Math.max(0, Number(progressRequired || 0) - Number(progressXp || 0));
  const progress = progressBar(progressXp, progressRequired, 18);
  const rankSuffix = trackedMembers > 0 ? `/${trackedMembers}` : "";

  return {
    embeds: [
      {
        title: "Level Card",
        description: [mention, `Rank #${rank}${rankSuffix} | Level ${level}`].join("\n"),
        color: 0x1f6feb,
        fields: [
          {
            name: "Progress",
            value: [
              `[${progress}] ${progressPercent}%`,
              `XP ${formatInteger(progressXp)}/${formatInteger(progressRequired)}`,
              `${formatInteger(xpToNext)} XP to next level`
            ].join("\n"),
            inline: false
          },
          {
            name: "Total XP",
            value: formatInteger(totalXp),
            inline: true
          },
          {
            name: "Messages",
            value: formatInteger(messageCount),
            inline: true
          },
          {
            name: "Next Level",
            value: `Level ${Math.max(0, Number(level || 0)) + 1}`,
            inline: true
          }
        ],
        footer: {
          text: "Keep chatting to earn XP."
        },
        timestamp: new Date().toISOString()
      }
    ]
  };
}

async function tryBuildLevelCardImagePayload({
  mention,
  displayName,
  avatarUrl,
  level,
  rank,
  trackedMembers,
  progressXp,
  progressRequired,
  totalXp,
  messageCount
}) {
  try {
    const imageData = await renderLevelCardImage({
      displayName,
      avatarUrl,
      level,
      rank,
      trackedMembers,
      progressXp,
      progressRequired,
      totalXp,
      messageCount
    });

    return {
      embeds: [
        {
          title: "Level Card",
          description: `${mention} | rank #${rank}/${trackedMembers}`,
          color: 0x1f6feb,
          image: {
            url: `attachment://${LEVEL_CARD_IMAGE_FILE}`
          },
          footer: {
            text: "Use /leaderboard [page] to view server rankings."
          },
          timestamp: new Date().toISOString()
        }
      ],
      files: [
        {
          name: LEVEL_CARD_IMAGE_FILE,
          data: imageData
        }
      ]
    };
  } catch {
    return null;
  }
}

function buildServerStatsPayload({
  guildName,
  guildId,
  ownerMention,
  createdAtText,
  memberCount,
  channelCount,
  roleCount,
  emojiCount,
  trackedMembers,
  topLevelText
}) {
  const lines = [
    `Server: ${guildName}`,
    `ID: ${guildId}`,
    `Owner: ${ownerMention}`,
    `Created: ${createdAtText}`
  ];

  if (topLevelText) {
    lines.push(`Top Level: ${topLevelText}`);
  }

  return {
    embeds: [
      {
        title: "Server Stats",
        description: lines.join("\n"),
        color: 0x0ea5e9,
        fields: [
          {
            name: "Members",
            value: formatInteger(memberCount),
            inline: true
          },
          {
            name: "Channels",
            value: formatInteger(channelCount),
            inline: true
          },
          {
            name: "Roles",
            value: formatInteger(roleCount),
            inline: true
          },
          {
            name: "Emojis",
            value: formatInteger(emojiCount),
            inline: true
          },
          {
            name: "Tracked Leveling Members",
            value: formatInteger(trackedMembers),
            inline: true
          }
        ],
        timestamp: new Date().toISOString()
      }
    ]
  };
}

async function tryBuildServerStatsImagePayload({
  guildName,
  guildId,
  ownerId,
  ownerMention,
  createdAtText,
  memberCount,
  channelCount,
  roleCount,
  emojiCount,
  trackedMembers,
  iconUrl,
  topLevelText
}) {
  try {
    const imageData = await renderServerStatsCardImage({
      guildName,
      guildId,
      ownerId,
      createdAtText,
      memberCount,
      channelCount,
      roleCount,
      emojiCount,
      trackedMembers,
      iconUrl,
      topLevelText
    });

    return {
      embeds: [
        {
          title: "Server Stats",
          description: `${ownerMention} | ${formatInteger(memberCount)} members`,
          color: 0x0ea5e9,
          image: {
            url: `attachment://${SERVER_STATS_IMAGE_FILE}`
          },
          footer: {
            text: "Use /rank to view your level card."
          },
          timestamp: new Date().toISOString()
        }
      ],
      files: [
        {
          name: SERVER_STATS_IMAGE_FILE,
          data: imageData
        }
      ]
    };
  } catch {
    return null;
  }
}

function buildLeaderboardPayload({ rows, page, totalPages, trackedMembers }) {
  const lines = rows.map((entry) => {
    const index = String(entry.rank).padStart(2, "0");
    return `${index}. <@${entry.user_id}> | L${entry.level} | ${formatInteger(entry.xp)} XP | ${formatInteger(entry.message_count)} msg`;
  });

  return {
    embeds: [
      {
        title: "Level Leaderboard",
        description: lines.join("\n"),
        color: 0x2ea043,
        fields: [
          {
            name: "Page",
            value: `${page}/${totalPages}`,
            inline: true
          },
          {
            name: "Tracked Members",
            value: formatInteger(trackedMembers),
            inline: true
          },
          {
            name: "Page Size",
            value: "10",
            inline: true
          }
        ],
        footer: {
          text: "Use /rank [user] for a detailed level card."
        },
        timestamp: new Date().toISOString()
      }
    ]
  };
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
  const helpSections = {
    general: {
      title: "General",
      summary: "Core utility and info commands.",
      commands: ["help [section]", "helpmenu [section]", "perks", "stats"]
    },
    admin: {
      title: "Admin",
      summary: "Server configuration and policy commands.",
      commands: [
        "serverconfig",
        "setlogchannel <channel_id|off>",
        "setwelcomechannel <channel_id|off>",
        "setresourcechannels <rules> <chat> <help> <about> <perks>",
        "setroles <AdminRoleName> | <ModRoleName>",
        "setverificationurl <url|off>",
        "setraidsettings <threshold> <join_rate_threshold> <window_seconds> <gate_duration_seconds> <timeout|kick>",
        "setlevelingchannel <channel_id|off>"
      ]
    },
    moderation: {
      title: "Moderation",
      summary: "Member actions, filters, and raid controls.",
      commands: [
        "warnings [user]",
        "purge <count> [channel_id]",
        "kick <user> [reason]",
        "ban <user> [reason]",
        "unban <user>",
        "mute <user> [duration_minutes] [reason]",
        "unmute <user> [reason]",
        "addrole <user> <role>",
        "removerole <user> <role>",
        "addbadword <word>",
        "removebadword <word>",
        "viewbadwords [page]",
        "reloadwords",
        "raidgate <on|off|status> [duration_seconds]",
        "pendingverifications [limit]",
        "verifyjoin <user>",
        "rejectjoin <user> [reason]",
        "raidsnapshot [limit]"
      ]
    },
    reactionroles: {
      title: "Reaction Roles",
      summary: "Map reactions to role assignment.",
      commands: [
        "reactionroleadd <#channel|channel_id> <message_link|message_id> <emoji> <role>",
        "reactionroleremove <#channel|channel_id> <message_link|message_id> <emoji> [role]",
        "reactionroleclear <#channel|channel_id> <message_link|message_id>",
        "reactionrolelist [message_id]"
      ]
    },
    leveling: {
      title: "Leveling",
      summary: "XP, rank, and leaderboard commands.",
      commands: ["rank [user]", "level [user]", "leaderboard [page]"]
    },
    security: {
      title: "Security",
      summary: "Staff TOTP enrollment and authorization.",
      commands: ["totpsetup [rotate]", "totpauth <code>", "totpstatus", "totplogout"]
    },
    ai: {
      title: "AI",
      summary: "AI assistant prompts.",
      commands: ["ask <question>", "joke"]
    }
  };

  const orderedHelpSections = ["general", "admin", "moderation", "reactionroles", "leveling", "security", "ai"];

  const helpAliasToSection = {
    general: "general",
    basics: "general",
    utility: "general",
    utilities: "general",
    admin: "admin",
    staff: "admin",
    config: "admin",
    moderation: "moderation",
    mod: "moderation",
    reactionrole: "reactionroles",
    reactionroles: "reactionroles",
    rr: "reactionroles",
    leveling: "leveling",
    level: "leveling",
    xp: "leveling",
    security: "security",
    totp: "security",
    verification: "security",
    ai: "ai"
  };

  function normalizeHelpSectionInput(value) {
    return String(value || "")
      .toLowerCase()
      .replace(/[^a-z0-9]/g, "");
  }

  function formatHelpOverview() {
    const lines = [
      "Help Menu",
      "",
      "Use /help <section> for focused command lists.",
      "Examples: /help admin, /help moderation, /help reactionroles",
      "",
      "Sections:"
    ];

    for (const sectionKey of orderedHelpSections) {
      const section = helpSections[sectionKey];
      lines.push(`- ${sectionKey}: ${section.summary}`);
    }

    lines.push("", "Prefixes: / and !");
    return lines.join("\n");
  }

  function formatHelpSection(sectionKey) {
    const section = helpSections[sectionKey];
    return [`${section.title} Commands`, "", ...section.commands.map((command) => `- ${command}`), "", "Tip: use /help to list all sections."].join("\n");
  }

  const handlers = {
    async help({ message, args }) {
      const requested = args.join(" ").trim();
      if (!requested) {
        await safeReply(message, formatHelpOverview(), {
          title: "Help Menu",
          kind: "info"
        });
        return;
      }

      const normalizedRequested = normalizeHelpSectionInput(requested);
      const sectionKey = helpAliasToSection[normalizedRequested];

      if (!sectionKey) {
        await safeReply(
          message,
          [
            `Unknown help section: ${requested}`,
            "",
            `Available sections: ${orderedHelpSections.join(", ")}`,
            "Example: /help admin"
          ].join("\n"),
          {
            title: "Help Menu",
            kind: "warning"
          }
        );
        return;
      }

      await safeReply(message, formatHelpSection(sectionKey), {
        title: `${helpSections[sectionKey].title} Help`,
        kind: "info"
      });
    },

    async helpmenu(ctx) {
      return handlers.help(ctx);
    },

    async perks({ message }) {
      await safeReply(message, String(process.env.PERKS_TEXT || "No perks text configured."));
    },

    async stats({ message }) {
      const guild = await resolveGuildFromMessage(message);
      if (!guild) {
        await safeReply(message, "This command only works in a server.");
        return;
      }

      let channelCount = Math.max(0, Number(guild.channels?.size || 0));
      if (channelCount === 0 && typeof guild.fetchChannels === "function") {
        try {
          const channels = await guild.fetchChannels();
          channelCount = Array.isArray(channels) ? channels.length : Math.max(0, Number(guild.channels?.size || 0));
        } catch {
          // Best effort.
        }
      }

      let roleCount = Math.max(0, Number(guild.roles?.size || 0));
      if (roleCount === 0 && typeof guild.fetchRoles === "function") {
        try {
          const roles = await guild.fetchRoles();
          roleCount = Array.isArray(roles) ? roles.length : Math.max(0, Number(guild.roles?.size || 0));
        } catch {
          // Best effort.
        }
      }

      const rawMemberCount = [guild.memberCount, guild.member_count, guild.approximate_member_count]
        .map((value) => Number(value))
        .find((value) => Number.isFinite(value) && value > 0);
      const memberCount = Math.max(0, Number.isFinite(rawMemberCount) ? rawMemberCount : Number(guild.members?.size || 0));

      const emojiCount = Math.max(0, Number(guild.emojis?.size || 0));
      const trackedMembers = db.getLevelMemberCount(guild.id);
      const topEntry = db.listLevelLeaderboard(guild.id, 1, 0)[0] || null;

      const createdAtText = formatDateTimeUtc(resolveGuildCreatedAt(guild));
      const ownerId = String(guild.ownerId || "").trim();
      const ownerMention = ownerId ? formatUserMention(ownerId) : "Unknown";
      const topLevelText = topEntry
        ? `${formatUserMention(topEntry.user_id)} | L${topEntry.level} | ${formatInteger(topEntry.xp)} XP`
        : "No leveling data yet.";
      const topLevelImageText = topEntry
        ? `#1 ${topEntry.user_id} | L${topEntry.level} | ${formatInteger(topEntry.xp)} XP`
        : "No leveling data yet.";

      const imagePayload = await tryBuildServerStatsImagePayload({
        guildName: String(guild.name || "Unknown Server"),
        guildId: String(guild.id || "-"),
        ownerId,
        ownerMention,
        createdAtText,
        memberCount,
        channelCount,
        roleCount,
        emojiCount,
        trackedMembers,
        iconUrl: resolveGuildIconUrl(guild),
        topLevelText: topLevelImageText
      });

      if (imagePayload) {
        await safeReply(message, imagePayload);
        return;
      }

      await safeReply(
        message,
        buildServerStatsPayload({
          guildName: String(guild.name || "Unknown Server"),
          guildId: String(guild.id || "-"),
          ownerMention,
          createdAtText,
          memberCount,
          channelCount,
          roleCount,
          emojiCount,
          trackedMembers,
          topLevelText
        })
      );
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
      const trackedMembers = Math.max(rank, db.getLevelMemberCount(guild.id));
      const mention = formatUserMention(targetUserId);

      let displayName = String(message.author?.username || targetUserId);
      let avatarUrl = message.author?.id === targetUserId ? resolveAvatarUrl(message.author) : null;

      try {
        const cachedMember = typeof guild.members?.get === "function" ? guild.members.get(targetUserId) : null;
        const fetchedMember =
          cachedMember || (typeof guild.fetchMember === "function" ? await guild.fetchMember(targetUserId) : null);

        if (fetchedMember) {
          displayName = String(
            fetchedMember.displayName ||
              fetchedMember.nick ||
              fetchedMember.user?.globalName ||
              fetchedMember.user?.displayName ||
              fetchedMember.user?.username ||
              displayName
          );

          avatarUrl = avatarUrl || resolveAvatarUrl(fetchedMember.user || fetchedMember);
        }
      } catch {
        // Best effort profile data.
      }

      const imagePayload = await tryBuildLevelCardImagePayload({
        mention,
        displayName,
        avatarUrl,
        level: snapshot.level,
        rank,
        trackedMembers,
        progressXp: snapshot.progress_xp,
        progressRequired: snapshot.progress_required,
        totalXp: snapshot.xp,
        messageCount: snapshot.message_count
      });

      if (imagePayload) {
        await safeReply(message, imagePayload);
        return;
      }

      await safeReply(
        message,
        buildLevelCardPayload({
          mention,
          level: snapshot.level,
          rank,
          trackedMembers,
          progressXp: snapshot.progress_xp,
          progressRequired: snapshot.progress_required,
          progressPercent: snapshot.progress_percent,
          totalXp: snapshot.xp,
          messageCount: snapshot.message_count
        })
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
      const trackedMembers = db.getLevelMemberCount(guild.id);
      if (trackedMembers === 0) {
        await safeReply(message, "No leveling data yet.");
        return;
      }

      const requestedPage = Number.parseInt(String(args[0] || "1"), 10);
      const page = Number.isFinite(requestedPage) && requestedPage > 0 ? requestedPage : 1;
      const totalPages = Math.max(1, Math.ceil(trackedMembers / pageSize));
      if (page > totalPages) {
        await safeReply(message, `No entries on that page. Try 1-${totalPages}.`);
        return;
      }

      const offset = (page - 1) * pageSize;

      const rows = db.listLevelLeaderboard(guild.id, pageSize, offset);
      if (rows.length === 0) {
        await safeReply(message, page === 1 ? "No leveling data yet." : "No entries on that page.");
        return;
      }

      await safeReply(
        message,
        buildLeaderboardPayload({
          rows,
          page,
          totalPages,
          trackedMembers
        })
      );
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
