export function createAdminCommandHandlers({
  PermissionFlags,
  requirePermission,
  resolveGuildFromMessage,
  safeReply,
  db,
  parseSnowflake
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

      const cfg = await db.getGuildConfig(guild.id);
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

      await db.updateGuildConfig(guild.id, { log_channel_id: channelId });
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

      await db.updateGuildConfig(guild.id, { welcome_channel_id: channelId });
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

      await db.updateGuildConfig(guild.id, {
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

      await db.updateGuildConfig(guild.id, {
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
        await db.updateGuildConfig(guild.id, { verification_url: null });
        await safeReply(message, "Verification URL cleared. Manual review mode is active.");
        return;
      }

      if (!value.startsWith("http://") && !value.startsWith("https://")) {
        await safeReply(message, "Provide a full http:// or https:// URL, or off.");
        return;
      }

      await db.updateGuildConfig(guild.id, { verification_url: value });
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

      await db.updateGuildConfig(guild.id, {
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

      await db.updateGuildConfig(guild.id, { leveling_channel_id: channelId });
      await safeReply(message, `Leveling channel ${channelId ? `set to ${channelId}` : "cleared"}.`);
    }
  };
}
