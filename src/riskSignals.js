export const DISCORD_EPOCH_MS = 1420070400000n;

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function safeDivision(numerator, denominator, fallback = 0) {
  if (!Number.isFinite(denominator) || denominator <= 0) {
    return fallback;
  }

  return numerator / denominator;
}

function normalizeContent(content) {
  return String(content || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/https?:\/\/\S+/g, "<url>")
    .replace(/<@!?\d{5,22}>/g, "<mention>")
    .trim();
}

export class SpamRiskEngine {
  constructor(options = {}) {
    this.options = {
      spamWindowSeconds: Number(options.spamWindowSeconds || 8),
      spamMessageThreshold: Number(options.spamMessageThreshold || 6),
      duplicateWindowSeconds: Number(options.duplicateWindowSeconds || 20),
      duplicateThreshold: Number(options.duplicateThreshold || 3),
      mentionThreshold: Number(options.mentionThreshold || 6),
      linkThreshold: Number(options.linkThreshold || 4)
    };

    this.guildUserState = new Map();
  }

  evaluateMessage({ guildId, userId, content, createdAtMs = Date.now() }) {
    const now = Number(createdAtMs) / 1000;
    const guildKey = String(guildId || "");
    const userKey = String(userId || "");
    if (!guildKey || !userKey) {
      return {
        isSpam: false,
        score: 0,
        severity: "none",
        reasons: [],
        metrics: {
          messagesInWindow: 0,
          duplicateCount: 0,
          mentionCount: 0,
          linkCount: 0
        }
      };
    }

    const stateKey = `${guildKey}:${userKey}`;
    const existing =
      this.guildUserState.get(stateKey) || {
        messages: [],
        duplicateMap: new Map()
      };

    const normalized = normalizeContent(content);
    const mentionCount = (String(content || "").match(/<@!?\d{5,22}>/g) || []).length;
    const linkCount = (String(content || "").match(/https?:\/\/\S+|www\.\S+/gi) || []).length;

    existing.messages.push({ ts: now, normalized, mentionCount, linkCount });
    existing.messages = existing.messages.filter(
      (entry) => now - entry.ts <= Math.max(this.options.spamWindowSeconds, this.options.duplicateWindowSeconds)
    );

    const duplicateMap = new Map();
    for (const entry of existing.messages) {
      if (!entry.normalized) {
        continue;
      }

      if (now - entry.ts > this.options.duplicateWindowSeconds) {
        continue;
      }

      duplicateMap.set(entry.normalized, (duplicateMap.get(entry.normalized) || 0) + 1);
    }

    existing.duplicateMap = duplicateMap;
    this.guildUserState.set(stateKey, existing);

    const messagesInWindow = existing.messages.filter((entry) => now - entry.ts <= this.options.spamWindowSeconds).length;
    const duplicateCount = normalized ? duplicateMap.get(normalized) || 0 : 0;

    const messageRateFeature = clamp(
      safeDivision(messagesInWindow, Math.max(1, this.options.spamMessageThreshold), 0),
      0,
      2
    );
    const duplicateFeature = clamp(
      safeDivision(duplicateCount, Math.max(1, this.options.duplicateThreshold), 0),
      0,
      2
    );
    const mentionFeature = clamp(
      safeDivision(mentionCount, Math.max(1, this.options.mentionThreshold), 0),
      0,
      2
    );
    const linkFeature = clamp(
      safeDivision(linkCount, Math.max(1, this.options.linkThreshold), 0),
      0,
      2
    );

    const score =
      0.46 * messageRateFeature + 0.34 * duplicateFeature + 0.26 * mentionFeature + 0.2 * linkFeature;

    const reasons = [];
    if (messageRateFeature >= 1) reasons.push("rapid message burst");
    if (duplicateFeature >= 1) reasons.push("repeated duplicate content");
    if (mentionFeature >= 1) reasons.push("mention spam");
    if (linkFeature >= 1) reasons.push("link spam");

    const severity = score >= 1.1 ? "high" : score >= 0.7 ? "medium" : score >= 0.45 ? "low" : "none";

    return {
      isSpam: severity !== "none",
      score,
      severity,
      reasons,
      metrics: {
        messagesInWindow,
        duplicateCount,
        mentionCount,
        linkCount
      }
    };
  }
}

export class RaidRiskEngine {
  constructor() {
    this.guildState = new Map();
    this.suspiciousSignals = new Map();
  }

  evaluateJoin({
    guildId,
    accountAgeDays,
    hasAvatar,
    profileScore,
    windowSeconds,
    joinRateThreshold
  }) {
    const now = Date.now() / 1000;
    const currentState =
      this.guildState.get(guildId) || {
        joinTimestamps: [],
        youngFlags: []
      };

    currentState.joinTimestamps.push(now);
    currentState.joinTimestamps = currentState.joinTimestamps.filter(
      (timestamp) => now - timestamp <= windowSeconds
    );

    const isYoung = accountAgeDays <= 7.0 ? 1 : 0;
    currentState.youngFlags.push([now, isYoung]);
    currentState.youngFlags = currentState.youngFlags.filter(
      ([timestamp]) => now - timestamp <= windowSeconds
    );

    this.guildState.set(guildId, currentState);

    const windowMinutes = Math.max(1.0, windowSeconds / 60.0);
    const shortWindowSeconds = clamp(Math.round(windowSeconds * 0.35), 15, 45);
    const shortWindowMinutes = Math.max(0.25, shortWindowSeconds / 60.0);
    const shortJoinCount = currentState.joinTimestamps.filter((timestamp) => now - timestamp <= shortWindowSeconds).length;
    const expectedShortCount = Math.max(
      1,
      Math.round(currentState.joinTimestamps.length * safeDivision(shortWindowSeconds, windowSeconds, 0.35))
    );

    const joinRatePerMinute = currentState.joinTimestamps.length / windowMinutes;
    const shortJoinRatePerMinute = shortJoinCount / shortWindowMinutes;
    const youngAccountRatio =
      currentState.youngFlags.length === 0
        ? 0
        : currentState.youngFlags.reduce((sum, [, flag]) => sum + flag, 0) / currentState.youngFlags.length;

    const burstFeature = clamp(joinRatePerMinute / Math.max(1.0, joinRateThreshold), 0, 1.5);
    const shortBurstFeature = clamp(shortJoinRatePerMinute / Math.max(1.0, joinRateThreshold * 1.25), 0, 2.0);
    const accelerationFeature = clamp(
      safeDivision(shortJoinCount - expectedShortCount, expectedShortCount, 0),
      0,
      2.0
    );
    const newAccountFeature = clamp((14.0 - accountAgeDays) / 14.0, 0, 1.0);
    const profileGapFeature = 1.0 - clamp(profileScore, 0, 1.0);
    const coordinatedFeature = clamp(youngAccountRatio, 0, 1.0);
    const avatarFeature = hasAvatar ? 0.0 : 1.0;

    const linear =
      -1.6 +
      1.95 * burstFeature +
      1.45 * shortBurstFeature +
      0.9 * accelerationFeature +
      1.35 * newAccountFeature +
      1.1 * coordinatedFeature +
      0.9 * profileGapFeature +
      0.45 * avatarFeature;

    const riskScore = 1.0 / (1.0 + Math.exp(-linear));

    let riskLevel = "low";
    if (riskScore >= 0.82) {
      riskLevel = "high";
    } else if (riskScore >= 0.6) {
      riskLevel = "medium";
    }

    const explanationParts = [];
    if (burstFeature >= 1.0) explanationParts.push("join burst above baseline");
    if (shortBurstFeature >= 1.0) explanationParts.push("sudden short-window spike");
    if (accelerationFeature >= 0.8) explanationParts.push("acceleration in join velocity");
    if (newAccountFeature >= 0.7) explanationParts.push("very new account");
    if (coordinatedFeature >= 0.6) explanationParts.push("cluster of young accounts");
    if (profileGapFeature >= 0.6) explanationParts.push("low profile completeness");
    if (explanationParts.length === 0) explanationParts.push("signals within expected range");

    return {
      accountAgeDays,
      hasAvatar,
      profileScore: clamp(profileScore, 0, 1),
      joinRatePerMinute,
      shortJoinRatePerMinute,
      shortJoinCount,
      accelerationFeature,
      youngAccountRatio,
      riskScore,
      riskLevel,
      explanation: explanationParts.join(", ")
    };
  }

  recordSuspiciousActivity({ guildId, userId, windowSeconds = 45 }) {
    const now = Date.now() / 1000;
    const normalizedWindow = clamp(Number(windowSeconds) || 45, 15, 300);

    const guildSignals =
      this.suspiciousSignals.get(guildId) || {
        events: []
      };

    guildSignals.events.push({ ts: now, userId: String(userId || "") });
    guildSignals.events = guildSignals.events.filter((entry) => now - entry.ts <= normalizedWindow);
    this.suspiciousSignals.set(guildId, guildSignals);

    const userSet = new Set(guildSignals.events.map((entry) => entry.userId).filter(Boolean));

    return {
      eventCount: guildSignals.events.length,
      uniqueUsers: userSet.size,
      windowSeconds: normalizedWindow
    };
  }
}

export function snowflakeToDate(snowflake) {
  try {
    const value = BigInt(String(snowflake));
    const timestampMs = (value >> 22n) + DISCORD_EPOCH_MS;
    return new Date(Number(timestampMs));
  } catch {
    return new Date();
  }
}
