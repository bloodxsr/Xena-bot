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

function sigmoid(value) {
  const safe = clamp(Number(value) || 0, -30, 30);
  return 1 / (1 + Math.exp(-safe));
}

function createRaidBaseline() {
  return {
    joinRateMean: 0.8,
    joinRateVar: 0.35,
    shortJoinRateMean: 0.95,
    shortJoinRateVar: 0.45,
    youngRatioMean: 0.2,
    youngRatioVar: 0.1,
    accountAgeMean: 20,
    accountAgeVar: 140,
    sampleCount: 0
  };
}

function createRaidModel() {
  return {
    bias: -1.85,
    weights: {
      burstFeature: 1.52,
      shortBurstFeature: 1.26,
      accelerationFeature: 0.88,
      newAccountFeature: 1.1,
      coordinatedFeature: 0.92,
      profileGapFeature: 0.68,
      avatarFeature: 0.24,
      burstAnomalyFeature: 1.06,
      shortAnomalyFeature: 0.94,
      youngAnomalyFeature: 0.81,
      accountAgeAnomalyFeature: 0.72,
      coordinatedSpamFeature: 1.19
    },
    updates: 0
  };
}

const HEURISTIC_WEIGHTS = {
  burstFeature: 1.75,
  shortBurstFeature: 1.42,
  accelerationFeature: 0.95,
  newAccountFeature: 1.08,
  coordinatedFeature: 0.96,
  profileGapFeature: 0.68,
  avatarFeature: 0.28,
  burstAnomalyFeature: 1.12,
  shortAnomalyFeature: 0.94,
  youngAnomalyFeature: 0.78,
  accountAgeAnomalyFeature: 0.64,
  coordinatedSpamFeature: 1.25
};

const FEATURE_LABELS = {
  burstFeature: "join burst above baseline",
  shortBurstFeature: "sudden short-window spike",
  accelerationFeature: "acceleration in join velocity",
  newAccountFeature: "very new account",
  coordinatedFeature: "cluster of young accounts",
  profileGapFeature: "low profile completeness",
  avatarFeature: "missing avatar signal",
  burstAnomalyFeature: "join-rate anomaly vs history",
  shortAnomalyFeature: "short-window anomaly vs history",
  youngAnomalyFeature: "young-account anomaly vs history",
  accountAgeAnomalyFeature: "account-age anomaly vs history",
  coordinatedSpamFeature: "coordinated spam telemetry"
};

function updateMeanAndVar(container, meanKey, varKey, value, alpha, minVariance) {
  const mean = Number(container[meanKey] || 0);
  const variance = Number(container[varKey] || minVariance);
  const delta = value - mean;
  container[meanKey] = mean + alpha * delta;
  container[varKey] = Math.max(minVariance, (1 - alpha) * variance + alpha * delta * delta);
}

function toFixedNumber(value, digits = 4) {
  return Number(Number(value || 0).toFixed(digits));
}

function toFeatureContributionList(features, heuristicWeight, modelWeight, modelWeights) {
  const list = [];
  for (const [key, featureValue] of Object.entries(features)) {
    const heuristicContribution = heuristicWeight * (HEURISTIC_WEIGHTS[key] || 0) * featureValue;
    const modelContribution = modelWeight * (modelWeights[key] || 0) * featureValue;
    const totalContribution = heuristicContribution + modelContribution;
    list.push({
      key,
      contribution: totalContribution
    });
  }

  list.sort((left, right) => right.contribution - left.contribution);
  return list;
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
  constructor(options = {}) {
    this.options = {
      learningRate: clamp(Number(options.learningRate || 0.018), 0.001, 0.2),
      weightDecay: clamp(Number(options.weightDecay || 0.0008), 0, 0.05),
      heuristicBlend: clamp(Number(options.heuristicBlend || 0.66), 0.35, 0.9),
      warmupEvents: clamp(Math.trunc(Number(options.warmupEvents || 40)), 5, 500),
      baselineAlpha: clamp(Number(options.baselineAlpha || 0.08), 0.01, 0.4),
      maxWeightMagnitude: clamp(Number(options.maxWeightMagnitude || 5), 2, 12)
    };

    this.guildState = new Map();
    this.suspiciousSignals = new Map();
  }

  ensureGuildState(guildId) {
    const key = String(guildId || "");
    if (!key) {
      return {
        joinTimestamps: [],
        youngFlags: [],
        baseline: createRaidBaseline(),
        model: createRaidModel()
      };
    }

    const existing = this.guildState.get(key);
    if (existing) {
      return existing;
    }

    const created = {
      joinTimestamps: [],
      youngFlags: [],
      baseline: createRaidBaseline(),
      model: createRaidModel()
    };

    this.guildState.set(key, created);
    return created;
  }

  getSuspiciousSnapshot(guildId, nowSeconds, windowSeconds) {
    const key = String(guildId || "");
    const normalizedWindow = clamp(Number(windowSeconds) || 45, 15, 300);
    const bucket = this.suspiciousSignals.get(key) || { events: [] };
    bucket.events = bucket.events.filter((entry) => nowSeconds - entry.ts <= normalizedWindow);
    this.suspiciousSignals.set(key, bucket);

    const uniqueUsers = new Set(bucket.events.map((entry) => entry.userId).filter(Boolean)).size;
    const weightedEvents = bucket.events.reduce((sum, entry) => sum + Number(entry.score || 1), 0);
    const eventRatePerMinute = weightedEvents / Math.max(0.25, normalizedWindow / 60);
    const coordinationDensity = safeDivision(weightedEvents, Math.max(1, uniqueUsers), 0);
    const suspiciousScore =
      0.48 * clamp(weightedEvents / Math.max(4, normalizedWindow / 12), 0, 2) +
      0.34 * clamp(uniqueUsers / 3, 0, 2) +
      0.24 * clamp(coordinationDensity / 2.2, 0, 2);

    return {
      eventCount: bucket.events.length,
      uniqueUsers,
      weightedEvents,
      eventRatePerMinute,
      coordinationDensity,
      suspiciousScore,
      windowSeconds: normalizedWindow
    };
  }

  modelLogit(model, features) {
    let value = Number(model.bias || 0);
    for (const [featureName, featureValue] of Object.entries(features)) {
      value += Number(model.weights[featureName] || 0) * featureValue;
    }
    return value;
  }

  weakLabel(features, accountAgeDays, profileScore) {
    const strongPositive =
      (features.shortBurstFeature >= 1.15 && features.accelerationFeature >= 0.65) ||
      (features.coordinatedSpamFeature >= 0.95 && features.burstFeature >= 0.9) ||
      (features.burstAnomalyFeature >= 1.05 && features.youngAnomalyFeature >= 0.78) ||
      (features.newAccountFeature >= 0.9 && features.coordinatedFeature >= 0.65 && features.shortBurstFeature >= 0.9);

    if (strongPositive) {
      return 1;
    }

    const strongNegative =
      features.burstFeature <= 0.55 &&
      features.shortBurstFeature <= 0.55 &&
      features.coordinatedSpamFeature <= 0.25 &&
      accountAgeDays >= 30 &&
      profileScore >= 0.5;

    if (strongNegative) {
      return 0;
    }

    return null;
  }

  updateModel(model, features, label, prediction) {
    const lr = this.options.learningRate;
    const wd = this.options.weightDecay;
    const maxMagnitude = this.options.maxWeightMagnitude;
    const error = prediction - label;

    model.bias = clamp(model.bias - lr * (error + wd * model.bias), -maxMagnitude, maxMagnitude);

    for (const [featureName, featureValue] of Object.entries(features)) {
      const current = Number(model.weights[featureName] || 0);
      const gradient = error * featureValue + wd * current;
      model.weights[featureName] = clamp(current - lr * gradient, -maxMagnitude, maxMagnitude);
    }

    model.updates += 1;
  }

  updateBaseline(baseline, joinRatePerMinute, shortJoinRatePerMinute, youngAccountRatio, accountAgeDays) {
    const alpha = this.options.baselineAlpha;

    updateMeanAndVar(baseline, "joinRateMean", "joinRateVar", joinRatePerMinute, alpha, 0.03);
    updateMeanAndVar(baseline, "shortJoinRateMean", "shortJoinRateVar", shortJoinRatePerMinute, alpha, 0.04);
    updateMeanAndVar(baseline, "youngRatioMean", "youngRatioVar", youngAccountRatio, alpha, 0.02);
    updateMeanAndVar(baseline, "accountAgeMean", "accountAgeVar", accountAgeDays, alpha, 12);
    baseline.sampleCount += 1;
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
    const state = this.ensureGuildState(guildId);
    const baseline = state.baseline;
    const model = state.model;

    const normalizedWindowSeconds = clamp(Number(windowSeconds) || 90, 30, 3600);

    state.joinTimestamps.push(now);
    state.joinTimestamps = state.joinTimestamps.filter(
      (timestamp) => now - timestamp <= normalizedWindowSeconds
    );

    const normalizedAccountAgeDays = Math.max(0, Number(accountAgeDays) || 0);
    const normalizedProfileScore = clamp(Number(profileScore) || 0, 0, 1);
    const isYoung = normalizedAccountAgeDays <= 7.0 ? 1 : 0;

    state.youngFlags.push([now, isYoung]);
    state.youngFlags = state.youngFlags.filter(([timestamp]) => now - timestamp <= normalizedWindowSeconds);

    const windowMinutes = Math.max(1.0, normalizedWindowSeconds / 60.0);
    const shortWindowSeconds = clamp(Math.round(normalizedWindowSeconds * 0.35), 15, 45);
    const shortWindowMinutes = Math.max(0.25, shortWindowSeconds / 60.0);
    const shortJoinCount = state.joinTimestamps.filter((timestamp) => now - timestamp <= shortWindowSeconds).length;
    const expectedShortCount = Math.max(
      1,
      Math.round(state.joinTimestamps.length * safeDivision(shortWindowSeconds, normalizedWindowSeconds, 0.35))
    );

    const joinRatePerMinute = state.joinTimestamps.length / windowMinutes;
    const shortJoinRatePerMinute = shortJoinCount / shortWindowMinutes;
    const youngAccountRatio =
      state.youngFlags.length === 0
        ? 0
        : state.youngFlags.reduce((sum, [, flag]) => sum + flag, 0) / state.youngFlags.length;

    const suspicious = this.getSuspiciousSnapshot(guildId, now, normalizedWindowSeconds);

    const joinRateStd = Math.sqrt(Math.max(baseline.joinRateVar, 0.03));
    const shortJoinRateStd = Math.sqrt(Math.max(baseline.shortJoinRateVar, 0.04));
    const youngRatioStd = Math.sqrt(Math.max(baseline.youngRatioVar, 0.02));
    const accountAgeStd = Math.sqrt(Math.max(baseline.accountAgeVar, 12));

    const joinRateZ = safeDivision(joinRatePerMinute - baseline.joinRateMean, joinRateStd, 0);
    const shortJoinRateZ = safeDivision(shortJoinRatePerMinute - baseline.shortJoinRateMean, shortJoinRateStd, 0);
    const youngRatioZ = safeDivision(youngAccountRatio - baseline.youngRatioMean, youngRatioStd, 0);
    const accountAgeZ = safeDivision(normalizedAccountAgeDays - baseline.accountAgeMean, accountAgeStd, 0);

    const burstFeature = clamp(joinRatePerMinute / Math.max(1.0, joinRateThreshold), 0, 1.8);
    const shortBurstFeature = clamp(shortJoinRatePerMinute / Math.max(1.0, joinRateThreshold * 1.2), 0, 2.2);
    const accelerationFeature = clamp(
      safeDivision(shortJoinCount - expectedShortCount, expectedShortCount, 0),
      0,
      2.1
    );
    const newAccountFeature = clamp((14.0 - normalizedAccountAgeDays) / 14.0, 0, 1.0);
    const profileGapFeature = 1.0 - normalizedProfileScore;
    const coordinatedFeature = clamp(youngAccountRatio, 0, 1.0);
    const avatarFeature = hasAvatar ? 0.0 : 1.0;

    const burstAnomalyFeature = clamp((joinRateZ + 0.55) / 2.2, 0, 2);
    const shortAnomalyFeature = clamp((shortJoinRateZ + 0.5) / 2.1, 0, 2);
    const youngAnomalyFeature = clamp((youngRatioZ + 0.35) / 1.9, 0, 2);
    const accountAgeAnomalyFeature = clamp(((-accountAgeZ) + 0.2) / 1.8, 0, 2);
    const coordinatedSpamFeature = clamp(suspicious.suspiciousScore / 1.15, 0, 2);

    const features = {
      burstFeature,
      shortBurstFeature,
      accelerationFeature,
      newAccountFeature,
      coordinatedFeature,
      profileGapFeature,
      avatarFeature,
      burstAnomalyFeature,
      shortAnomalyFeature,
      youngAnomalyFeature,
      accountAgeAnomalyFeature,
      coordinatedSpamFeature
    };

    const heuristicLogit =
      -2.15 +
      1.75 * burstFeature +
      1.42 * shortBurstFeature +
      0.95 * accelerationFeature +
      1.08 * newAccountFeature +
      0.96 * coordinatedFeature +
      0.68 * profileGapFeature +
      0.28 * avatarFeature +
      1.12 * burstAnomalyFeature +
      0.94 * shortAnomalyFeature +
      0.78 * youngAnomalyFeature +
      0.64 * accountAgeAnomalyFeature +
      1.25 * coordinatedSpamFeature;

    const modelLogit = this.modelLogit(model, features);

    const warmupProgress = clamp(safeDivision(baseline.sampleCount, this.options.warmupEvents, 0), 0, 1);
    const heuristicWeight = clamp(this.options.heuristicBlend + (1 - warmupProgress) * 0.16, 0.45, 0.92);
    const modelWeight = 1 - heuristicWeight;

    const heuristicScore = sigmoid(heuristicLogit);
    const adaptiveScore = sigmoid(modelLogit);
    const blendedLogit = heuristicWeight * heuristicLogit + modelWeight * modelLogit;
    const agreement = 1 - clamp(Math.abs(heuristicScore - adaptiveScore) / 0.6, 0, 1);
    const modelConfidence = clamp(0.35 + 0.45 * warmupProgress + 0.2 * agreement, 0.2, 0.98);
    const riskScore = clamp(sigmoid(blendedLogit) * (0.85 + 0.15 * modelConfidence), 0, 1);

    let riskLevel = "low";
    if (riskScore >= 0.85 || (riskScore >= 0.78 && coordinatedSpamFeature >= 1.0)) {
      riskLevel = "high";
    } else if (riskScore >= 0.62 || (riskScore >= 0.55 && shortAnomalyFeature >= 0.9)) {
      riskLevel = "medium";
    }

    const contributions = toFeatureContributionList(features, heuristicWeight, modelWeight, model.weights);
    const explanationParts = Array.from(
      new Set(
        contributions
          .filter((entry) => entry.contribution >= 0.32)
          .slice(0, 4)
          .map((entry) => FEATURE_LABELS[entry.key] || entry.key)
      )
    );

    if (explanationParts.length === 0) {
      explanationParts.push("signals within expected range");
    }

    const weakLabel = this.weakLabel(features, normalizedAccountAgeDays, normalizedProfileScore);
    if (weakLabel != null) {
      this.updateModel(model, features, weakLabel, adaptiveScore);
    }

    this.updateBaseline(
      baseline,
      joinRatePerMinute,
      shortJoinRatePerMinute,
      youngAccountRatio,
      normalizedAccountAgeDays
    );
    return {
      accountAgeDays: normalizedAccountAgeDays,
      hasAvatar,
      profileScore: normalizedProfileScore,
      joinRatePerMinute,
      shortJoinRatePerMinute,
      shortJoinCount,
      accelerationFeature,
      youngAccountRatio,
      riskScore,
      riskLevel,
      heuristicScore,
      adaptiveScore,
      modelConfidence,
      anomaly: {
        joinRateZ,
        shortJoinRateZ,
        youngRatioZ,
        accountAgeZ
      },
      suspiciousActivity: {
        eventCount: suspicious.eventCount,
        uniqueUsers: suspicious.uniqueUsers,
        weightedEvents: toFixedNumber(suspicious.weightedEvents),
        eventRatePerMinute: toFixedNumber(suspicious.eventRatePerMinute),
        suspiciousScore: toFixedNumber(suspicious.suspiciousScore)
      },
      modelState: {
        sampleCount: baseline.sampleCount,
        updates: model.updates,
        heuristicWeight: toFixedNumber(heuristicWeight),
        modelWeight: toFixedNumber(modelWeight)
      },
      explanation: explanationParts.join(", ")
    };
  }

  recordSuspiciousActivity({ guildId, userId, windowSeconds = 45, score = 1 }) {
    const now = Date.now() / 1000;
    const normalizedWindow = clamp(Number(windowSeconds) || 45, 15, 300);
    const normalizedScore = clamp(Number(score) || 1, 0.2, 4);

    const key = String(guildId || "");
    const guildSignals = this.suspiciousSignals.get(key) || { events: [] };
    guildSignals.events.push({ ts: now, userId: String(userId || ""), score: normalizedScore });
    this.suspiciousSignals.set(key, guildSignals);

    const snapshot = this.getSuspiciousSnapshot(key, now, normalizedWindow);
    return {
      eventCount: snapshot.eventCount,
      uniqueUsers: snapshot.uniqueUsers,
      weightedEvents: toFixedNumber(snapshot.weightedEvents),
      eventRatePerMinute: toFixedNumber(snapshot.eventRatePerMinute),
      coordinationDensity: toFixedNumber(snapshot.coordinationDensity),
      suspiciousScore: toFixedNumber(snapshot.suspiciousScore),
      windowSeconds: snapshot.windowSeconds
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
