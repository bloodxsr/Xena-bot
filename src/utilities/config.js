import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const srcRoot = path.resolve(__dirname, "..");
const projectRoot = path.resolve(srcRoot, "..");
const repoRoot = path.resolve(projectRoot, "..");

dotenv.config({ path: path.join(projectRoot, ".env") });
dotenv.config({ path: path.join(repoRoot, ".env"), override: false });

function readTrimmed(filePath) {
  try {
    const text = fs.readFileSync(filePath, "utf-8").trim();
    return text || null;
  } catch {
    return null;
  }
}

function parsePrefixes(rawValue) {
  const raw = String(rawValue ?? "/,!");
  const parsed = raw
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);

  return parsed.length > 0 ? parsed : ["/", "!"];
}

function parseBoolean(rawValue, fallback = false) {
  if (rawValue == null) {
    return fallback;
  }

  const text = String(rawValue).trim().toLowerCase();
  if (["1", "true", "yes", "on", "enabled"].includes(text)) {
    return true;
  }
  if (["0", "false", "no", "off", "disabled"].includes(text)) {
    return false;
  }
  return fallback;
}

function parseInteger(rawValue, fallback) {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.trunc(parsed);
}

function parseNumber(rawValue, fallback) {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return parsed;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function resolvePath(rawPath, fallbackAbsolutePath) {
  if (!rawPath || String(rawPath).trim() === "") {
    return fallbackAbsolutePath;
  }

  const text = String(rawPath).trim();
  if (path.isAbsolute(text)) {
    return text;
  }

  return path.resolve(projectRoot, text);
}

function normalizeUrl(rawValue, fallback) {
  const text = String(rawValue || fallback).trim();
  if (!text) {
    return fallback;
  }

  return text.replace(/\/+$/, "");
}

function firstDefined(...values) {
  for (const value of values) {
    if (value != null && String(value).trim() !== "") {
      return String(value).trim();
    }
  }
  return null;
}

export function loadConfig() {
  const nodeEnv = String(process.env.NODE_ENV || "development")
    .trim()
    .toLowerCase();
  const allowFileSecretFallback =
    nodeEnv !== "production" && parseBoolean(process.env.ALLOW_FILE_SECRET_FALLBACK, true);
  const tokenFilePath = path.join(projectRoot, "token.txt");
  const googleFilePath = path.join(projectRoot, "google.txt");

  const token = firstDefined(
    process.env.FLUXER_BOT_TOKEN,
    process.env.BOT_TOKEN,
    process.env.TOKEN,
    allowFileSecretFallback ? readTrimmed(tokenFilePath) : null
  );

  if (!token) {
    throw new Error(
      nodeEnv === "production"
        ? "Missing bot token. Set FLUXER_BOT_TOKEN in environment variables."
        : "Missing bot token. Set FLUXER_BOT_TOKEN in .env or use bot_js/token.txt (development only)."
    );
  }

  const googleApiKey = firstDefined(
    process.env.GOOGLE_API_KEY,
    process.env.GEMINI_API_KEY,
    allowFileSecretFallback ? readTrimmed(googleFilePath) : null
  );

  const messageBaseUrl = normalizeUrl(process.env.FLUXER_WEB_BASE_URL, "https://fluxer.app");
  const raidMlHost = String(process.env.RAID_ML_HOST || "127.0.0.1").trim() || "127.0.0.1";
  const raidMlPort = clamp(parseInteger(process.env.RAID_ML_PORT, 8787), 1, 65535);
  const raidMlServiceUrl = String(process.env.RAID_ML_SERVICE_URL || `http://${raidMlHost}:${raidMlPort}`).trim();

  const levelingMinXpPerMessage = clamp(parseInteger(process.env.LEVELING_XP_MIN, 8), 1, 100);
  const levelingMaxXpPerMessage = Math.max(
    levelingMinXpPerMessage,
    clamp(parseInteger(process.env.LEVELING_XP_MAX, 16), 1, 200)
  );

  return {
    projectRoot,
    repoRoot,
    nodeEnv,
    allowFileSecretFallback,
    botToken: token,
    web: {
      baseUrl: messageBaseUrl
    },
    prefixes: parsePrefixes(process.env.BOT_PREFIXES),
    databasePath: resolvePath(process.env.DB_PATH, path.join(projectRoot, "data", "warnings.db")),
    wordsJsonPath: resolvePath(process.env.WORDS_JSON_PATH, path.join(projectRoot, "data", "words.json")),
    maxWarnings: Math.max(1, Math.min(parseInteger(process.env.MAX_WARNINGS, 4), 20)),
    ai: {
      apiKey: googleApiKey,
      modelName: String(process.env.AI_MODEL_NAME || "gemini-2.5-flash"),
      rateLimitSeconds: Math.max(1, Math.min(parseInteger(process.env.AI_RATE_LIMIT_SECONDS, 5), 60)),
      maxResponseLength: Math.max(250, Math.min(parseInteger(process.env.AI_MAX_RESPONSE_LENGTH, 1500), 4000)),
      maxQuestionLength: Math.max(50, Math.min(parseInteger(process.env.AI_MAX_QUESTION_LENGTH, 600), 2000))
    },
    automod: {
      spamDetectionEnabled: parseBoolean(process.env.SPAM_DETECTION_ENABLED, true),
      spamWindowSeconds: clamp(parseInteger(process.env.SPAM_WINDOW_SECONDS, 8), 4, 60),
      spamMessageThreshold: clamp(parseInteger(process.env.SPAM_MESSAGE_THRESHOLD, 6), 3, 20),
      duplicateWindowSeconds: clamp(parseInteger(process.env.SPAM_DUPLICATE_WINDOW_SECONDS, 20), 6, 120),
      duplicateThreshold: clamp(parseInteger(process.env.SPAM_DUPLICATE_THRESHOLD, 3), 2, 10),
      mentionThreshold: clamp(parseInteger(process.env.SPAM_MENTION_THRESHOLD, 6), 3, 30),
      linkThreshold: clamp(parseInteger(process.env.SPAM_LINK_THRESHOLD, 4), 2, 20),
      warningOnlyThreshold: clamp(parseNumber(process.env.SPAM_WARNING_THRESHOLD, 0.45), 0.2, 2),
      spamScoreMuteThreshold: clamp(parseNumber(process.env.SPAM_MUTE_THRESHOLD, 0.75), 0.3, 2),
      timeoutSeconds: clamp(parseInteger(process.env.AUTOMOD_TIMEOUT_SECONDS, 600), 60, 86400),
      severeTimeoutSeconds: clamp(parseInteger(process.env.AUTOMOD_SEVERE_TIMEOUT_SECONDS, 1800), 60, 86400),
      raidEscalationWindowSeconds: clamp(parseInteger(process.env.RAID_ESCALATION_WINDOW_SECONDS, 45), 15, 300),
      raidEscalationEventThreshold: clamp(parseInteger(process.env.RAID_ESCALATION_EVENT_THRESHOLD, 6), 3, 30),
      raidEscalationUserThreshold: clamp(parseInteger(process.env.RAID_ESCALATION_USER_THRESHOLD, 3), 2, 20)
    },
    raidMl: {
      backend: String(process.env.RAID_ML_BACKEND || "js").trim().toLowerCase() === "rust" ? "rust" : "js",
      serviceUrl: raidMlServiceUrl,
      timeoutMs: clamp(parseInteger(process.env.RAID_ML_TIMEOUT_MS, 350), 50, 10000),
      maxConsecutiveFailures: clamp(parseInteger(process.env.RAID_ML_MAX_CONSECUTIVE_FAILURES, 4), 1, 50),
      circuitResetMs: clamp(parseInteger(process.env.RAID_ML_CIRCUIT_RESET_MS, 15000), 1000, 120000),
      healthCheckIntervalMs: clamp(parseInteger(process.env.RAID_ML_HEALTH_CHECK_INTERVAL_MS, 10000), 2000, 120000),
      learningRate: clamp(parseNumber(process.env.RAID_ML_LEARNING_RATE, 0.018), 0.001, 0.2),
      weightDecay: clamp(parseNumber(process.env.RAID_ML_WEIGHT_DECAY, 0.0008), 0, 0.05),
      heuristicBlend: clamp(parseNumber(process.env.RAID_ML_HEURISTIC_BLEND, 0.66), 0.35, 0.9),
      warmupEvents: clamp(parseInteger(process.env.RAID_ML_WARMUP_EVENTS, 40), 5, 500),
      baselineAlpha: clamp(parseNumber(process.env.RAID_ML_BASELINE_ALPHA, 0.08), 0.01, 0.4),
      maxWeightMagnitude: clamp(parseNumber(process.env.RAID_ML_MAX_WEIGHT_MAGNITUDE, 5), 2, 12)
    },
    leveling: {
      cooldownSeconds: clamp(parseInteger(process.env.LEVELING_XP_COOLDOWN_SECONDS, 45), 5, 600),
      minXpPerMessage: levelingMinXpPerMessage,
      maxXpPerMessage: levelingMaxXpPerMessage,
      minMessageLength: clamp(parseInteger(process.env.LEVELING_MIN_MESSAGE_LENGTH, 4), 1, 400),
      ignoreCommandMessages: parseBoolean(process.env.LEVELING_IGNORE_COMMAND_MESSAGES, true),
      announceLevelUp: parseBoolean(process.env.LEVELING_ANNOUNCE_LEVEL_UP, true)
    },
    totp: {
      enabled: parseBoolean(process.env.TOTP_ENABLED, true),
      issuer: String(process.env.TOTP_ISSUER || "FluxerBot").trim() || "FluxerBot",
      authWindowDays: clamp(parseInteger(process.env.TOTP_AUTH_WINDOW_DAYS, 30), 1, 365),
      codeDigits: clamp(parseInteger(process.env.TOTP_CODE_DIGITS, 6), 6, 8),
      periodSeconds: clamp(parseInteger(process.env.TOTP_PERIOD_SECONDS, 30), 15, 120),
      verifyWindowSteps: clamp(parseInteger(process.env.TOTP_VERIFY_WINDOW_STEPS, 1), 0, 5),
      secretLength: clamp(parseInteger(process.env.TOTP_SECRET_LENGTH, 32), 16, 64)
    },
    uptime: {
      enabled: parseBoolean(process.env.ENABLE_UPTIME_SERVER, false),
      host: String(process.env.UPTIME_HOST || "0.0.0.0"),
      port: Math.max(1, Math.min(parseInteger(process.env.UPTIME_PORT, 8080), 65535))
    }
  };
}
