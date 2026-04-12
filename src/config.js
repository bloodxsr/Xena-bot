import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");
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

function firstDefined(...values) {
  for (const value of values) {
    if (value != null && String(value).trim() !== "") {
      return String(value).trim();
    }
  }
  return null;
}

export function loadConfig() {
  const token = firstDefined(
    process.env.FLUXER_BOT_TOKEN,
    process.env.BOT_TOKEN,
    process.env.TOKEN,
    readTrimmed(path.join(projectRoot, "token.txt"))
  );

  if (!token) {
    throw new Error("Missing bot token. Set FLUXER_BOT_TOKEN or create bot_js/token.txt.");
  }

  const googleApiKey = firstDefined(
    process.env.GOOGLE_API_KEY,
    process.env.GEMINI_API_KEY,
    readTrimmed(path.join(projectRoot, "google.txt"))
  );

  return {
    projectRoot,
    repoRoot,
    botToken: token,
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
    uptime: {
      enabled: parseBoolean(process.env.ENABLE_UPTIME_SERVER, false),
      host: String(process.env.UPTIME_HOST || "0.0.0.0"),
      port: Math.max(1, Math.min(parseInteger(process.env.UPTIME_PORT, 8080), 65535))
    }
  };
}
