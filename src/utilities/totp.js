import crypto from "node:crypto";

const BASE32_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function normalizeBase32Secret(secretBase32) {
  const normalized = String(secretBase32 || "")
    .toUpperCase()
    .replace(/\s+/g, "")
    .replace(/-/g, "")
    .replace(/=+$/g, "");

  if (!/^[A-Z2-7]+$/.test(normalized)) {
    throw new Error("Invalid base32 secret.");
  }

  return normalized;
}

function decodeBase32(secretBase32) {
  const normalized = normalizeBase32Secret(secretBase32);
  let bits = "";

  for (const character of normalized) {
    const index = BASE32_ALPHABET.indexOf(character);
    if (index < 0) {
      throw new Error("Invalid base32 secret.");
    }
    bits += index.toString(2).padStart(5, "0");
  }

  const bytes = [];
  for (let offset = 0; offset + 8 <= bits.length; offset += 8) {
    bytes.push(Number.parseInt(bits.slice(offset, offset + 8), 2));
  }

  return Buffer.from(bytes);
}

function hotp(secretBase32, counter, digits = 6, algorithm = "sha1") {
  const normalizedDigits = clamp(Number(digits) || 6, 6, 8);
  const key = decodeBase32(secretBase32);
  const counterBuffer = Buffer.alloc(8);
  counterBuffer.writeBigUInt64BE(BigInt(counter));

  const digest = crypto.createHmac(String(algorithm || "sha1").toLowerCase(), key).update(counterBuffer).digest();
  const offset = digest[digest.length - 1] & 0x0f;

  const binary =
    ((digest[offset] & 0x7f) << 24) |
    ((digest[offset + 1] & 0xff) << 16) |
    ((digest[offset + 2] & 0xff) << 8) |
    (digest[offset + 3] & 0xff);

  const modulo = 10 ** normalizedDigits;
  return String(binary % modulo).padStart(normalizedDigits, "0");
}

export function normalizeTotpCode(rawCode) {
  return String(rawCode || "").replace(/\D+/g, "");
}

export function generateTotpSecret(length = 32) {
  const normalizedLength = clamp(Number(length) || 32, 16, 64);
  const bytes = crypto.randomBytes(Math.ceil((normalizedLength * 5) / 8));

  let bits = "";
  for (const byte of bytes) {
    bits += byte.toString(2).padStart(8, "0");
  }

  let output = "";
  for (let offset = 0; offset < bits.length; offset += 5) {
    const chunk = bits.slice(offset, offset + 5).padEnd(5, "0");
    output += BASE32_ALPHABET[Number.parseInt(chunk, 2)];
  }

  return output.slice(0, normalizedLength);
}

export function formatTotpSecret(secretBase32, chunkSize = 4) {
  const normalized = normalizeBase32Secret(secretBase32);
  const size = clamp(Number(chunkSize) || 4, 2, 8);

  const groups = [];
  for (let offset = 0; offset < normalized.length; offset += size) {
    groups.push(normalized.slice(offset, offset + size));
  }

  return groups.join(" ");
}

export function createTotpCode(secretBase32, options = {}) {
  const periodSeconds = clamp(Number(options.periodSeconds) || 30, 15, 120);
  const digits = clamp(Number(options.digits) || 6, 6, 8);
  const algorithm = String(options.algorithm || "sha1");

  const unixSeconds = Math.floor(Date.now() / 1000);
  const counter = Math.floor(unixSeconds / periodSeconds);
  return hotp(secretBase32, counter, digits, algorithm);
}

export function verifyTotpCode(secretBase32, rawCode, options = {}) {
  const periodSeconds = clamp(Number(options.periodSeconds) || 30, 15, 120);
  const digits = clamp(Number(options.digits) || 6, 6, 8);
  const algorithm = String(options.algorithm || "sha1");
  const windowSteps = clamp(Number(options.windowSteps) || 1, 0, 5);

  const normalizedCode = normalizeTotpCode(rawCode);
  if (normalizedCode.length !== digits) {
    return false;
  }

  const unixSeconds = Math.floor(Date.now() / 1000);
  const currentCounter = Math.floor(unixSeconds / periodSeconds);

  for (let offset = -windowSteps; offset <= windowSteps; offset += 1) {
    const candidateCounter = currentCounter + offset;
    if (candidateCounter < 0) {
      continue;
    }

    const candidate = hotp(secretBase32, candidateCounter, digits, algorithm);
    if (candidate === normalizedCode) {
      return true;
    }
  }

  return false;
}

export function buildTotpAuthUri({ secretBase32, issuer, accountName, digits = 6, periodSeconds = 30, algorithm = "SHA1" }) {
  const normalizedSecret = normalizeBase32Secret(secretBase32);
  const normalizedIssuer = String(issuer || "FluxerBot").trim() || "FluxerBot";
  const normalizedAccountName = String(accountName || "staff").trim() || "staff";
  const normalizedDigits = clamp(Number(digits) || 6, 6, 8);
  const normalizedPeriod = clamp(Number(periodSeconds) || 30, 15, 120);
  const normalizedAlgorithm = String(algorithm || "SHA1").trim().toUpperCase() || "SHA1";

  const label = `${normalizedIssuer}:${normalizedAccountName}`;

  return (
    `otpauth://totp/${encodeURIComponent(label)}` +
    `?secret=${encodeURIComponent(normalizedSecret)}` +
    `&issuer=${encodeURIComponent(normalizedIssuer)}` +
    `&algorithm=${encodeURIComponent(normalizedAlgorithm)}` +
    `&digits=${normalizedDigits}` +
    `&period=${normalizedPeriod}`
  );
}
