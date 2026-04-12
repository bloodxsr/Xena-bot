import * as nodeEmoji from "node-emoji";

const CUSTOM_EMOJI_RE = /^<(a?):([^:>]+):(\d{5,22})>$/;
const CUSTOM_NAME_ID_RE = /^([^:\s]+):(\d{5,22})$/;
const UNICODE_VARIATION_SELECTOR_RE = /\uFE0F/g;
const EMOJI_ALIAS_RE = /^:[a-z0-9_+-]+:$/i;

function normalizeUnicodeEmojiKey(value) {
  return String(value ?? "")
    .normalize("NFKC")
    .replace(UNICODE_VARIATION_SELECTOR_RE, "")
    .trim();
}

function emojifyAlias(raw) {
  const text = String(raw ?? "").trim();
  if (!EMOJI_ALIAS_RE.test(text)) {
    return text;
  }

  const converted = String(nodeEmoji.emojify(text) || text).trim();
  return converted || text;
}

function aliasFromUnicode(raw) {
  const text = String(raw ?? "").trim();
  if (!text) {
    return "";
  }

  const alias = String(nodeEmoji.unemojify(text) || "").trim();
  if (!alias || alias === text || !alias.includes(":")) {
    return "";
  }

  return alias;
}

function pushUnique(target, value) {
  const text = String(value ?? "").trim();
  if (!text) {
    return;
  }

  if (!target.includes(text)) {
    target.push(text);
  }
}

export function normalizeEmojiInput(rawValue) {
  const raw = String(rawValue ?? "").trim();
  if (!raw) {
    throw new Error("emoji is required");
  }

  const mentionMatch = raw.match(CUSTOM_EMOJI_RE);
  if (mentionMatch) {
    const [, animatedPrefix, name, id] = mentionMatch;
    return {
      key: id,
      display: `<${animatedPrefix ? "a" : ""}:${name}:${id}>`,
      reactionValue: { name, id },
      customName: name,
      customId: id
    };
  }

  const nameIdMatch = raw.match(CUSTOM_NAME_ID_RE);
  if (nameIdMatch) {
    const [, name, id] = nameIdMatch;
    return {
      key: id,
      display: `<:${name}:${id}>`,
      reactionValue: { name, id },
      customName: name,
      customId: id
    };
  }

  const aliasInput = EMOJI_ALIAS_RE.test(raw);
  const unicodeValue = emojifyAlias(raw);
  if (aliasInput && unicodeValue === raw) {
    throw new Error("Unknown emoji alias. Use the actual emoji character (example: 🫡) or custom format <:name:id>.");
  }

  const normalizedUnicode = normalizeUnicodeEmojiKey(unicodeValue || raw);
  const alias = aliasFromUnicode(normalizedUnicode);

  return {
    key: normalizedUnicode || raw,
    display: normalizedUnicode || raw,
    reactionValue: normalizedUnicode || raw,
    aliases: alias ? [alias] : []
  };
}

export function emojiKeyFromGatewayEmoji(emoji) {
  const candidates = emojiKeyCandidatesFromGatewayEmoji(emoji);
  return candidates.length > 0 ? candidates[0] : "";
}

export function emojiKeyCandidatesFromGatewayEmoji(emoji) {
  if (!emoji) {
    return [];
  }

  const candidates = [];

  if (emoji.id) {
    pushUnique(candidates, emoji.id);
  }

  if (emoji.name) {
    pushUnique(candidates, emoji.name);
    pushUnique(candidates, normalizeUnicodeEmojiKey(emoji.name));

    const alias = aliasFromUnicode(emoji.name);
    if (alias) {
      pushUnique(candidates, alias);
    }
  }

  return candidates;
}

export function emojiRouteTokenFromNormalized(normalized) {
  if (!normalized || typeof normalized !== "object") {
    return "";
  }

  if (normalized.customName && normalized.customId) {
    return `${normalized.customName}:${normalized.customId}`;
  }

  if (typeof normalized.reactionValue === "string") {
    const text = String(normalized.reactionValue).trim();
    if (text) {
      return text;
    }
  }

  return String(normalized.display || normalized.key || "").trim();
}
