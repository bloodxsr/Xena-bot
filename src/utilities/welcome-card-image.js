import { createCanvas, loadImage } from "@napi-rs/canvas";

const WIDTH = 1040;
const HEIGHT = 360;
const AVATAR_SIZE = 132;

function normalizeHexColor(value, fallback) {
  const text = String(value ?? "").trim();
  if (/^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(text)) {
    return text.toLowerCase();
  }

  return fallback;
}

function normalizeOpacity(value, fallback) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }

  return Math.max(0, Math.min(numeric, 1));
}

function fontFamilyFromStyle(style) {
  const key = String(style || "default").trim().toLowerCase();
  if (key === "clean") {
    return '"Segoe UI", "Inter", sans-serif';
  }

  if (key === "cinematic") {
    return '"Georgia", "Times New Roman", serif';
  }

  return "sans-serif";
}

function drawRoundedRect(ctx, x, y, width, height, radius) {
  const r = Math.max(0, Math.min(radius, width / 2, height / 2));
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + width - r, y);
  ctx.quadraticCurveTo(x + width, y, x + width, y + r);
  ctx.lineTo(x + width, y + height - r);
  ctx.quadraticCurveTo(x + width, y + height, x + width - r, y + height);
  ctx.lineTo(x + r, y + height);
  ctx.quadraticCurveTo(x, y + height, x, y + height - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
}

function fillRoundedRect(ctx, x, y, width, height, radius, fillStyle) {
  ctx.save();
  drawRoundedRect(ctx, x, y, width, height, radius);
  ctx.fillStyle = fillStyle;
  ctx.fill();
  ctx.restore();
}

function drawImageCover(ctx, image, width, height) {
  const imageWidth = Number(image?.width || 0);
  const imageHeight = Number(image?.height || 0);
  if (!Number.isFinite(imageWidth) || !Number.isFinite(imageHeight) || imageWidth <= 0 || imageHeight <= 0) {
    return;
  }

  const scale = Math.max(width / imageWidth, height / imageHeight);
  const drawWidth = imageWidth * scale;
  const drawHeight = imageHeight * scale;
  const offsetX = (width - drawWidth) / 2;
  const offsetY = (height - drawHeight) / 2;
  ctx.drawImage(image, offsetX, offsetY, drawWidth, drawHeight);
}

function initialsFromName(displayName) {
  const parts = String(displayName || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2);

  if (parts.length === 0) {
    return "U";
  }

  return parts.map((entry) => entry[0].toUpperCase()).join("");
}

async function drawAvatar(ctx, avatarUrl, displayName) {
  const x = 52;
  const y = (HEIGHT - AVATAR_SIZE) / 2;
  const radius = AVATAR_SIZE / 2;

  fillRoundedRect(ctx, x - 5, y - 5, AVATAR_SIZE + 10, AVATAR_SIZE + 10, radius + 8, "rgba(248, 250, 252, 0.18)");

  let image = null;
  if (avatarUrl) {
    try {
      image = await loadImage(String(avatarUrl));
    } catch {
      image = null;
    }
  }

  ctx.save();
  ctx.beginPath();
  ctx.arc(x + radius, y + radius, radius, 0, Math.PI * 2);
  ctx.closePath();
  ctx.clip();

  if (image) {
    ctx.drawImage(image, x, y, AVATAR_SIZE, AVATAR_SIZE);
  } else {
    const gradient = ctx.createLinearGradient(x, y, x + AVATAR_SIZE, y + AVATAR_SIZE);
    gradient.addColorStop(0, "#334155");
    gradient.addColorStop(1, "#0f172a");
    ctx.fillStyle = gradient;
    ctx.fillRect(x, y, AVATAR_SIZE, AVATAR_SIZE);

    ctx.fillStyle = "#e2e8f0";
    ctx.font = "700 48px sans-serif";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillText(initialsFromName(displayName), x + radius, y + radius + 2);
    ctx.textAlign = "left";
    ctx.textBaseline = "alphabetic";
  }

  ctx.restore();
}

export async function renderWelcomeCardImage(options) {
  const guildName = String(options.guildName || "Server").trim() || "Server";
  const displayName = String(options.displayName || "Member").trim() || "Member";
  const titleText = String(options.titleText || `Welcome to ${guildName}`).trim() || `Welcome to ${guildName}`;
  const subtitleText = String(options.subtitleText || "We're glad you're here.").trim() || "We're glad you're here.";
  const memberCount = Math.max(0, Number(options.memberCount || 0));
  const avatarUrl = options.avatarUrl ? String(options.avatarUrl) : null;

  const primaryColor = normalizeHexColor(options.primaryColor, "#f8fafc");
  const accentColor = normalizeHexColor(options.accentColor, "#6dd6ff");
  const overlayOpacity = normalizeOpacity(options.overlayOpacity, 0.48);
  const fontFamily = fontFamilyFromStyle(options.fontStyle);
  const backgroundUrl = options.backgroundUrl ? String(options.backgroundUrl).trim() : "";

  const canvas = createCanvas(WIDTH, HEIGHT);
  const ctx = canvas.getContext("2d");

  let backgroundImage = null;
  if (backgroundUrl) {
    try {
      backgroundImage = await loadImage(backgroundUrl);
    } catch {
      backgroundImage = null;
    }
  }

  if (backgroundImage) {
    drawImageCover(ctx, backgroundImage, WIDTH, HEIGHT);
    ctx.fillStyle = `rgba(3, 8, 18, ${overlayOpacity})`;
    ctx.fillRect(0, 0, WIDTH, HEIGHT);
  } else {
    const baseGradient = ctx.createLinearGradient(0, 0, WIDTH, HEIGHT);
    baseGradient.addColorStop(0, "#0b1427");
    baseGradient.addColorStop(0.5, "#1b2a47");
    baseGradient.addColorStop(1, "#0f2038");
    ctx.fillStyle = baseGradient;
    ctx.fillRect(0, 0, WIDTH, HEIGHT);
  }

  const accentGlow = ctx.createRadialGradient(860, 24, 20, 860, 24, 340);
  accentGlow.addColorStop(0, "rgba(109, 214, 255, 0.5)");
  accentGlow.addColorStop(1, "rgba(109, 214, 255, 0)");
  ctx.fillStyle = accentGlow;
  ctx.fillRect(0, 0, WIDTH, HEIGHT);

  fillRoundedRect(ctx, 24, 24, WIDTH - 48, HEIGHT - 48, 24, "rgba(255, 255, 255, 0.08)");

  await drawAvatar(ctx, avatarUrl, displayName);

  fillRoundedRect(ctx, 214, 72, WIDTH - 266, 218, 20, "rgba(8, 14, 26, 0.52)");

  ctx.fillStyle = primaryColor;
  ctx.font = `700 44px ${fontFamily}`;
  ctx.fillText(titleText.slice(0, 42), 244, 136);

  ctx.fillStyle = accentColor;
  ctx.font = `600 24px ${fontFamily}`;
  ctx.fillText(subtitleText.slice(0, 68), 244, 182);

  ctx.fillStyle = "#cbd5e1";
  ctx.font = `500 19px ${fontFamily}`;
  ctx.fillText(`Member: ${displayName}`.slice(0, 58), 244, 228);
  ctx.fillText(`Server: ${guildName}`.slice(0, 58), 244, 258);

  if (memberCount > 0) {
    fillRoundedRect(ctx, WIDTH - 194, 40, 140, 62, 14, "rgba(15, 23, 42, 0.8)");
    ctx.fillStyle = accentColor;
    ctx.font = `600 14px ${fontFamily}`;
    ctx.fillText("MEMBER #", WIDTH - 176, 64);
    ctx.fillStyle = primaryColor;
    ctx.font = `700 24px ${fontFamily}`;
    ctx.fillText(String(Math.trunc(memberCount)), WIDTH - 176, 89);
  }

  return canvas.toBuffer("image/png");
}
