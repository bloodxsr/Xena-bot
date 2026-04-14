import { createCanvas, loadImage } from "@napi-rs/canvas";

const WIDTH = 980;
const HEIGHT = 320;
const AVATAR_SIZE = 164;

function normalizeHexColor(value, fallback) {
  const text = String(value ?? "").trim();
  if (/^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(text)) {
    return text.toLowerCase();
  }

  return fallback;
}

function colorWithAlpha(hexColor, alphaHex) {
  const normalized = normalizeHexColor(hexColor, "#000000");
  if (normalized.length === 4) {
    const r = normalized[1];
    const g = normalized[2];
    const b = normalized[3];
    return `#${r}${r}${g}${g}${b}${b}${alphaHex}`;
  }

  return `${normalized}${alphaHex}`;
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

  if (key === "cyber") {
    return '"Consolas", "Courier New", monospace';
  }

  return "sans-serif";
}

function clamp(value, min, max) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return min;
  }

  return Math.max(min, Math.min(max, numeric));
}

function formatInteger(value) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) {
    return "0";
  }

  return Math.trunc(numeric).toLocaleString("en-US");
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

function drawBadge(ctx, options) {
  const x = options.x;
  const y = options.y;
  const width = options.width;
  const height = options.height;
  const label = String(options.label || "").toUpperCase();
  const value = String(options.value || "0");

  fillRoundedRect(ctx, x, y, width, height, 18, "rgba(15, 23, 42, 0.72)");

  ctx.fillStyle = "#93c5fd";
  ctx.font = "600 16px sans-serif";
  ctx.fillText(label, x + 18, y + 30);

  ctx.fillStyle = "#f8fafc";
  ctx.font = "700 28px sans-serif";
  ctx.fillText(value, x + 18, y + 66);
}

async function drawAvatar(ctx, avatarUrl, displayName) {
  const x = 44;
  const y = (HEIGHT - AVATAR_SIZE) / 2;
  const radius = AVATAR_SIZE / 2;

  fillRoundedRect(ctx, x - 6, y - 6, AVATAR_SIZE + 12, AVATAR_SIZE + 12, radius + 10, "rgba(148, 163, 184, 0.18)");

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
    const fallbackGradient = ctx.createLinearGradient(x, y, x + AVATAR_SIZE, y + AVATAR_SIZE);
    fallbackGradient.addColorStop(0, "#334155");
    fallbackGradient.addColorStop(1, "#0f172a");
    ctx.fillStyle = fallbackGradient;
    ctx.fillRect(x, y, AVATAR_SIZE, AVATAR_SIZE);

    ctx.fillStyle = "#e2e8f0";
    ctx.font = "700 58px sans-serif";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillText(initialsFromName(displayName), x + radius, y + radius + 4);
    ctx.textAlign = "left";
    ctx.textBaseline = "alphabetic";
  }

  ctx.restore();
}

export async function renderLevelCardImage(options) {
  const displayName = String(options.displayName || "Unknown User").trim() || "Unknown User";
  const avatarUrl = options.avatarUrl ? String(options.avatarUrl) : null;
  const primaryColor = normalizeHexColor(options.primaryColor, "#66f2c4");
  const accentColor = normalizeHexColor(options.accentColor, "#6da8ff");
  const overlayOpacity = normalizeOpacity(options.overlayOpacity, 0.38);
  const fontFamily = fontFamilyFromStyle(options.fontStyle);
  const backgroundUrl = options.backgroundUrl ? String(options.backgroundUrl).trim() : "";

  const level = Math.max(0, Math.trunc(Number(options.level || 0)));
  const rank = Math.max(1, Math.trunc(Number(options.rank || 1)));
  const trackedMembers = Math.max(rank, Math.trunc(Number(options.trackedMembers || rank)));
  const progressXp = Math.max(0, Number(options.progressXp || 0));
  const progressRequired = Math.max(1, Number(options.progressRequired || 1));
  const progressPercent = Math.round(clamp((progressXp / progressRequired) * 100, 0, 100));
  const totalXp = Math.max(0, Number(options.totalXp || 0));
  const messageCount = Math.max(0, Number(options.messageCount || 0));
  const xpToNext = Math.max(0, progressRequired - progressXp);

  const canvas = createCanvas(WIDTH, HEIGHT);
  const ctx = canvas.getContext("2d");

  let customBackground = null;
  if (backgroundUrl) {
    try {
      customBackground = await loadImage(backgroundUrl);
    } catch {
      customBackground = null;
    }
  }

  if (customBackground) {
    drawImageCover(ctx, customBackground, WIDTH, HEIGHT);
    ctx.fillStyle = `rgba(6, 10, 20, ${overlayOpacity})`;
    ctx.fillRect(0, 0, WIDTH, HEIGHT);
  } else {
    const backgroundGradient = ctx.createLinearGradient(0, 0, WIDTH, HEIGHT);
    backgroundGradient.addColorStop(0, "#0b1220");
    backgroundGradient.addColorStop(0.55, "#111f36");
    backgroundGradient.addColorStop(1, "#122a47");
    ctx.fillStyle = backgroundGradient;
    ctx.fillRect(0, 0, WIDTH, HEIGHT);
  }

  const glowGradient = ctx.createRadialGradient(760, 40, 20, 760, 40, 320);
  glowGradient.addColorStop(0, colorWithAlpha(accentColor, "88"));
  glowGradient.addColorStop(1, colorWithAlpha(accentColor, "00"));
  ctx.fillStyle = glowGradient;
  ctx.fillRect(0, 0, WIDTH, HEIGHT);

  fillRoundedRect(ctx, 24, 24, WIDTH - 48, HEIGHT - 48, 28, "rgba(255, 255, 255, 0.06)");

  await drawAvatar(ctx, avatarUrl, displayName);

  ctx.fillStyle = "#f8fafc";
  ctx.font = `700 42px ${fontFamily}`;
  ctx.fillText(displayName.slice(0, 28), 240, 96);

  ctx.fillStyle = accentColor;
  ctx.font = `600 20px ${fontFamily}`;
  ctx.fillText(`Rank #${rank}/${trackedMembers} | Level ${level}`, 240, 132);

  const progressTrackX = 240;
  const progressTrackY = 168;
  const progressTrackWidth = 500;
  const progressTrackHeight = 34;
  const progressRatio = clamp(progressXp / progressRequired, 0, 1);
  const filledWidth = Math.max(10, Math.round(progressTrackWidth * progressRatio));

  fillRoundedRect(ctx, progressTrackX, progressTrackY, progressTrackWidth, progressTrackHeight, 16, "rgba(148, 163, 184, 0.22)");
  const progressGradient = ctx.createLinearGradient(progressTrackX, 0, progressTrackX + progressTrackWidth, 0);
  progressGradient.addColorStop(0, accentColor);
  progressGradient.addColorStop(1, primaryColor);
  fillRoundedRect(ctx, progressTrackX, progressTrackY, filledWidth, progressTrackHeight, 16, progressGradient);

  ctx.fillStyle = "#e2e8f0";
  ctx.font = `600 17px ${fontFamily}`;
  ctx.fillText(`${progressPercent}% progress`, 240, 228);

  ctx.fillStyle = "#94a3b8";
  ctx.font = `500 16px ${fontFamily}`;
  ctx.fillText(
    `XP ${formatInteger(progressXp)}/${formatInteger(progressRequired)} (${formatInteger(xpToNext)} to next level)`,
    240,
    256
  );

  drawBadge(ctx, {
    label: "Total XP",
    value: formatInteger(totalXp),
    x: 768,
    y: 66,
    width: 180,
    height: 86
  });

  drawBadge(ctx, {
    label: "Messages",
    value: formatInteger(messageCount),
    x: 768,
    y: 166,
    width: 180,
    height: 86
  });

  return canvas.toBuffer("image/png");
}
