import { createCanvas, loadImage } from "@napi-rs/canvas";

const WIDTH = 1040;
const HEIGHT = 380;
const ICON_SIZE = 136;

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

function initialsFromName(guildName) {
  const words = String(guildName || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2);

  if (words.length === 0) {
    return "SV";
  }

  return words.map((part) => part.charAt(0).toUpperCase()).join("");
}

function drawMetricCard(ctx, options) {
  const x = options.x;
  const y = options.y;
  const width = options.width;
  const height = options.height;
  const label = String(options.label || "").toUpperCase();
  const value = String(options.value || "0");

  fillRoundedRect(ctx, x, y, width, height, 16, "rgba(15, 23, 42, 0.74)");

  ctx.fillStyle = "#93c5fd";
  ctx.font = "600 16px sans-serif";
  ctx.fillText(label, x + 14, y + 28);

  ctx.fillStyle = "#f8fafc";
  ctx.font = "700 30px sans-serif";
  ctx.fillText(value, x + 14, y + 66);
}

async function drawGuildIcon(ctx, iconUrl, guildName) {
  const x = 42;
  const y = 46;
  const radius = ICON_SIZE / 2;

  fillRoundedRect(ctx, x - 6, y - 6, ICON_SIZE + 12, ICON_SIZE + 12, radius + 10, "rgba(148, 163, 184, 0.17)");

  let image = null;
  if (iconUrl) {
    try {
      image = await loadImage(String(iconUrl));
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
    ctx.drawImage(image, x, y, ICON_SIZE, ICON_SIZE);
  } else {
    const fallbackGradient = ctx.createLinearGradient(x, y, x + ICON_SIZE, y + ICON_SIZE);
    fallbackGradient.addColorStop(0, "#334155");
    fallbackGradient.addColorStop(1, "#0f172a");
    ctx.fillStyle = fallbackGradient;
    ctx.fillRect(x, y, ICON_SIZE, ICON_SIZE);

    ctx.fillStyle = "#e2e8f0";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.font = "700 46px sans-serif";
    ctx.fillText(initialsFromName(guildName), x + radius, y + radius + 2);
    ctx.textAlign = "left";
    ctx.textBaseline = "alphabetic";
  }

  ctx.restore();
}

export async function renderServerStatsCardImage(options) {
  const guildName = String(options.guildName || "Unknown Server").trim() || "Unknown Server";
  const guildId = String(options.guildId || "-");
  const ownerId = String(options.ownerId || "").trim();
  const ownerText = ownerId ? ownerId : "unknown";
  const createdAtText = String(options.createdAtText || "unknown");
  const memberCount = Math.max(0, Number(options.memberCount || 0));
  const channelCount = Math.max(0, Number(options.channelCount || 0));
  const roleCount = Math.max(0, Number(options.roleCount || 0));
  const emojiCount = Math.max(0, Number(options.emojiCount || 0));
  const trackedMembers = Math.max(0, Number(options.trackedMembers || 0));
  const topLevelText = String(options.topLevelText || "No leveling data yet.");
  const iconUrl = options.iconUrl ? String(options.iconUrl) : null;

  const canvas = createCanvas(WIDTH, HEIGHT);
  const ctx = canvas.getContext("2d");

  const backgroundGradient = ctx.createLinearGradient(0, 0, WIDTH, HEIGHT);
  backgroundGradient.addColorStop(0, "#0f172a");
  backgroundGradient.addColorStop(0.5, "#1e293b");
  backgroundGradient.addColorStop(1, "#111827");
  ctx.fillStyle = backgroundGradient;
  ctx.fillRect(0, 0, WIDTH, HEIGHT);

  const glow = ctx.createRadialGradient(900, 30, 20, 900, 30, 320);
  glow.addColorStop(0, "rgba(14, 165, 233, 0.55)");
  glow.addColorStop(1, "rgba(14, 165, 233, 0)");
  ctx.fillStyle = glow;
  ctx.fillRect(0, 0, WIDTH, HEIGHT);

  fillRoundedRect(ctx, 24, 24, WIDTH - 48, HEIGHT - 48, 24, "rgba(255, 255, 255, 0.06)");

  await drawGuildIcon(ctx, iconUrl, guildName);

  ctx.fillStyle = "#f8fafc";
  ctx.font = "700 42px sans-serif";
  ctx.fillText(guildName.slice(0, 30), 212, 98);

  ctx.fillStyle = "#93c5fd";
  ctx.font = "600 18px sans-serif";
  ctx.fillText(`Server ID: ${guildId}`, 212, 128);

  ctx.fillStyle = "#cbd5e1";
  ctx.font = "500 18px sans-serif";
  ctx.fillText(`Owner: ${ownerText}`, 212, 158);
  ctx.fillText(`Created: ${createdAtText}`, 212, 184);

  const cardY = 220;
  drawMetricCard(ctx, {
    label: "Members",
    value: formatInteger(memberCount),
    x: 42,
    y: cardY,
    width: 180,
    height: 92
  });

  drawMetricCard(ctx, {
    label: "Channels",
    value: formatInteger(channelCount),
    x: 238,
    y: cardY,
    width: 180,
    height: 92
  });

  drawMetricCard(ctx, {
    label: "Roles",
    value: formatInteger(roleCount),
    x: 434,
    y: cardY,
    width: 180,
    height: 92
  });

  drawMetricCard(ctx, {
    label: "Leveling",
    value: formatInteger(trackedMembers),
    x: 630,
    y: cardY,
    width: 180,
    height: 92
  });

  fillRoundedRect(ctx, 826, cardY, 172, 92, 16, "rgba(15, 23, 42, 0.74)");
  ctx.fillStyle = "#93c5fd";
  ctx.font = "600 16px sans-serif";
  ctx.fillText("EMOJIS", 840, cardY + 28);
  ctx.fillStyle = "#f8fafc";
  ctx.font = "700 30px sans-serif";
  ctx.fillText(formatInteger(emojiCount), 840, cardY + 66);

  fillRoundedRect(ctx, 42, 314, WIDTH - 84, 42, 12, "rgba(30, 41, 59, 0.75)");
  ctx.fillStyle = "#cbd5e1";
  ctx.font = "500 16px sans-serif";
  ctx.fillText(`Top Level: ${topLevelText}`.slice(0, 120), 56, 342);

  return canvas.toBuffer("image/png");
}
