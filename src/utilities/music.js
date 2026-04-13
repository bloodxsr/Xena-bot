import play from "play-dl";
import ytdl from "@distube/ytdl-core";
import ytdlp from "yt-dlp-exec";
import { getVoiceManager, LiveKitRtcConnection } from "@fluxerjs/voice";

const MAX_QUEUE_SIZE = 50;
const MAX_PLAYLIST_TRACKS = 25;
const MAX_QUERY_LENGTH = 300;
const TITLE_MAX_LENGTH = 120;
const IDLE_DISCONNECT_MS = 5 * 60 * 1000;
const PLAYBACK_POLL_MS = 500;
const PLAYBACK_STALL_MS = 2 * 60 * 60 * 1000;
const PLAYBACK_SETTLE_MS = 300;
const HIGH_QUALITY_STREAM_LEVEL = 2;
const YTDL_HIGH_WATER_MARK = 1 << 25;

const DEFAULT_PLAYDL_USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36";

const VALID_LIVEKIT_LOG_LEVELS = new Set(["fatal", "error", "warn", "info", "debug", "trace", "silent"]);

function normalizeLiveKitLogLevel(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (VALID_LIVEKIT_LOG_LEVELS.has(normalized)) {
    return normalized;
  }

  return "info";
}

function suppressLiveKitConnectedLog() {
  const prototype = LiveKitRtcConnection?.prototype;
  if (!prototype || typeof prototype.debug !== "function") {
    return;
  }

  if (prototype.__fluxerConnectedLogPatched) {
    return;
  }

  const originalDebug = prototype.debug;
  Object.defineProperty(prototype, "__fluxerConnectedLogPatched", {
    value: true,
    configurable: false,
    enumerable: false,
    writable: false
  });

  prototype.debug = function patchedLiveKitDebug(message, data) {
    const text = String(message ?? "").trim().toLowerCase();
    if (text === "connected to room") {
      return;
    }

    return originalDebug.call(this, message, data);
  };
}

async function configureLiveKitLoggerLevel() {
  try {
    const desiredLevel = normalizeLiveKitLogLevel(process.env.LIVEKIT_RTC_LOG_LEVEL);
    const logModuleUrl = new URL("../../node_modules/@livekit/rtc-node/dist/log.js", import.meta.url);
    const livekitModule = await import(logModuleUrl.href);
    if (livekitModule?.log) {
      livekitModule.log.level = desiredLevel;
    }
  } catch {
    // Ignore logger setup failures to avoid breaking music runtime startup.
  }
}

suppressLiveKitConnectedLog();
configureLiveKitLoggerLevel();

function truncateText(value, maxLength = TITLE_MAX_LENGTH) {
  const text = String(value ?? "").trim();
  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, Math.max(0, maxLength - 3)).trimEnd()}...`;
}

function sanitizeQueryInput(value) {
  const text = String(value ?? "").trim();
  if (text.startsWith("<") && text.endsWith(">")) {
    return text.slice(1, -1).trim();
  }

  return text;
}

function normalizeTrackUrl(value) {
  const text = String(value ?? "").trim();
  if (!text) {
    return "";
  }

  const lowered = text.toLowerCase();
  if (lowered === "undefined" || lowered === "null") {
    return "";
  }

  try {
    const parsed = new URL(text);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return "";
    }

    return parsed.toString();
  } catch {
    return "";
  }
}

function isHttpUrl(value) {
  return Boolean(normalizeTrackUrl(value));
}

function isYouTubeUrl(value) {
  const normalized = normalizeTrackUrl(value);
  if (!normalized) {
    return false;
  }

  try {
    const host = new URL(normalized).hostname.toLowerCase();
    return host === "youtu.be" || host === "youtube.com" || host.endsWith(".youtube.com");
  } catch {
    return false;
  }
}

function formatDuration(seconds) {
  const totalSeconds = Math.max(0, Math.floor(Number(seconds || 0)));
  if (totalSeconds <= 0) {
    return "live";
  }

  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const secs = totalSeconds % 60;

  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, "0")}:${String(secs).padStart(2, "0")}`;
  }

  return `${minutes}:${String(secs).padStart(2, "0")}`;
}

function normalizeValidationKind(value) {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

function buildTrackRecord({ title, streamUrl, displayUrl, durationSeconds, requestedByUserId }) {
  const normalizedDuration = Math.max(0, Math.floor(Number(durationSeconds || 0)));
  const normalizedStreamUrl = normalizeTrackUrl(streamUrl);
  const normalizedDisplayUrl = String(displayUrl || normalizedStreamUrl).trim();
  const fallbackTitle = normalizedDisplayUrl || normalizedStreamUrl || "Unknown track";

  return {
    id: `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`,
    title: truncateText(title || fallbackTitle),
    streamUrl: normalizedStreamUrl,
    displayUrl: normalizedDisplayUrl,
    durationSeconds: normalizedDuration,
    durationText: formatDuration(normalizedDuration),
    requestedByUserId: String(requestedByUserId || "").trim() || null,
    queuedAt: Date.now()
  };
}

function createSingleTrackResult({ title, streamUrl, displayUrl, durationSeconds, requestedByUserId, errorMessage }) {
  const track = buildTrackRecord({
    title,
    streamUrl,
    displayUrl,
    durationSeconds,
    requestedByUserId
  });

  if (!track.streamUrl) {
    throw new Error(errorMessage || "Could not resolve a playable track URL.");
  }

  return {
    sourceType: "single",
    playlistTitle: null,
    tracks: [track]
  };
}

function removeListenerSafe(emitter, eventName, listener) {
  if (!emitter || typeof listener !== "function") {
    return;
  }

  if (typeof emitter.removeListener === "function") {
    emitter.removeListener(eventName, listener);
    return;
  }

  if (typeof emitter.off === "function") {
    emitter.off(eventName, listener);
  }
}

function waitForPlaybackToFinish(connection) {
  return new Promise((resolve) => {
    if (!connection) {
      resolve();
      return;
    }

    let finished = false;
    let lastPlayingAt = Date.now();
    let sawPlaying = false;

    const finish = () => {
      if (finished) {
        return;
      }

      finished = true;
      cleanup();
      resolve();
    };

    const interval = setInterval(() => {
      if (!connection) {
        finish();
        return;
      }

      if (connection.playing === true) {
        sawPlaying = true;
        lastPlayingAt = Date.now();
        return;
      }

      const elapsedMs = Date.now() - lastPlayingAt;
      if (sawPlaying && elapsedMs >= PLAYBACK_SETTLE_MS) {
        finish();
      }
    }, PLAYBACK_POLL_MS);

    if (typeof interval.unref === "function") {
      interval.unref();
    }

    const timeout = setTimeout(() => {
      finish();
    }, PLAYBACK_STALL_MS);

    if (typeof timeout.unref === "function") {
      timeout.unref();
    }

    function cleanup() {
      clearInterval(interval);
      clearTimeout(timeout);
    }
  });
}

async function resolveTracksFromInput(input, requestedByUserId, options = {}) {
  const query = sanitizeQueryInput(input);
  if (!query) {
    throw new Error("Usage: play <url or search terms>");
  }

  if (query.length > MAX_QUERY_LENGTH) {
    throw new Error(`Query is too long. Max ${MAX_QUERY_LENGTH} characters.`);
  }

  const maxPlaylistTracks = Math.max(
    1,
    Math.min(Number(options.maxPlaylistTracks || MAX_PLAYLIST_TRACKS), MAX_QUEUE_SIZE)
  );

  if (isHttpUrl(query)) {
    const validationKind = normalizeValidationKind(await play.validate(query).catch(() => false));

    if (validationKind === "yt_video") {
      const info = await play.video_basic_info(query);
      const details = info?.video_details;
      return createSingleTrackResult({
        title: details?.title || query,
        streamUrl: details?.url || query,
        displayUrl: details?.url || query,
        durationSeconds: Number(details?.durationInSec || 0),
        requestedByUserId,
        errorMessage: "Could not resolve a playable YouTube URL for this video."
      });
    }

    if (validationKind === "so_track") {
      const details = await play.soundcloud(query);
      if (!details || details.type !== "track") {
        throw new Error("Could not load this SoundCloud track.");
      }

      return createSingleTrackResult({
        title: details.name || query,
        streamUrl: details.url || query,
        displayUrl: details.permalink || details.url || query,
        durationSeconds: Number(details.durationInSec || 0),
        requestedByUserId,
        errorMessage: "Could not resolve a playable SoundCloud URL for this track."
      });
    }

    if (validationKind === "yt_playlist") {
      const playlist = await play.playlist_info(query, { incomplete: true });

      if (typeof playlist.fetch === "function") {
        try {
          await playlist.fetch(maxPlaylistTracks);
        } catch {
          // Best effort.
        }
      }

      let videos = Array.isArray(playlist?.videos) ? playlist.videos : [];
      if (videos.length === 0 && typeof playlist.page === "function") {
        try {
          const firstPage = playlist.page(1);
          videos = Array.isArray(firstPage) ? firstPage : [];
        } catch {
          // Best effort.
        }
      }

      const tracks = videos
        .slice(0, maxPlaylistTracks)
        .map((video) =>
          buildTrackRecord({
            title: video?.title || query,
            streamUrl: video?.url || "",
            displayUrl: video?.url || query,
            durationSeconds: Number(video?.durationInSec || 0),
            requestedByUserId
          })
        )
        .filter((track) => Boolean(track.streamUrl));

      if (tracks.length === 0) {
        throw new Error("No playable tracks found in this YouTube playlist.");
      }

      return {
        sourceType: "playlist",
        playlistTitle: truncateText(playlist?.title || "YouTube Playlist"),
        tracks
      };
    }

    if (validationKind === "so_playlist") {
      const details = await play.soundcloud(query);
      if (!details || details.type !== "playlist") {
        throw new Error("Could not load this SoundCloud playlist.");
      }

      if (typeof details.fetch === "function") {
        try {
          await details.fetch();
        } catch {
          // Best effort.
        }
      }

      const rawTracks = Array.isArray(details.tracks) ? details.tracks : [];
      const tracks = rawTracks
        .slice(0, maxPlaylistTracks)
        .map((track) =>
          buildTrackRecord({
            title: track?.name || details.name || query,
            streamUrl: track?.url || track?.permalink || "",
            displayUrl: track?.permalink || track?.url || details.url || query,
            durationSeconds: Number(track?.durationInSec || 0),
            requestedByUserId
          })
        )
        .filter((track) => Boolean(track.streamUrl));

      if (tracks.length === 0) {
        throw new Error("No playable tracks found in this SoundCloud playlist.");
      }

      return {
        sourceType: "playlist",
        playlistTitle: truncateText(details.name || "SoundCloud Playlist"),
        tracks
      };
    }

    if (validationKind === "sp_playlist" || validationKind === "dz_playlist") {
      throw new Error("Spotify and Deezer playlists are not supported for direct playback. Use YouTube or SoundCloud playlists.");
    }

    if (validationKind.startsWith("sp_") || validationKind.startsWith("dz_")) {
      throw new Error("Spotify and Deezer links are not supported for direct playback. Use YouTube or SoundCloud.");
    }

    return createSingleTrackResult({
      title: query,
      streamUrl: query,
      displayUrl: query,
      durationSeconds: 0,
      requestedByUserId,
      errorMessage: "URL is not a playable YouTube or SoundCloud track."
    });
  }

  const results = await play.search(query, {
    limit: 1,
    source: { youtube: "video" }
  });

  const first = Array.isArray(results) ? results[0] : null;
  if (!first) {
    throw new Error("No results found.");
  }

  return createSingleTrackResult({
    title: first.title || query,
    streamUrl: first.url || "",
    displayUrl: first.url || "",
    durationSeconds: Number(first.durationInSec || 0),
    requestedByUserId,
    errorMessage: "Search returned a result without a playable URL."
  });
}

function listTrackUrlCandidates(track) {
  const candidates = [normalizeTrackUrl(track?.streamUrl), normalizeTrackUrl(track?.displayUrl)].filter(Boolean);
  return Array.from(new Set(candidates));
}

async function initializePlayDlTokenConfig(logError = () => {}) {
  const tokenOptions = {};

  const youtubeCookie = String(process.env.PLAYDL_YOUTUBE_COOKIE || process.env.YOUTUBE_COOKIE || "").trim();
  if (youtubeCookie) {
    tokenOptions.youtube = {
      cookie: youtubeCookie
    };
  }

  const userAgent = String(process.env.PLAYDL_USERAGENT || "").trim() || DEFAULT_PLAYDL_USER_AGENT;
  tokenOptions.useragent = [userAgent];

  try {
    await play.setToken(tokenOptions);
  } catch (error) {
    logError("music play-dl token initialization failed", error);
  }
}

function pickYouTubeWebmOpusUrl(formats) {
  const candidates = Array.isArray(formats)
    ? formats.filter((format) => {
        const url = String(format?.url || "").trim();
        if (!url) {
          return false;
        }

        const mimeType = String(format?.mimeType || "").toLowerCase();
        const codecs = String(format?.codecs || "").toLowerCase();
        const audioCodec = String(format?.audioCodec || "").toLowerCase();
        const container = String(format?.container || "").toLowerCase();

        const hasWebm = mimeType.includes("audio/webm") || container === "webm";
        const hasOpus = codecs.includes("opus") || audioCodec.includes("opus") || mimeType.includes("opus");
        return hasWebm && hasOpus;
      })
    : [];

  candidates.sort((a, b) => Number(b?.audioBitrate || b?.bitrate || 0) - Number(a?.audioBitrate || a?.bitrate || 0));
  return String(candidates[0]?.url || "").trim() || "";
}

async function createYouTubeFallbackSource(url) {
  const info = await ytdl.getInfo(url);
  const webmOpusUrl = normalizeTrackUrl(pickYouTubeWebmOpusUrl(info?.formats));
  if (webmOpusUrl) {
    return {
      playInput: webmOpusUrl,
      sourceStream: null,
      strategy: "ytdl-webm-opus"
    };
  }

  const format = ytdl.chooseFormat(info.formats, {
    quality: "highestaudio",
    filter: "audioonly"
  });

  if (!format || typeof format.itag !== "number") {
    throw new Error("No playable YouTube audio format found.");
  }

  const stream = ytdl.downloadFromInfo(info, {
    quality: format.itag,
    filter: "audioonly",
    highWaterMark: YTDL_HIGH_WATER_MARK,
    dlChunkSize: 0
  });

  return {
    playInput: stream,
    sourceStream: stream,
    strategy: "ytdl-stream"
  };
}

async function resolveYouTubeUrlViaYtDlp(url) {
  const output = await ytdlp(url, {
    getUrl: true,
    format: "bestaudio[acodec=opus][ext=webm]/bestaudio[acodec=opus]/bestaudio",
    noCheckCertificates: true,
    noWarnings: true,
    preferFreeFormats: true,
    forceIpv4: true
  });

  const firstLine = String(output || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find(Boolean);

  const resolvedUrl = normalizeTrackUrl(firstLine || "");
  if (!resolvedUrl) {
    throw new Error("yt-dlp did not return a valid playback URL.");
  }

  return resolvedUrl;
}

function isLiveKitConnection(connection) {
  return Boolean(connection && typeof connection.isConnected === "function");
}

async function createPlaybackSource(track, options = {}) {
  const preferUrlOnly = options.preferUrlOnly === true;
  const candidates = listTrackUrlCandidates(track);
  if (candidates.length === 0) {
    throw new Error("Track has no valid playback URL.");
  }

  const strategies = [
    {
      label: "hq-direct",
      options: {
        quality: HIGH_QUALITY_STREAM_LEVEL,
        discordPlayerCompatibility: false
      }
    },
    {
      label: "default-direct",
      options: {
        discordPlayerCompatibility: false
      }
    },
    {
      label: "compat-mode",
      options: {
        discordPlayerCompatibility: true
      }
    }
  ];

  let lastError = null;

  for (const url of candidates) {
    if (isYouTubeUrl(url)) {
      try {
        const ytSource = await createYouTubeFallbackSource(url);
        if (ytSource?.playInput) {
          return {
            playInput: ytSource.playInput,
            sourceStream: ytSource.sourceStream || null,
            streamUrl: url,
            strategy: ytSource.strategy || "ytdl-core"
          };
        }
      } catch (error) {
        lastError = error;
      }

      try {
        const ytDlpUrl = await resolveYouTubeUrlViaYtDlp(url);
        if (ytDlpUrl) {
          return {
            playInput: ytDlpUrl,
            sourceStream: null,
            streamUrl: ytDlpUrl,
            strategy: "yt-dlp-url"
          };
        }
      } catch (error) {
        lastError = error;
      }
    }

    for (const strategy of strategies) {
      if (preferUrlOnly) {
        continue;
      }

      try {
        const source = await play.stream(url, strategy.options);
        if (source?.stream) {
          return {
            playInput: source.stream,
            sourceStream: source.stream,
            streamUrl: url,
            strategy: strategy.label
          };
        }
      } catch (error) {
        lastError = error;
      }
    }
  }

  throw lastError || new Error(preferUrlOnly ? "Failed to create URL-compatible playback source for LiveKit." : "Failed to create playback stream.");
}

export function createMusicRuntime({ client, logError = () => {} } = {}) {
  if (!client) {
    throw new Error("createMusicRuntime requires a client");
  }

  const playDlTokenReady = initializePlayDlTokenConfig(logError);

  const voiceManager = getVoiceManager(client);
  const guildStates = new Map();

  function getState(guildId) {
    return guildStates.get(String(guildId || "")) || null;
  }

  function clearIdleTimer(state) {
    if (!state?.idleTimer) {
      return;
    }

    clearTimeout(state.idleTimer);
    state.idleTimer = null;
  }

  function unbindConnection(state) {
    if (!state?.boundConnection) {
      return;
    }

    removeListenerSafe(state.boundConnection, "error", state.connectionErrorHandler);
    removeListenerSafe(state.boundConnection, "disconnect", state.connectionDisconnectHandler);
    state.boundConnection = null;
    state.connectionErrorHandler = null;
    state.connectionDisconnectHandler = null;
  }

  function destroyGuildState(guildId, { leaveVoice = true } = {}) {
    const normalizedGuildId = String(guildId || "").trim();
    const state = guildStates.get(normalizedGuildId);
    if (!state) {
      return false;
    }

    guildStates.delete(normalizedGuildId);
    clearIdleTimer(state);
    unbindConnection(state);

    state.queue = [];
    state.currentTrack = null;
    state.pausedTrack = null;
    state.paused = false;
    state.activeSourceStream = null;

    if (state.connection && typeof state.connection.stop === "function") {
      try {
        state.connection.stop();
      } catch (error) {
        logError("music stop during cleanup failed", error);
      }
    }

    state.connection = null;

    if (leaveVoice) {
      try {
        voiceManager.leave(normalizedGuildId);
      } catch (error) {
        logError("music leave failed", error);
      }
    }

    return true;
  }

  function scheduleIdleLeave(state) {
    clearIdleTimer(state);

    if (!state || state.currentTrack || state.pausedTrack || state.paused || state.queue.length > 0) {
      return;
    }

    const timer = setTimeout(() => {
      destroyGuildState(state.guildId, { leaveVoice: true });
    }, IDLE_DISCONNECT_MS);

    if (typeof timer.unref === "function") {
      timer.unref();
    }

    state.idleTimer = timer;
  }

  function bindConnection(state, connection) {
    if (!state || !connection) {
      return;
    }

    if (state.boundConnection === connection) {
      state.connection = connection;
      return;
    }

    unbindConnection(state);

    const onError = (error) => {
      logError("music voice connection error", error);
    };

    const onDisconnect = () => {
      const activeState = guildStates.get(state.guildId);
      if (activeState !== state) {
        return;
      }

      destroyGuildState(state.guildId, { leaveVoice: false });
    };

    if (typeof connection.on === "function") {
      connection.on("error", onError);
      connection.on("disconnect", onDisconnect);
    }

    state.connection = connection;
    state.boundConnection = connection;
    state.connectionErrorHandler = onError;
    state.connectionDisconnectHandler = onDisconnect;
  }

  async function resolveUserVoiceChannel(guild, userId) {
    const guildId = String(guild?.id || "").trim();
    const normalizedUserId = String(userId || "").trim();
    if (!guildId || !normalizedUserId) {
      throw new Error("Guild and user are required for voice commands.");
    }

    const voiceChannelId = voiceManager.getVoiceChannelId(guildId, normalizedUserId);
    if (!voiceChannelId) {
      throw new Error("Join a voice channel first.");
    }

    let channel = guild?.channels?.get?.(voiceChannelId) || null;

    if (!channel || typeof channel.isVoice !== "function" || !channel.isVoice()) {
      try {
        channel = await client.channels.resolve(voiceChannelId);
      } catch {
        channel = null;
      }
    }

    if (!channel || typeof channel.isVoice !== "function" || !channel.isVoice()) {
      throw new Error("Could not resolve your voice channel.");
    }

    return channel;
  }

  async function ensureGuildConnection(guild, userId) {
    const guildId = String(guild?.id || "").trim();
    if (!guildId) {
      throw new Error("This command only works in a server.");
    }

    const voiceChannel = await resolveUserVoiceChannel(guild, userId);
    const voiceChannelId = String(voiceChannel?.id || "").trim();

    let state = guildStates.get(guildId);
    if (!state) {
      state = {
        guildId,
        channelId: null,
        connection: null,
        queue: [],
        currentTrack: null,
        pausedTrack: null,
        paused: false,
        activeSourceStream: null,
        isAdvancing: false,
        idleTimer: null,
        boundConnection: null,
        connectionErrorHandler: null,
        connectionDisconnectHandler: null
      };
      guildStates.set(guildId, state);
    }

    if (state.connection && state.channelId && state.channelId !== voiceChannelId) {
      if (state.currentTrack || state.queue.length > 0 || state.connection.playing === true) {
        throw new Error(`Music is already active in <#${state.channelId}>. Use leave first.`);
      }

      destroyGuildState(guildId, { leaveVoice: true });

      state = {
        guildId,
        channelId: null,
        connection: null,
        queue: [],
        currentTrack: null,
        pausedTrack: null,
        paused: false,
        activeSourceStream: null,
        isAdvancing: false,
        idleTimer: null,
        boundConnection: null,
        connectionErrorHandler: null,
        connectionDisconnectHandler: null
      };

      guildStates.set(guildId, state);
    }

    if (!state.connection || state.channelId !== voiceChannelId) {
      const connection = await voiceManager.join(voiceChannel);
      state.channelId = voiceChannelId;
      bindConnection(state, connection);
    } else {
      bindConnection(state, state.connection);
    }

    clearIdleTimer(state);
    return state;
  }

  async function advanceQueue(state) {
    if (!state || state.isAdvancing || state.paused || state.pausedTrack) {
      return;
    }

    state.isAdvancing = true;

    try {
      while (guildStates.get(state.guildId) === state) {
        if (state.queue.length === 0) {
          state.currentTrack = null;
          break;
        }

        if (!state.connection) {
          break;
        }

        const nextTrack = state.queue.shift();
        state.currentTrack = nextTrack;
        state.paused = false;

        try {
          const preferUrlOnly = isLiveKitConnection(state.connection);
          const playback = await createPlaybackSource(nextTrack, { preferUrlOnly });

          if (playback.streamUrl && playback.streamUrl !== nextTrack.streamUrl) {
            state.currentTrack = {
              ...nextTrack,
              streamUrl: playback.streamUrl
            };
          }

          state.activeSourceStream = playback.sourceStream || null;
          await state.connection.play(playback.playInput);
          await waitForPlaybackToFinish(state.connection);
        } catch (error) {
          logError("music playback failed", error);
        } finally {
          state.activeSourceStream = null;
          if (!state.pausedTrack) {
            state.paused = false;
          }
          state.currentTrack = null;
        }
      }
    } finally {
      state.isAdvancing = false;

      if (guildStates.get(state.guildId) === state) {
        scheduleIdleLeave(state);
      }
    }
  }

  return {
    async join({ guild, userId }) {
      const state = await ensureGuildConnection(guild, userId);
      return {
        guildId: state.guildId,
        channelId: state.channelId
      };
    },

    async enqueue({ guild, userId, query, requestedByUserId }) {
      await playDlTokenReady;

      const state = await ensureGuildConnection(guild, userId);

      const availableSlots = MAX_QUEUE_SIZE - state.queue.length;
      if (availableSlots <= 0) {
        throw new Error(`Queue is full. Max ${MAX_QUEUE_SIZE} tracks.`);
      }

      const resolved = await resolveTracksFromInput(query, requestedByUserId || userId, {
        maxPlaylistTracks: Math.min(MAX_PLAYLIST_TRACKS, availableSlots)
      });

      const tracks = Array.isArray(resolved.tracks) ? resolved.tracks.slice(0, availableSlots) : [];
      if (tracks.length === 0) {
        throw new Error("No playable tracks found.");
      }

      state.queue.push(...tracks);

      const startedNow = !state.currentTrack && !state.isAdvancing && !state.paused && !state.pausedTrack;
      if (startedNow) {
        void advanceQueue(state);
      }

      return {
        tracks,
        firstTrack: tracks[0],
        tracksQueued: tracks.length,
        sourceType: resolved.sourceType,
        playlistTitle: resolved.playlistTitle || null,
        startedNow,
        queueSize: state.queue.length,
        currentTrack: state.currentTrack,
        channelId: state.channelId
      };
    },

    skip(guildId) {
      const state = getState(guildId);
      if (!state || !state.connection) {
        return {
          skipped: false,
          track: null,
          queueLength: 0
        };
      }

      const track = state.currentTrack || state.pausedTrack || null;

      if (state.paused && state.pausedTrack) {
        state.paused = false;
        state.pausedTrack = null;
        state.activeSourceStream = null;

        if (!state.isAdvancing && state.queue.length > 0) {
          void advanceQueue(state);
        }

        return {
          skipped: Boolean(track),
          track,
          queueLength: state.queue.length
        };
      }

      if (state.currentTrack || state.connection.playing === true) {
        try {
          state.connection.stop();
          state.pausedTrack = null;
          state.paused = false;
          state.activeSourceStream = null;
        } catch (error) {
          logError("music skip failed", error);
        }
      } else if (!state.isAdvancing && state.queue.length > 0) {
        void advanceQueue(state);
      }

      return {
        skipped: Boolean(track),
        track,
        queueLength: state.queue.length
      };
    },

    stop(guildId) {
      const state = getState(guildId);
      if (!state || !state.connection) {
        return {
          stopped: false,
          hadTrack: false,
          cleared: 0
        };
      }

      const hadTrack = Boolean(state.currentTrack);
      const cleared = state.queue.length;

      state.queue = [];
      state.currentTrack = null;
      state.pausedTrack = null;
      state.paused = false;
      state.activeSourceStream = null;

      try {
        state.connection.stop();
      } catch (error) {
        logError("music stop failed", error);
      }

      scheduleIdleLeave(state);

      return {
        stopped: hadTrack || cleared > 0,
        hadTrack,
        cleared
      };
    },

    leave(guildId) {
      return destroyGuildState(guildId, { leaveVoice: true });
    },

    pause(guildId) {
      const state = getState(guildId);
      if (!state || !state.connection) {
        return {
          paused: false,
          alreadyPaused: false,
          track: null
        };
      }

      if (!state.currentTrack && state.pausedTrack) {
        return {
          paused: false,
          alreadyPaused: true,
          track: state.pausedTrack
        };
      }

      if (!state.currentTrack) {
        return {
          paused: false,
          alreadyPaused: false,
          track: null
        };
      }

      if (state.paused) {
        return {
          paused: false,
          alreadyPaused: true,
          track: state.currentTrack
        };
      }

      let pausedAny = false;
      const sourceStream = state.activeSourceStream;
      if (sourceStream && typeof sourceStream.pause === "function") {
        try {
          sourceStream.pause();
          pausedAny = true;
        } catch {
          // Best effort.
        }
      }

      const currentStream = state.connection.currentStream;
      if (currentStream && currentStream !== sourceStream && typeof currentStream.pause === "function") {
        try {
          currentStream.pause();
          pausedAny = true;
        } catch {
          // Best effort.
        }
      }

      if (!pausedAny) {
        // Fallback for sources that cannot be natively paused (common with LiveKit URL playback).
        const pausedTrack = state.currentTrack;
        state.pausedTrack = pausedTrack;
        state.currentTrack = null;
        state.activeSourceStream = null;
        state.paused = true;
        clearIdleTimer(state);

        try {
          state.connection.stop();
        } catch {
          // Best effort.
        }

        return {
          paused: true,
          alreadyPaused: false,
          track: pausedTrack
        };
      }

      state.paused = true;
      return {
        paused: true,
        alreadyPaused: false,
        track: state.currentTrack
      };
    },

    resume(guildId) {
      const state = getState(guildId);
      if (!state || !state.connection) {
        return {
          resumed: false,
          alreadyResumed: false,
          track: null
        };
      }

      if (state.pausedTrack) {
        const track = state.pausedTrack;
        state.pausedTrack = null;
        state.paused = false;
        state.queue.unshift(track);

        if (!state.isAdvancing) {
          void advanceQueue(state);
        }

        return {
          resumed: true,
          alreadyResumed: false,
          track
        };
      }

      if (!state.currentTrack) {
        return {
          resumed: false,
          alreadyResumed: false,
          track: null
        };
      }

      if (!state.paused) {
        return {
          resumed: false,
          alreadyResumed: true,
          track: state.currentTrack
        };
      }

      let resumedAny = false;
      const sourceStream = state.activeSourceStream;
      if (sourceStream && typeof sourceStream.resume === "function") {
        try {
          sourceStream.resume();
          resumedAny = true;
        } catch {
          // Best effort.
        }
      }

      const currentStream = state.connection.currentStream;
      if (currentStream && currentStream !== sourceStream && typeof currentStream.resume === "function") {
        try {
          currentStream.resume();
          resumedAny = true;
        } catch {
          // Best effort.
        }
      }

      if (!resumedAny) {
        return {
          resumed: false,
          alreadyResumed: false,
          track: state.currentTrack
        };
      }

      state.paused = false;
      return {
        resumed: true,
        alreadyResumed: false,
        track: state.currentTrack
      };
    },

    getNowPlaying(guildId) {
      const state = getState(guildId);
      if (!state) {
        return null;
      }

      const activeTrack = state.currentTrack || state.pausedTrack;
      if (!activeTrack) {
        return null;
      }

      return {
        ...activeTrack,
        channelId: state.channelId,
        paused: state.paused === true
      };
    },

    getQueueSnapshot(guildId) {
      const state = getState(guildId);
      if (!state) {
        return {
          channelId: null,
          currentTrack: null,
          paused: false,
          queue: []
        };
      }

      const activeTrack = state.currentTrack || (state.paused ? state.pausedTrack : null);

      return {
        channelId: state.channelId,
        paused: state.paused === true,
        currentTrack: activeTrack,
        queue: [...state.queue]
      };
    }
  };
}
