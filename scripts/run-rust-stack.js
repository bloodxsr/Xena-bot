import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");

const npmExecutable = process.platform === "win32" ? "npm.cmd" : "npm";
const interruptExitCodes = new Set([130, 3221225786, -1073741510]);
const SIDECAR_HEALTH_TIMEOUT_MS = 2000;

function resolveSidecarServiceUrl() {
  const configured = String(process.env.RAID_ML_SERVICE_URL || "").trim();
  if (configured) {
    return configured.replace(/\/+$/, "");
  }

  const host = String(process.env.RAID_ML_HOST || "127.0.0.1").trim() || "127.0.0.1";
  const port = String(process.env.RAID_ML_PORT || "8787").trim() || "8787";
  return `http://${host}:${port}`;
}

async function isSidecarHealthy(serviceUrl) {
  const controller = new AbortController();
  const timeout = setTimeout(() => {
    controller.abort();
  }, SIDECAR_HEALTH_TIMEOUT_MS);

  if (typeof timeout.unref === "function") {
    timeout.unref();
  }

  try {
    const response = await fetch(`${serviceUrl}/health`, {
      method: "GET",
      signal: controller.signal
    });

    if (!response.ok) {
      return false;
    }

    const body = await response.text();
    return /"ok"\s*:\s*true/i.test(body);
  } catch {
    return false;
  } finally {
    clearTimeout(timeout);
  }
}

function normalizeExitCode(code, signal) {
  if (signal === "SIGINT" || signal === "SIGTERM") {
    return 0;
  }

  if (code == null) {
    return 1;
  }

  return interruptExitCodes.has(code) ? 0 : code;
}

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: projectRoot,
      stdio: "inherit",
      ...options
    });

    child.on("error", (error) => {
      reject(error);
    });

    child.on("close", (code, signal) => {
      resolve({ code: normalizeExitCode(code, signal), signal });
    });
  });
}

function stopChild(child, signal = "SIGINT") {
  if (!child || child.killed) {
    return;
  }

  try {
    child.kill(signal);
  } catch {
    // Ignore child shutdown errors.
  }
}

async function start() {
  const buildResult =
    process.platform === "win32"
      ? await runCommand("npm run ml:sidecar:build", [], { shell: true })
      : await runCommand(npmExecutable, ["run", "ml:sidecar:build"]);

  if (buildResult.code !== 0) {
    process.exit(buildResult.code || 1);
    return;
  }

  const sidecarServiceUrl = resolveSidecarServiceUrl();
  const reuseExistingSidecar = await isSidecarHealthy(sidecarServiceUrl);
  if (reuseExistingSidecar) {
    console.log(`rust stack: reusing running raid sidecar at ${sidecarServiceUrl}`);
  }

  const sidecar = reuseExistingSidecar
    ? null
    : spawn(process.execPath, [path.resolve(projectRoot, "scripts", "run-ml-sidecar.js")], {
        cwd: projectRoot,
        stdio: "inherit"
      });

  const bot = spawn(process.execPath, [path.resolve(projectRoot, "src", "start-rust.js")], {
    cwd: projectRoot,
    stdio: "inherit"
  });

  const state = {
    shuttingDown: false,
    exitCode: 0,
    closed: {
      sidecar: sidecar == null,
      bot: false
    }
  };

  function finalizeIfDone() {
    if (state.closed.sidecar && state.closed.bot) {
      process.exit(state.exitCode);
    }
  }

  function requestShutdown(exitCode = 0) {
    if (!state.shuttingDown) {
      state.shuttingDown = true;
      if (state.exitCode === 0) {
        state.exitCode = exitCode;
      }

      stopChild(bot, "SIGINT");
      if (sidecar) {
        stopChild(sidecar, "SIGINT");
      }
    } else if (state.exitCode === 0) {
      state.exitCode = exitCode;
    }

    finalizeIfDone();
  }

  process.on("SIGINT", () => {
    requestShutdown(0);
  });

  process.on("SIGTERM", () => {
    requestShutdown(0);
  });

  if (sidecar) {
    sidecar.on("error", () => {
      requestShutdown(1);
    });
  }

  bot.on("error", () => {
    requestShutdown(1);
  });

  if (sidecar) {
    sidecar.on("close", (code, signal) => {
      state.closed.sidecar = true;
      const normalized = normalizeExitCode(code, signal);

      if (!state.shuttingDown) {
        requestShutdown(normalized);
        return;
      }

      if (normalized !== 0 && state.exitCode === 0) {
        state.exitCode = normalized;
      }

      finalizeIfDone();
    });
  }

  bot.on("close", (code, signal) => {
    state.closed.bot = true;
    const normalized = normalizeExitCode(code, signal);

    if (!state.shuttingDown) {
      requestShutdown(normalized);
      return;
    }

    if (normalized !== 0 && state.exitCode === 0) {
      state.exitCode = normalized;
    }

    finalizeIfDone();
  });
}

start().catch((error) => {
  console.error(`failed to start rust stack: ${String(error)}`);
  process.exit(1);
});
