import { existsSync } from "node:fs";
import { spawn } from "node:child_process";
import path from "node:path";

const STATUS_CONTROL_C_EXIT_UNSIGNED = 0xc000013a;
const STATUS_CONTROL_C_EXIT_SIGNED = -1073741510;

const executableName = process.platform === "win32" ? "raid-ml-sidecar.exe" : "raid-ml-sidecar";
const executablePath = path.resolve(process.cwd(), "raid_ml_sidecar", "target", "release", executableName);

if (!existsSync(executablePath)) {
  console.error(`sidecar binary not found at ${executablePath}`);
  console.error("Run `npm run ml:sidecar:build` first.");
  process.exit(1);
}

const child = spawn(executablePath, {
  stdio: "inherit",
  windowsHide: false
});

let shutdownRequested = false;

function requestShutdown(signal = "SIGINT") {
  if (shutdownRequested) {
    return;
  }

  shutdownRequested = true;

  if (!child.killed) {
    try {
      child.kill(signal);
    } catch {
      // Ignore shutdown signal forwarding failures.
    }
  }
}

process.on("SIGINT", () => {
  requestShutdown("SIGINT");
});

process.on("SIGTERM", () => {
  requestShutdown("SIGTERM");
});

child.on("error", (error) => {
  console.error(`failed to start raid sidecar: ${String(error)}`);
  process.exit(1);
});

child.on("close", (code, signal) => {
  const interrupted =
    signal === "SIGINT" ||
    signal === "SIGTERM" ||
    code === 130 ||
    code === STATUS_CONTROL_C_EXIT_UNSIGNED ||
    code === STATUS_CONTROL_C_EXIT_SIGNED;

  if (shutdownRequested || interrupted) {
    process.exit(0);
  }

  process.exit(code ?? 1);
});
