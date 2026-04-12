function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function normalizeBaseUrl(rawValue) {
  const text = String(rawValue || "").trim();
  if (!text) {
    return "";
  }

  return text.replace(/\/+$/, "");
}

export class RaidMlClient {
  constructor(options = {}, logger = null) {
    this.backend = String(options.backend || "js").trim().toLowerCase();
    this.baseUrl = normalizeBaseUrl(options.serviceUrl);
    this.timeoutMs = clamp(Math.trunc(Number(options.timeoutMs || 350)), 50, 10000);
    this.maxConsecutiveFailures = clamp(Math.trunc(Number(options.maxConsecutiveFailures || 4)), 1, 50);
    this.circuitResetMs = clamp(Math.trunc(Number(options.circuitResetMs || 15000)), 1000, 120000);

    this.logger = typeof logger === "function" ? logger : null;
    this.consecutiveFailures = 0;
    this.circuitOpenUntil = 0;
    this.lastLogAt = 0;
  }

  get enabled() {
    return this.backend === "rust" && Boolean(this.baseUrl);
  }

  async checkHealth() {
    const response = await this.request("GET", "/health", null, { suppressLog: true });
    return Boolean(response?.ok);
  }

  async evaluateJoin(payload) {
    return this.request("POST", "/v1/raid/join", payload);
  }

  async recordSuspiciousActivity(payload) {
    return this.request("POST", "/v1/raid/suspicious", payload);
  }

  async request(method, routePath, payload = null, options = {}) {
    if (!this.enabled) {
      return null;
    }

    if (Date.now() < this.circuitOpenUntil) {
      return null;
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await fetch(`${this.baseUrl}${routePath}`, {
        method,
        headers: {
          "Content-Type": "application/json"
        },
        body: payload == null ? undefined : JSON.stringify(payload),
        signal: controller.signal
      });

      if (!response.ok) {
        throw new Error(`status=${response.status}`);
      }

      const data = await response.json();
      this.consecutiveFailures = 0;
      this.circuitOpenUntil = 0;
      return data && typeof data === "object" ? data : null;
    } catch (error) {
      this.consecutiveFailures += 1;
      if (this.consecutiveFailures >= this.maxConsecutiveFailures) {
        this.circuitOpenUntil = Date.now() + this.circuitResetMs;
      }

      const now = Date.now();
      if (!options.suppressLog && this.logger && now - this.lastLogAt >= 15000) {
        this.lastLogAt = now;
        this.logger("raid ml sidecar request failed", error);
      }

      return null;
    } finally {
      clearTimeout(timeout);
    }
  }
}
