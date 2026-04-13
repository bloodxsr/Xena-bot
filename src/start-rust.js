const raidMlHost = String(process.env.RAID_ML_HOST || "127.0.0.1").trim() || "127.0.0.1";
const raidMlPort = String(process.env.RAID_ML_PORT || "8787").trim() || "8787";

process.env.RAID_ML_BACKEND = process.env.RAID_ML_BACKEND || "rust";
process.env.RAID_ML_SERVICE_URL = process.env.RAID_ML_SERVICE_URL || `http://${raidMlHost}:${raidMlPort}`;

await import("./index.js");
