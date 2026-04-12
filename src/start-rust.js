process.env.RAID_ML_BACKEND = process.env.RAID_ML_BACKEND || "rust";
process.env.RAID_ML_SERVICE_URL = process.env.RAID_ML_SERVICE_URL || "http://127.0.0.1:8787";

await import("./index.js");
