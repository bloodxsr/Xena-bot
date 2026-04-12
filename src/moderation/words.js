import fs from "node:fs";
import path from "node:path";

function escapeRegExp(text) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function normalizeWord(word) {
  return String(word ?? "").trim().toLowerCase();
}

function ensureWordsFile(jsonPath) {
  const dirPath = path.dirname(jsonPath);
  fs.mkdirSync(dirPath, { recursive: true });

  if (!fs.existsSync(jsonPath)) {
    fs.writeFileSync(jsonPath, "[]\n", "utf-8");
  }
}

export class WordStore {
  constructor(jsonPath) {
    this.jsonPath = jsonPath;
    this.words = [];
    this.patterns = [];
  }

  load() {
    ensureWordsFile(this.jsonPath);

    try {
      const parsed = JSON.parse(fs.readFileSync(this.jsonPath, "utf-8"));
      if (Array.isArray(parsed)) {
        this.words = Array.from(new Set(parsed.map((entry) => normalizeWord(entry)).filter(Boolean))).sort();
      } else {
        this.words = [];
      }
    } catch {
      this.words = [];
    }

    this.rebuildPatterns();
  }

  save() {
    ensureWordsFile(this.jsonPath);
    fs.writeFileSync(this.jsonPath, `${JSON.stringify(this.words, null, 2)}\n`, "utf-8");
  }

  rebuildPatterns() {
    this.patterns = this.words.map((word) => ({
      word,
      regex: new RegExp(`\\b${escapeRegExp(word)}\\b`, "i")
    }));
  }

  list() {
    return [...this.words];
  }

  add(word) {
    const normalized = normalizeWord(word);
    if (!normalized) {
      throw new Error("word is required");
    }

    if (!this.words.includes(normalized)) {
      this.words.push(normalized);
      this.words.sort((a, b) => a.localeCompare(b));
      this.rebuildPatterns();
      this.save();
      return true;
    }

    return false;
  }

  remove(word) {
    const normalized = normalizeWord(word);
    const before = this.words.length;
    this.words = this.words.filter((entry) => entry !== normalized);

    if (this.words.length !== before) {
      this.rebuildPatterns();
      this.save();
      return true;
    }

    return false;
  }

  reload() {
    this.words = [];
    this.patterns = [];
    this.load();
  }

  findBlockedWord(content) {
    const text = String(content ?? "");
    for (const { word, regex } of this.patterns) {
      if (regex.test(text)) {
        return word;
      }
    }

    return null;
  }
}
