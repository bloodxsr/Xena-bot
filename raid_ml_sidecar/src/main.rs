use axum::{extract::State, routing::{get, post}, Json, Router};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    env,
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

const FEATURE_COUNT: usize = 12;
const IDX_BURST: usize = 0;
const IDX_SHORT_BURST: usize = 1;
const IDX_ACCELERATION: usize = 2;
const IDX_NEW_ACCOUNT: usize = 3;
const IDX_COORDINATED: usize = 4;
const IDX_PROFILE_GAP: usize = 5;
const IDX_AVATAR: usize = 6;
const IDX_BURST_ANOMALY: usize = 7;
const IDX_SHORT_ANOMALY: usize = 8;
const IDX_YOUNG_ANOMALY: usize = 9;
const IDX_ACCOUNT_AGE_ANOMALY: usize = 10;
const IDX_COORDINATED_SPAM: usize = 11;

const FEATURE_LABELS: [&str; FEATURE_COUNT] = [
    "join burst above baseline",
    "sudden short-window spike",
    "acceleration in join velocity",
    "very new account",
    "cluster of young accounts",
    "low profile completeness",
    "missing avatar signal",
    "join-rate anomaly vs history",
    "short-window anomaly vs history",
    "young-account anomaly vs history",
    "account-age anomaly vs history",
    "coordinated spam telemetry",
];

const HEURISTIC_WEIGHTS: [f64; FEATURE_COUNT] = [
    1.75, 1.42, 0.95, 1.08, 0.96, 0.68, 0.28, 1.12, 0.94, 0.78, 0.64, 1.25,
];

const DEFAULT_MODEL_WEIGHTS: [f64; FEATURE_COUNT] = [
    1.52, 1.26, 0.88, 1.10, 0.92, 0.68, 0.24, 1.06, 0.94, 0.81, 0.72, 1.19,
];

#[derive(Clone, Copy)]
struct ServiceConfig {
    learning_rate: f64,
    weight_decay: f64,
    heuristic_blend: f64,
    warmup_events: f64,
    baseline_alpha: f64,
    max_weight_magnitude: f64,
}

impl ServiceConfig {
    fn from_env() -> Self {
        Self {
            learning_rate: clamp(parse_env_f64("RAID_ML_LEARNING_RATE", 0.018), 0.001, 0.2),
            weight_decay: clamp(parse_env_f64("RAID_ML_WEIGHT_DECAY", 0.0008), 0.0, 0.05),
            heuristic_blend: clamp(parse_env_f64("RAID_ML_HEURISTIC_BLEND", 0.66), 0.35, 0.9),
            warmup_events: clamp(parse_env_f64("RAID_ML_WARMUP_EVENTS", 40.0), 5.0, 500.0),
            baseline_alpha: clamp(parse_env_f64("RAID_ML_BASELINE_ALPHA", 0.08), 0.01, 0.4),
            max_weight_magnitude: clamp(parse_env_f64("RAID_ML_MAX_WEIGHT_MAGNITUDE", 5.0), 2.0, 12.0),
        }
    }
}

struct ServiceState {
    config: ServiceConfig,
    guilds: HashMap<String, GuildState>,
    suspicious: HashMap<String, Vec<SuspiciousEvent>>,
}

impl ServiceState {
    fn new(config: ServiceConfig) -> Self {
        Self {
            config,
            guilds: HashMap::new(),
            suspicious: HashMap::new(),
        }
    }
}

#[derive(Clone)]
struct GuildState {
    join_timestamps: Vec<f64>,
    young_flags: Vec<(f64, f64)>,
    baseline: Baseline,
    model: OnlineModel,
}

impl Default for GuildState {
    fn default() -> Self {
        Self {
            join_timestamps: Vec::new(),
            young_flags: Vec::new(),
            baseline: Baseline::default(),
            model: OnlineModel::default(),
        }
    }
}

#[derive(Clone)]
struct Baseline {
    join_rate_mean: f64,
    join_rate_var: f64,
    short_join_rate_mean: f64,
    short_join_rate_var: f64,
    young_ratio_mean: f64,
    young_ratio_var: f64,
    account_age_mean: f64,
    account_age_var: f64,
    sample_count: u64,
}

impl Default for Baseline {
    fn default() -> Self {
        Self {
            join_rate_mean: 0.8,
            join_rate_var: 0.35,
            short_join_rate_mean: 0.95,
            short_join_rate_var: 0.45,
            young_ratio_mean: 0.2,
            young_ratio_var: 0.1,
            account_age_mean: 20.0,
            account_age_var: 140.0,
            sample_count: 0,
        }
    }
}

#[derive(Clone)]
struct OnlineModel {
    bias: f64,
    weights: [f64; FEATURE_COUNT],
    updates: u64,
}

impl Default for OnlineModel {
    fn default() -> Self {
        Self {
            bias: -1.85,
            weights: DEFAULT_MODEL_WEIGHTS,
            updates: 0,
        }
    }
}

#[derive(Clone)]
struct SuspiciousEvent {
    ts: f64,
    user_id: String,
    score: f64,
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JoinRequest {
    guild_id: String,
    account_age_days: f64,
    has_avatar: bool,
    profile_score: f64,
    window_seconds: f64,
    join_rate_threshold: f64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SuspiciousRequest {
    guild_id: String,
    user_id: String,
    window_seconds: Option<f64>,
    score: Option<f64>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct SuspiciousResponse {
    event_count: usize,
    unique_users: usize,
    weighted_events: f64,
    event_rate_per_minute: f64,
    coordination_density: f64,
    suspicious_score: f64,
    window_seconds: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AnomalyResponse {
    join_rate_z: f64,
    short_join_rate_z: f64,
    young_ratio_z: f64,
    account_age_z: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ModelStateResponse {
    sample_count: u64,
    updates: u64,
    heuristic_weight: f64,
    model_weight: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JoinResponse {
    account_age_days: f64,
    has_avatar: bool,
    profile_score: f64,
    join_rate_per_minute: f64,
    short_join_rate_per_minute: f64,
    short_join_count: u64,
    acceleration_feature: f64,
    young_account_ratio: f64,
    risk_score: f64,
    risk_level: String,
    heuristic_score: f64,
    adaptive_score: f64,
    model_confidence: f64,
    anomaly: AnomalyResponse,
    suspicious_activity: SuspiciousResponse,
    model_state: ModelStateResponse,
    explanation: String,
}

type SharedState = Arc<Mutex<ServiceState>>;

#[tokio::main]
async fn main() {
    let host = env::var("RAID_ML_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = parse_env_u16("RAID_ML_PORT", 8787);
    let addr: SocketAddr = format!("{}:{}", host, port)
        .parse()
        .expect("invalid RAID_ML_HOST or RAID_ML_PORT");

    let state = Arc::new(Mutex::new(ServiceState::new(ServiceConfig::from_env())));

    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/raid/join", post(score_join))
        .route("/v1/raid/suspicious", post(record_suspicious))
        .with_state(state);

    println!("raid-ml-sidecar listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind listener");

    axum::serve(listener, app).await.expect("sidecar stopped unexpectedly");
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

async fn record_suspicious(
    State(state): State<SharedState>,
    Json(input): Json<SuspiciousRequest>,
) -> Json<SuspiciousResponse> {
    let now = now_seconds();
    let window_seconds = clamp(input.window_seconds.unwrap_or(45.0), 15.0, 300.0);
    let score = clamp(input.score.unwrap_or(1.0), 0.2, 4.0);

    let mut guard = state.lock().await;
    let events = guard.suspicious.entry(input.guild_id).or_default();
    events.push(SuspiciousEvent {
        ts: now,
        user_id: input.user_id,
        score,
    });

    let snapshot = build_suspicious_snapshot(events, now, window_seconds);
    Json(snapshot)
}

async fn score_join(
    State(state): State<SharedState>,
    Json(input): Json<JoinRequest>,
) -> Json<JoinResponse> {
    let now = now_seconds();
    let window_seconds = clamp(input.window_seconds, 30.0, 3600.0);
    let join_rate_threshold = input.join_rate_threshold.max(1.0);
    let account_age_days = input.account_age_days.max(0.0);
    let profile_score = clamp(input.profile_score, 0.0, 1.0);

    let mut guard = state.lock().await;
    let config = guard.config;

    let suspicious_snapshot = {
        let events = guard
            .suspicious
            .entry(input.guild_id.clone())
            .or_default();
        build_suspicious_snapshot(events, now, window_seconds)
    };

    let guild = guard.guilds.entry(input.guild_id).or_default();

    guild.join_timestamps.push(now);
    guild
        .join_timestamps
        .retain(|ts| now - *ts <= window_seconds);

    let is_young = if account_age_days <= 7.0 { 1.0 } else { 0.0 };
    guild.young_flags.push((now, is_young));
    guild
        .young_flags
        .retain(|(ts, _)| now - *ts <= window_seconds);

    let window_minutes = (window_seconds / 60.0).max(1.0);
    let short_window_seconds = clamp((window_seconds * 0.35).round(), 15.0, 45.0);
    let short_window_minutes = (short_window_seconds / 60.0).max(0.25);

    let short_join_count = guild
        .join_timestamps
        .iter()
        .filter(|ts| now - **ts <= short_window_seconds)
        .count() as u64;

    let expected_short_count = ((guild.join_timestamps.len() as f64)
        * safe_div(short_window_seconds, window_seconds, 0.35))
        .round()
        .max(1.0);

    let join_rate_per_minute = safe_div(guild.join_timestamps.len() as f64, window_minutes, 0.0);
    let short_join_rate_per_minute = safe_div(short_join_count as f64, short_window_minutes, 0.0);

    let young_account_ratio = if guild.young_flags.is_empty() {
        0.0
    } else {
        guild.young_flags.iter().map(|(_, v)| *v).sum::<f64>() / (guild.young_flags.len() as f64)
    };

    let join_rate_std = guild.baseline.join_rate_var.max(0.03).sqrt();
    let short_join_rate_std = guild.baseline.short_join_rate_var.max(0.04).sqrt();
    let young_ratio_std = guild.baseline.young_ratio_var.max(0.02).sqrt();
    let account_age_std = guild.baseline.account_age_var.max(12.0).sqrt();

    let join_rate_z = safe_div(join_rate_per_minute - guild.baseline.join_rate_mean, join_rate_std, 0.0);
    let short_join_rate_z =
        safe_div(short_join_rate_per_minute - guild.baseline.short_join_rate_mean, short_join_rate_std, 0.0);
    let young_ratio_z = safe_div(young_account_ratio - guild.baseline.young_ratio_mean, young_ratio_std, 0.0);
    let account_age_z = safe_div(account_age_days - guild.baseline.account_age_mean, account_age_std, 0.0);

    let mut features = [0.0; FEATURE_COUNT];
    features[IDX_BURST] = clamp(join_rate_per_minute / join_rate_threshold, 0.0, 1.8);
    features[IDX_SHORT_BURST] = clamp(short_join_rate_per_minute / (join_rate_threshold * 1.2), 0.0, 2.2);
    features[IDX_ACCELERATION] = clamp(
        safe_div((short_join_count as f64) - expected_short_count, expected_short_count, 0.0),
        0.0,
        2.1,
    );
    features[IDX_NEW_ACCOUNT] = clamp((14.0 - account_age_days) / 14.0, 0.0, 1.0);
    features[IDX_COORDINATED] = clamp(young_account_ratio, 0.0, 1.0);
    features[IDX_PROFILE_GAP] = 1.0 - profile_score;
    features[IDX_AVATAR] = if input.has_avatar { 0.0 } else { 1.0 };
    features[IDX_BURST_ANOMALY] = clamp((join_rate_z + 0.55) / 2.2, 0.0, 2.0);
    features[IDX_SHORT_ANOMALY] = clamp((short_join_rate_z + 0.5) / 2.1, 0.0, 2.0);
    features[IDX_YOUNG_ANOMALY] = clamp((young_ratio_z + 0.35) / 1.9, 0.0, 2.0);
    features[IDX_ACCOUNT_AGE_ANOMALY] = clamp(((-account_age_z) + 0.2) / 1.8, 0.0, 2.0);
    features[IDX_COORDINATED_SPAM] = clamp(suspicious_snapshot.suspicious_score / 1.15, 0.0, 2.0);

    let heuristic_logit = -2.15 + dot(&features, &HEURISTIC_WEIGHTS);
    let model_logit = guild.model.bias + dot(&features, &guild.model.weights);

    let warmup_progress = clamp(
        safe_div(guild.baseline.sample_count as f64, config.warmup_events, 0.0),
        0.0,
        1.0,
    );

    let heuristic_weight = clamp(config.heuristic_blend + (1.0 - warmup_progress) * 0.16, 0.45, 0.92);
    let model_weight = 1.0 - heuristic_weight;

    let heuristic_score = sigmoid(heuristic_logit);
    let adaptive_score = sigmoid(model_logit);
    let blended_logit = heuristic_weight * heuristic_logit + model_weight * model_logit;
    let agreement = 1.0 - clamp((heuristic_score - adaptive_score).abs() / 0.6, 0.0, 1.0);
    let model_confidence = clamp(0.35 + 0.45 * warmup_progress + 0.2 * agreement, 0.2, 0.98);
    let risk_score = clamp(sigmoid(blended_logit) * (0.85 + 0.15 * model_confidence), 0.0, 1.0);

    let risk_level = if risk_score >= 0.85 || (risk_score >= 0.78 && features[IDX_COORDINATED_SPAM] >= 1.0) {
        "high"
    } else if risk_score >= 0.62 || (risk_score >= 0.55 && features[IDX_SHORT_ANOMALY] >= 0.9) {
        "medium"
    } else {
        "low"
    }
    .to_string();

    let mut contributions: Vec<(usize, f64)> = (0..FEATURE_COUNT)
        .map(|idx| {
            let contribution = heuristic_weight * HEURISTIC_WEIGHTS[idx] * features[idx]
                + model_weight * guild.model.weights[idx] * features[idx];
            (idx, contribution)
        })
        .collect();

    contributions.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(Ordering::Equal)
    });

    let mut explanation_parts = Vec::new();
    let mut seen = HashSet::new();
    for (idx, contribution) in contributions {
        if contribution < 0.32 {
            continue;
        }

        let label = FEATURE_LABELS[idx];
        if seen.insert(label) {
            explanation_parts.push(label.to_string());
        }

        if explanation_parts.len() >= 4 {
            break;
        }
    }

    if explanation_parts.is_empty() {
        explanation_parts.push("signals within expected range".to_string());
    }

    if let Some(label) = weak_label(&features, account_age_days, profile_score) {
        update_model(&mut guild.model, &features, label, adaptive_score, config);
    }

    update_baseline(
        &mut guild.baseline,
        join_rate_per_minute,
        short_join_rate_per_minute,
        young_account_ratio,
        account_age_days,
        config.baseline_alpha,
    );

    Json(JoinResponse {
        account_age_days,
        has_avatar: input.has_avatar,
        profile_score,
        join_rate_per_minute,
        short_join_rate_per_minute,
        short_join_count,
        acceleration_feature: features[IDX_ACCELERATION],
        young_account_ratio,
        risk_score,
        risk_level,
        heuristic_score,
        adaptive_score,
        model_confidence,
        anomaly: AnomalyResponse {
            join_rate_z: to_fixed(join_rate_z, 4),
            short_join_rate_z: to_fixed(short_join_rate_z, 4),
            young_ratio_z: to_fixed(young_ratio_z, 4),
            account_age_z: to_fixed(account_age_z, 4),
        },
        suspicious_activity: suspicious_snapshot,
        model_state: ModelStateResponse {
            sample_count: guild.baseline.sample_count,
            updates: guild.model.updates,
            heuristic_weight: to_fixed(heuristic_weight, 4),
            model_weight: to_fixed(model_weight, 4),
        },
        explanation: explanation_parts.join(", "),
    })
}

fn build_suspicious_snapshot(events: &mut Vec<SuspiciousEvent>, now: f64, window_seconds: f64) -> SuspiciousResponse {
    events.retain(|event| now - event.ts <= window_seconds);

    let unique_users = events
        .iter()
        .map(|event| event.user_id.clone())
        .collect::<HashSet<_>>()
        .len();

    let weighted_events = events.iter().map(|event| event.score).sum::<f64>();
    let event_rate_per_minute = safe_div(weighted_events, (window_seconds / 60.0).max(0.25), 0.0);
    let coordination_density = safe_div(weighted_events, unique_users.max(1) as f64, 0.0);

    let suspicious_score =
        0.48 * clamp(weighted_events / (window_seconds / 12.0).max(4.0), 0.0, 2.0)
            + 0.34 * clamp(unique_users as f64 / 3.0, 0.0, 2.0)
            + 0.24 * clamp(coordination_density / 2.2, 0.0, 2.0);

    SuspiciousResponse {
        event_count: events.len(),
        unique_users,
        weighted_events: to_fixed(weighted_events, 4),
        event_rate_per_minute: to_fixed(event_rate_per_minute, 4),
        coordination_density: to_fixed(coordination_density, 4),
        suspicious_score: to_fixed(suspicious_score, 4),
        window_seconds,
    }
}

fn weak_label(features: &[f64; FEATURE_COUNT], account_age_days: f64, profile_score: f64) -> Option<f64> {
    let strong_positive =
        (features[IDX_SHORT_BURST] >= 1.15 && features[IDX_ACCELERATION] >= 0.65)
            || (features[IDX_COORDINATED_SPAM] >= 0.95 && features[IDX_BURST] >= 0.9)
            || (features[IDX_BURST_ANOMALY] >= 1.05 && features[IDX_YOUNG_ANOMALY] >= 0.78)
            || (features[IDX_NEW_ACCOUNT] >= 0.9
                && features[IDX_COORDINATED] >= 0.65
                && features[IDX_SHORT_BURST] >= 0.9);

    if strong_positive {
        return Some(1.0);
    }

    let strong_negative =
        features[IDX_BURST] <= 0.55
            && features[IDX_SHORT_BURST] <= 0.55
            && features[IDX_COORDINATED_SPAM] <= 0.25
            && account_age_days >= 30.0
            && profile_score >= 0.5;

    if strong_negative {
        return Some(0.0);
    }

    None
}

fn update_model(
    model: &mut OnlineModel,
    features: &[f64; FEATURE_COUNT],
    label: f64,
    prediction: f64,
    config: ServiceConfig,
) {
    let error = prediction - label;
    model.bias = clamp(
        model.bias - config.learning_rate * (error + config.weight_decay * model.bias),
        -config.max_weight_magnitude,
        config.max_weight_magnitude,
    );

    for (idx, weight) in model.weights.iter_mut().enumerate() {
        let gradient = error * features[idx] + config.weight_decay * *weight;
        *weight = clamp(
            *weight - config.learning_rate * gradient,
            -config.max_weight_magnitude,
            config.max_weight_magnitude,
        );
    }

    model.updates += 1;
}

fn update_baseline(
    baseline: &mut Baseline,
    join_rate_per_minute: f64,
    short_join_rate_per_minute: f64,
    young_account_ratio: f64,
    account_age_days: f64,
    alpha: f64,
) {
    update_mean_var(
        &mut baseline.join_rate_mean,
        &mut baseline.join_rate_var,
        join_rate_per_minute,
        alpha,
        0.03,
    );
    update_mean_var(
        &mut baseline.short_join_rate_mean,
        &mut baseline.short_join_rate_var,
        short_join_rate_per_minute,
        alpha,
        0.04,
    );
    update_mean_var(
        &mut baseline.young_ratio_mean,
        &mut baseline.young_ratio_var,
        young_account_ratio,
        alpha,
        0.02,
    );
    update_mean_var(
        &mut baseline.account_age_mean,
        &mut baseline.account_age_var,
        account_age_days,
        alpha,
        12.0,
    );
    baseline.sample_count += 1;
}

fn update_mean_var(mean: &mut f64, variance: &mut f64, value: f64, alpha: f64, min_variance: f64) {
    let delta = value - *mean;
    *mean += alpha * delta;
    *variance = ((1.0 - alpha) * *variance + alpha * delta * delta).max(min_variance);
}

fn dot(left: &[f64; FEATURE_COUNT], right: &[f64; FEATURE_COUNT]) -> f64 {
    let mut total = 0.0;
    for idx in 0..FEATURE_COUNT {
        total += left[idx] * right[idx];
    }
    total
}

fn parse_env_f64(name: &str, default: f64) -> f64 {
    env::var(name)
        .ok()
        .and_then(|text| text.trim().parse::<f64>().ok())
        .unwrap_or(default)
}

fn parse_env_u16(name: &str, default: u16) -> u16 {
    env::var(name)
        .ok()
        .and_then(|text| text.trim().parse::<u16>().ok())
        .unwrap_or(default)
}

fn clamp(value: f64, min: f64, max: f64) -> f64 {
    value.max(min).min(max)
}

fn safe_div(numerator: f64, denominator: f64, fallback: f64) -> f64 {
    if denominator.is_finite() && denominator > 0.0 {
        numerator / denominator
    } else {
        fallback
    }
}

fn sigmoid(value: f64) -> f64 {
    let safe = clamp(value, -30.0, 30.0);
    1.0 / (1.0 + (-safe).exp())
}

fn now_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0)
}

fn to_fixed(value: f64, digits: i32) -> f64 {
    let factor = 10_f64.powi(digits);
    (value * factor).round() / factor
}
