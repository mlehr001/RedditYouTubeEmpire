-- Story Engine PostgreSQL Schema
-- All tables follow strict separation: real content vs AI-assist content

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ─────────────────────────────────────────────
-- SOURCES
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sources (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name        TEXT NOT NULL UNIQUE,
    adapter     TEXT NOT NULL,
    config      JSONB NOT NULL DEFAULT '{}',
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    last_fetched_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- RAW STORIES (verbatim from source — NEVER modified)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_stories (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id       UUID NOT NULL REFERENCES sources(id),
    external_id     TEXT NOT NULL,
    url             TEXT NOT NULL,
    title           TEXT NOT NULL,
    body            TEXT NOT NULL,
    author          TEXT,
    subreddit       TEXT,
    upvotes         INT,
    comment_count   INT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_payload     JSONB NOT NULL DEFAULT '{}',
    UNIQUE(source_id, external_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_stories_source ON raw_stories(source_id);
CREATE INDEX IF NOT EXISTS idx_raw_stories_fetched ON raw_stories(fetched_at DESC);

-- ─────────────────────────────────────────────
-- NORMALIZED STORIES
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stories (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    raw_story_id    UUID NOT NULL REFERENCES raw_stories(id) UNIQUE,
    title           TEXT NOT NULL,
    body            TEXT NOT NULL,
    word_count      INT NOT NULL,
    char_count      INT NOT NULL,
    language        TEXT NOT NULL DEFAULT 'en',
    normalized_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    pipeline_status TEXT NOT NULL DEFAULT 'normalized'
        CHECK (pipeline_status IN (
            'normalized','cleaned','scored','hooked',
            'validated','censored','formatted','stored',
            'video_queued','video_built','published','rejected','failed'
        )),
    rejection_reason TEXT,
    failed_stage     TEXT,
    error_message    TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stories_status ON stories(pipeline_status);
CREATE INDEX IF NOT EXISTS idx_stories_updated ON stories(updated_at DESC);

-- ─────────────────────────────────────────────
-- AI ASSIST
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ai_assist (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    story_id        UUID NOT NULL REFERENCES stories(id) UNIQUE,
    score           NUMERIC(4,2),
    score_rationale TEXT,
    scored_at       TIMESTAMPTZ,
    hook_order      JSONB,
    hook_text       TEXT,
    hooked_at       TIMESTAMPTZ,
    formatted_lines JSONB,
    formatted_at    TIMESTAMPTZ,
    ai_title        TEXT,
    titled_at       TIMESTAMPTZ,
    category        TEXT,
    tags            JSONB DEFAULT '[]',
    classified_at   TIMESTAMPTZ,
    model_used      TEXT,
    prompt_version  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- CENSORSHIP LOG
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS censor_log (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    story_id    UUID NOT NULL REFERENCES stories(id),
    rule_hit    TEXT NOT NULL,
    severity    TEXT NOT NULL CHECK (severity IN ('warn','block')),
    field       TEXT NOT NULL,
    matched     TEXT NOT NULL,
    action      TEXT NOT NULL CHECK (action IN ('flagged','blocked','redacted')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_censor_story ON censor_log(story_id);

-- ─────────────────────────────────────────────
-- VALIDATION LOG
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS validation_log (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    story_id    UUID NOT NULL REFERENCES stories(id),
    stage       TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    checks      JSONB NOT NULL DEFAULT '[]',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_validation_story ON validation_log(story_id);

-- ─────────────────────────────────────────────
-- VIDEOS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS videos (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    story_id        UUID NOT NULL REFERENCES stories(id),
    format          TEXT NOT NULL CHECK (format IN ('short','long')),
    resolution      TEXT NOT NULL DEFAULT '1080x1920',
    duration_sec    NUMERIC(8,2),
    file_path       TEXT,
    file_size_bytes BIGINT,
    ffmpeg_command  TEXT,
    build_status    TEXT NOT NULL DEFAULT 'queued'
        CHECK (build_status IN ('queued','building','done','failed')),
    error_message   TEXT,
    built_at        TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_videos_story ON videos(story_id);
CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(build_status);

-- ─────────────────────────────────────────────
-- PUBLISH HISTORY
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS publish_history (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    video_id        UUID NOT NULL REFERENCES videos(id),
    platform        TEXT NOT NULL,
    platform_id     TEXT,
    platform_url    TEXT,
    status          TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','published','failed','removed')),
    error_message   TEXT,
    scheduled_at    TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_publish_video ON publish_history(video_id);
CREATE INDEX IF NOT EXISTS idx_publish_platform ON publish_history(platform, status);

-- ─────────────────────────────────────────────
-- ANALYTICS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    publish_id      UUID NOT NULL REFERENCES publish_history(id),
    snapshot_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    views           BIGINT DEFAULT 0,
    likes           BIGINT DEFAULT 0,
    comments        BIGINT DEFAULT 0,
    shares          BIGINT DEFAULT 0,
    watch_time_sec  BIGINT DEFAULT 0,
    retention_pct   NUMERIC(5,2),
    raw_payload     JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_analytics_publish ON analytics(publish_id);
CREATE INDEX IF NOT EXISTS idx_analytics_snapshot ON analytics(snapshot_at DESC);

-- ─────────────────────────────────────────────
-- PIPELINE JOBS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_jobs (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    story_id    UUID REFERENCES stories(id),
    stage       TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('running','done','failed','retrying')),
    attempt     INT NOT NULL DEFAULT 1,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    duration_ms INT,
    error       TEXT,
    meta        JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_jobs_story ON pipeline_jobs(story_id);
CREATE INDEX IF NOT EXISTS idx_jobs_stage ON pipeline_jobs(stage, status);
CREATE INDEX IF NOT EXISTS idx_jobs_started ON pipeline_jobs(started_at DESC);