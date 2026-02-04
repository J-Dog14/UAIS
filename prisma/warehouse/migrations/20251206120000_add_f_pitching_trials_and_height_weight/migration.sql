-- Create f_pitching_trials table if it doesn't exist (e.g. created by Python script or new env).
-- Adds height and weight columns if table existed without them.
-- Safe to run idempotently.

-- ============================================================================
-- Create f_pitching_trials table (if not exists)
-- ============================================================================
CREATE TABLE IF NOT EXISTS "public"."f_pitching_trials" (
    "id" SERIAL PRIMARY KEY,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'pitching',
    "source_athlete_id" VARCHAR(100),
    "owner_filename" TEXT,
    "trial_index" INTEGER NOT NULL,
    "velocity_mph" DECIMAL,
    "score" DECIMAL,
    "age_at_collection" DECIMAL,
    "age_group" TEXT,
    "height" DECIMAL,
    "weight" DECIMAL,
    "metrics" JSONB NOT NULL,
    "session_xml_path" TEXT,
    "session_data_xml_path" TEXT,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT "f_pitching_trials_athlete_uuid_fkey"
        FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE
);

-- Add height/weight if table already existed without them (e.g. created by older Python script)
ALTER TABLE "public"."f_pitching_trials" ADD COLUMN IF NOT EXISTS "height" DECIMAL;
ALTER TABLE "public"."f_pitching_trials" ADD COLUMN IF NOT EXISTS "weight" DECIMAL;

-- Indexes
CREATE UNIQUE INDEX IF NOT EXISTS "idx_f_pitching_trials_unique"
    ON "public"."f_pitching_trials"("athlete_uuid", "session_date", "trial_index");
CREATE INDEX IF NOT EXISTS "idx_f_pitching_trials_date"
    ON "public"."f_pitching_trials"("session_date");
CREATE INDEX IF NOT EXISTS "idx_f_pitching_trials_owner"
    ON "public"."f_pitching_trials"("owner_filename");
