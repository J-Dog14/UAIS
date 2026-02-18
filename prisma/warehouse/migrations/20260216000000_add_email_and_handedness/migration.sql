-- Add email to d_athletes; add Handedness enum and handedness to f_pitching_trials.
-- Plan: Athletic-Screen-First Athlete Workflow.

-- ============================================================================
-- d_athletes: add email (nullable, normalized: lowercase, trim)
-- ============================================================================
ALTER TABLE "analytics"."d_athletes"
ADD COLUMN IF NOT EXISTS "email" TEXT;

CREATE INDEX IF NOT EXISTS "idx_d_athletes_email"
ON "analytics"."d_athletes"("email");

-- ============================================================================
-- public: create Handedness enum (Left, Right) for f_pitching_trials
-- ============================================================================
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'Handedness') THEN
    CREATE TYPE "public"."Handedness" AS ENUM ('Left', 'Right');
  END IF;
END
$$;

-- ============================================================================
-- f_pitching_trials: add handedness column (from filename LH/RH)
-- ============================================================================
ALTER TABLE "public"."f_pitching_trials"
ADD COLUMN IF NOT EXISTS "handedness" "public"."Handedness";
