-- Add hitting trial tracking columns to d_athletes (mirror pitching trial columns).

ALTER TABLE "analytics"."d_athletes"
ADD COLUMN IF NOT EXISTS "has_hitting_trial_data" BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE "analytics"."d_athletes"
ADD COLUMN IF NOT EXISTS "hitting_trial_count" INTEGER NOT NULL DEFAULT 0;
