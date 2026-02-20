-- Add name column to f_pitching_trials for quick athlete identification.

ALTER TABLE "public"."f_pitching_trials"
ADD COLUMN IF NOT EXISTS "name" VARCHAR(255);
