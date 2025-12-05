-- Baseline Migration: Sync existing tables and columns
-- This migration adds tables and columns that already exist in the database
-- but are missing from the migration history. All operations use IF NOT EXISTS
-- to make this migration safe to run even if objects already exist.

-- ============================================================================
-- Add missing columns to d_athletes
-- ============================================================================
ALTER TABLE "analytics"."d_athletes" 
ADD COLUMN IF NOT EXISTS "has_arm_action_data" BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE "analytics"."d_athletes" 
ADD COLUMN IF NOT EXISTS "has_curveball_test_data" BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE "analytics"."d_athletes" 
ADD COLUMN IF NOT EXISTS "arm_action_session_count" INTEGER NOT NULL DEFAULT 0;

ALTER TABLE "analytics"."d_athletes" 
ADD COLUMN IF NOT EXISTS "curveball_test_session_count" INTEGER NOT NULL DEFAULT 0;

-- ============================================================================
-- Create source_athlete_map table (if it doesn't exist)
-- ============================================================================
CREATE TABLE IF NOT EXISTS "analytics"."source_athlete_map" (
    "source_system" TEXT NOT NULL,
    "source_athlete_id" TEXT NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY ("source_system", "source_athlete_id"),
    FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE
);

-- Create indexes for source_athlete_map
CREATE INDEX IF NOT EXISTS "idx_source_map_uuid" ON "analytics"."source_athlete_map"("athlete_uuid");
CREATE INDEX IF NOT EXISTS "idx_source_map_system" ON "analytics"."source_athlete_map"("source_system");

-- ============================================================================
-- Create f_arm_action table (if it doesn't exist)
-- ============================================================================
CREATE TABLE IF NOT EXISTS "public"."f_arm_action" (
    "id" SERIAL PRIMARY KEY,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'arm_action',
    "source_athlete_id" VARCHAR(100),
    "filename" TEXT,
    "movement_type" TEXT,
    "foot_contact_frame" INTEGER,
    "release_frame" INTEGER,
    "age_at_collection" DECIMAL,
    "age_group" TEXT,
    "created_at" TIMESTAMP NOT NULL DEFAULT NOW(),
    "arm_abduction_at_footplant" DECIMAL,
    "max_abduction" DECIMAL,
    "shoulder_angle_at_footplant" DECIMAL,
    "max_er" DECIMAL,
    "arm_velo" DECIMAL,
    "max_torso_rot_velo" DECIMAL,
    "torso_angle_at_footplant" DECIMAL,
    "score" DECIMAL,
    FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE
);

-- Create indexes for f_arm_action
CREATE INDEX IF NOT EXISTS "idx_f_arm_action_uuid" ON "public"."f_arm_action"("athlete_uuid");
CREATE INDEX IF NOT EXISTS "idx_f_arm_action_date" ON "public"."f_arm_action"("session_date");
CREATE INDEX IF NOT EXISTS "idx_f_arm_action_movement_type" ON "public"."f_arm_action"("movement_type");

-- ============================================================================
-- Create f_curveball_test table (if it doesn't exist)
-- Note: This creates only the main columns. The actual table has 306+ additional
-- columns for angle/accel data that are managed separately.
-- ============================================================================
CREATE TABLE IF NOT EXISTS "public"."f_curveball_test" (
    "id" SERIAL PRIMARY KEY,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'curveball_test',
    "source_athlete_id" VARCHAR(100),
    "filename" TEXT,
    "pitch_type" TEXT,
    "foot_contact_frame" INTEGER,
    "release_frame" INTEGER,
    "pitch_stability_score" DECIMAL,
    "age_at_collection" DECIMAL,
    "age_group" TEXT,
    "created_at" TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE
);

-- Create indexes for f_curveball_test
CREATE INDEX IF NOT EXISTS "idx_f_curveball_test_uuid" ON "public"."f_curveball_test"("athlete_uuid");
CREATE INDEX IF NOT EXISTS "idx_f_curveball_test_date" ON "public"."f_curveball_test"("session_date");
CREATE INDEX IF NOT EXISTS "idx_f_curveball_test_pitch_type" ON "public"."f_curveball_test"("pitch_type");

-- ============================================================================
-- Restore missing foreign keys (if they were removed)
-- ============================================================================
-- Note: These foreign keys may have been removed due to orphaned records.
-- We'll add them back only if they don't exist and if there are no orphaned records.

DO $$
BEGIN
    -- f_athletic_screen_nmt
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'f_athletic_screen_nmt_athlete_uuid_fkey'
    ) THEN
        -- Check if there are orphaned records first
        IF NOT EXISTS (
            SELECT 1 FROM "public"."f_athletic_screen_nmt" n
            LEFT JOIN "analytics"."d_athletes" a ON n.athlete_uuid = a.athlete_uuid
            WHERE a.athlete_uuid IS NULL
        ) THEN
            ALTER TABLE "public"."f_athletic_screen_nmt"
            ADD CONSTRAINT "f_athletic_screen_nmt_athlete_uuid_fkey"
            FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE;
        END IF;
    END IF;

    -- f_athletic_screen_ppu
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'f_athletic_screen_ppu_athlete_uuid_fkey'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM "public"."f_athletic_screen_ppu" n
            LEFT JOIN "analytics"."d_athletes" a ON n.athlete_uuid = a.athlete_uuid
            WHERE a.athlete_uuid IS NULL
        ) THEN
            ALTER TABLE "public"."f_athletic_screen_ppu"
            ADD CONSTRAINT "f_athletic_screen_ppu_athlete_uuid_fkey"
            FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE;
        END IF;
    END IF;

    -- f_kinematics_pitching
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'f_kinematics_pitching_athlete_uuid_fkey'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM "public"."f_kinematics_pitching" n
            LEFT JOIN "analytics"."d_athletes" a ON n.athlete_uuid = a.athlete_uuid
            WHERE a.athlete_uuid IS NULL
        ) THEN
            ALTER TABLE "public"."f_kinematics_pitching"
            ADD CONSTRAINT "f_kinematics_pitching_athlete_uuid_fkey"
            FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE;
        END IF;
    END IF;

    -- f_pro_sup
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'f_pro_sup_athlete_uuid_fkey'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM "public"."f_pro_sup" n
            LEFT JOIN "analytics"."d_athletes" a ON n.athlete_uuid = a.athlete_uuid
            WHERE a.athlete_uuid IS NULL
        ) THEN
            ALTER TABLE "public"."f_pro_sup"
            ADD CONSTRAINT "f_pro_sup_athlete_uuid_fkey"
            FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE;
        END IF;
    END IF;

    -- f_readiness_screen_cmj
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'f_readiness_screen_cmj_athlete_uuid_fkey'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM "public"."f_readiness_screen_cmj" n
            LEFT JOIN "analytics"."d_athletes" a ON n.athlete_uuid = a.athlete_uuid
            WHERE a.athlete_uuid IS NULL
        ) THEN
            ALTER TABLE "public"."f_readiness_screen_cmj"
            ADD CONSTRAINT "f_readiness_screen_cmj_athlete_uuid_fkey"
            FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE;
        END IF;
    END IF;

    -- f_readiness_screen_i
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'f_readiness_screen_i_athlete_uuid_fkey'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM "public"."f_readiness_screen_i" n
            LEFT JOIN "analytics"."d_athletes" a ON n.athlete_uuid = a.athlete_uuid
            WHERE a.athlete_uuid IS NULL
        ) THEN
            ALTER TABLE "public"."f_readiness_screen_i"
            ADD CONSTRAINT "f_readiness_screen_i_athlete_uuid_fkey"
            FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE;
        END IF;
    END IF;

    -- f_readiness_screen_ir90
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'f_readiness_screen_ir90_athlete_uuid_fkey'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM "public"."f_readiness_screen_ir90" n
            LEFT JOIN "analytics"."d_athletes" a ON n.athlete_uuid = a.athlete_uuid
            WHERE a.athlete_uuid IS NULL
        ) THEN
            ALTER TABLE "public"."f_readiness_screen_ir90"
            ADD CONSTRAINT "f_readiness_screen_ir90_athlete_uuid_fkey"
            FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE;
        END IF;
    END IF;

    -- f_readiness_screen_ppu
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'f_readiness_screen_ppu_athlete_uuid_fkey'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM "public"."f_readiness_screen_ppu" n
            LEFT JOIN "analytics"."d_athletes" a ON n.athlete_uuid = a.athlete_uuid
            WHERE a.athlete_uuid IS NULL
        ) THEN
            ALTER TABLE "public"."f_readiness_screen_ppu"
            ADD CONSTRAINT "f_readiness_screen_ppu_athlete_uuid_fkey"
            FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE;
        END IF;
    END IF;

    -- f_readiness_screen_t
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'f_readiness_screen_t_athlete_uuid_fkey'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM "public"."f_readiness_screen_t" n
            LEFT JOIN "analytics"."d_athletes" a ON n.athlete_uuid = a.athlete_uuid
            WHERE a.athlete_uuid IS NULL
        ) THEN
            ALTER TABLE "public"."f_readiness_screen_t"
            ADD CONSTRAINT "f_readiness_screen_t_athlete_uuid_fkey"
            FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE;
        END IF;
    END IF;

    -- f_readiness_screen_y
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'f_readiness_screen_y_athlete_uuid_fkey'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM "public"."f_readiness_screen_y" n
            LEFT JOIN "analytics"."d_athletes" a ON n.athlete_uuid = a.athlete_uuid
            WHERE a.athlete_uuid IS NULL
        ) THEN
            ALTER TABLE "public"."f_readiness_screen_y"
            ADD CONSTRAINT "f_readiness_screen_y_athlete_uuid_fkey"
            FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE;
        END IF;
    END IF;
END $$;

-- ============================================================================
-- Add foreign keys with Prisma's expected constraint names
-- ============================================================================
-- These foreign keys were created by prisma db push, so we're adding them
-- to the migration history to keep it in sync

-- f_arm_action
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'FArmAction_athleteUuid_fkey'
    ) THEN
        ALTER TABLE "public"."f_arm_action"
        ADD CONSTRAINT "FArmAction_athleteUuid_fkey"
        FOREIGN KEY ("athlete_uuid") 
        REFERENCES "analytics"."d_athletes"("athlete_uuid") 
        ON DELETE CASCADE;
    END IF;
END $$;

-- f_curveball_test
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'FCurveballTest_athleteUuid_fkey'
    ) THEN
        ALTER TABLE "public"."f_curveball_test"
        ADD CONSTRAINT "FCurveballTest_athleteUuid_fkey"
        FOREIGN KEY ("athlete_uuid") 
        REFERENCES "analytics"."d_athletes"("athlete_uuid") 
        ON DELETE CASCADE;
    END IF;
END $$;

-- source_athlete_map
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'DSourceAthleteMap_athleteUuid_fkey'
    ) THEN
        ALTER TABLE "analytics"."source_athlete_map"
        ADD CONSTRAINT "DSourceAthleteMap_athleteUuid_fkey"
        FOREIGN KEY ("athlete_uuid") 
        REFERENCES "analytics"."d_athletes"("athlete_uuid") 
        ON DELETE CASCADE;
    END IF;
END $$;

