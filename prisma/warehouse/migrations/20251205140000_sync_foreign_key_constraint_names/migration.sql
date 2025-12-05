-- Migration to sync foreign key constraint names with Prisma's expected naming
-- This migration renames foreign key constraints to match Prisma's convention
-- Prisma uses: ModelName_fieldName_fkey (e.g., FArmAction_athleteUuid_fkey)

-- Note: These constraints already exist with different names, so we're just renaming them
-- to match what Prisma expects. The actual foreign key relationships are unchanged.

-- Rename foreign key constraints to match Prisma's naming convention
DO $$
BEGIN
    -- f_arm_action
    IF EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'f_arm_action_athlete_uuid_fkey'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'FArmAction_athleteUuid_fkey'
    ) THEN
        ALTER TABLE "public"."f_arm_action"
        RENAME CONSTRAINT "f_arm_action_athlete_uuid_fkey" TO "FArmAction_athleteUuid_fkey";
    END IF;

    -- f_curveball_test
    IF EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'f_curveball_test_athlete_uuid_fkey'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'FCurveballTest_athleteUuid_fkey'
    ) THEN
        ALTER TABLE "public"."f_curveball_test"
        RENAME CONSTRAINT "f_curveball_test_athlete_uuid_fkey" TO "FCurveballTest_athleteUuid_fkey";
    END IF;

    -- source_athlete_map
    IF EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'source_athlete_map_athlete_uuid_fkey'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'DSourceAthleteMap_athleteUuid_fkey'
    ) THEN
        ALTER TABLE "analytics"."source_athlete_map"
        RENAME CONSTRAINT "source_athlete_map_athlete_uuid_fkey" TO "DSourceAthleteMap_athleteUuid_fkey";
    END IF;
END $$;

