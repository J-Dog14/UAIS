-- AlterTable: Add metric_name, frame, and value columns to f_kinematics_hitting
-- This migration adds the columns needed to match the structure of f_kinematics_pitching
-- Since the table is empty, we can safely add NOT NULL constraints

-- Add columns (with temporary defaults since table might not be empty)
ALTER TABLE "public"."f_kinematics_hitting" 
ADD COLUMN IF NOT EXISTS "metric_name" TEXT,
ADD COLUMN IF NOT EXISTS "frame" INTEGER,
ADD COLUMN IF NOT EXISTS "value" DECIMAL;

-- Make columns NOT NULL (safe since table is empty)
-- If table has data, this will fail - adjust accordingly
ALTER TABLE "public"."f_kinematics_hitting" 
ALTER COLUMN "metric_name" SET NOT NULL,
ALTER COLUMN "frame" SET NOT NULL;

-- Add unique constraint
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'idx_f_hitting_unique'
    ) THEN
        ALTER TABLE "public"."f_kinematics_hitting"
        ADD CONSTRAINT "idx_f_hitting_unique" 
        UNIQUE ("athlete_uuid", "session_date", "metric_name", "frame");
    END IF;
END $$;

-- Add index on metric_name
CREATE INDEX IF NOT EXISTS "idx_f_hitting_metric" ON "public"."f_kinematics_hitting"("metric_name");

