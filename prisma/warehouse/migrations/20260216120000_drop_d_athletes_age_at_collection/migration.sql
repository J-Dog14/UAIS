-- Remove age_at_collection from d_athletes.
-- Age at collection is stored per row in fact tables; the dimension no longer holds this field.

ALTER TABLE "analytics"."d_athletes"
DROP COLUMN IF EXISTS "age_at_collection";
