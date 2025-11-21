/*
  Warnings:

  - You are about to drop the column `email` on the `d_athletes` table. All the data in the column will be lost.
  - You are about to drop the column `phone` on the `d_athletes` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "analytics"."d_athletes" DROP COLUMN "email",
DROP COLUMN "phone",
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_athletic_screen" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_athletic_screen_cmj" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_athletic_screen_dj" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_athletic_screen_nmt" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_athletic_screen_ppu" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_athletic_screen_slv" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_kinematics_hitting" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_kinematics_pitching" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_mobility" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_pro_sup" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_proteus" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_readiness_screen" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_readiness_screen_cmj" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_readiness_screen_i" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_readiness_screen_ir90" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_readiness_screen_ppu" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_readiness_screen_t" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;

-- AlterTable
ALTER TABLE "public"."f_readiness_screen_y" ADD COLUMN     "age_at_collection" DECIMAL,
ADD COLUMN     "age_group" TEXT;
