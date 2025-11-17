-- AlterTable
ALTER TABLE "public"."f_mobility" ADD COLUMN     "assessment_date" DATE,
ALTER COLUMN "source_system" SET DEFAULT 'mobility';

-- AlterTable
ALTER TABLE "public"."f_pro_sup" ADD COLUMN     "cumulative_rom" DECIMAL,
ADD COLUMN     "raw_total_score" DECIMAL;

-- AlterTable
ALTER TABLE "public"."f_proteus" ADD COLUMN     "acceleration_high" DECIMAL,
ADD COLUMN     "acceleration_low" DECIMAL,
ADD COLUMN     "acceleration_mean" DECIMAL,
ADD COLUMN     "birth_date" TEXT,
ADD COLUMN     "braking_high" DECIMAL,
ADD COLUMN     "braking_low" DECIMAL,
ADD COLUMN     "braking_mean" DECIMAL,
ADD COLUMN     "consistency_high" DECIMAL,
ADD COLUMN     "consistency_low" DECIMAL,
ADD COLUMN     "consistency_mean" DECIMAL,
ADD COLUMN     "deceleration_high" DECIMAL,
ADD COLUMN     "deceleration_low" DECIMAL,
ADD COLUMN     "deceleration_mean" DECIMAL,
ADD COLUMN     "dominance" TEXT,
ADD COLUMN     "exercise_created_at" TEXT,
ADD COLUMN     "exercise_id" INTEGER,
ADD COLUMN     "exercise_name" TEXT,
ADD COLUMN     "explosiveness_high" DECIMAL,
ADD COLUMN     "explosiveness_low" DECIMAL,
ADD COLUMN     "explosiveness_mean" DECIMAL,
ADD COLUMN     "height" INTEGER,
ADD COLUMN     "movement" TEXT,
ADD COLUMN     "personal_record" BOOLEAN,
ADD COLUMN     "position" TEXT,
ADD COLUMN     "power_high" DECIMAL,
ADD COLUMN     "power_low" DECIMAL,
ADD COLUMN     "power_mean" DECIMAL,
ADD COLUMN     "proteus_attachment" TEXT,
ADD COLUMN     "range_of_motion_high" DECIMAL,
ADD COLUMN     "range_of_motion_low" DECIMAL,
ADD COLUMN     "range_of_motion_mean" DECIMAL,
ADD COLUMN     "reps" INTEGER,
ADD COLUMN     "resistance" INTEGER,
ADD COLUMN     "session_created_at" TEXT,
ADD COLUMN     "session_id" INTEGER,
ADD COLUMN     "session_name" TEXT,
ADD COLUMN     "set_number" INTEGER,
ADD COLUMN     "sex" TEXT,
ADD COLUMN     "sport" TEXT,
ADD COLUMN     "user_id" INTEGER,
ADD COLUMN     "user_name" TEXT,
ADD COLUMN     "velocity_high" DECIMAL,
ADD COLUMN     "velocity_low" DECIMAL,
ADD COLUMN     "velocity_mean" DECIMAL,
ADD COLUMN     "weight" INTEGER,
ALTER COLUMN "source_system" SET DEFAULT 'proteus';

-- CreateTable
CREATE TABLE "public"."f_athletic_screen_cmj" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'athletic_screen',
    "source_athlete_id" VARCHAR(100),
    "trial_name" TEXT,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "jh_in" DECIMAL,
    "peak_power" DECIMAL,
    "pp_forceplate" DECIMAL,
    "force_at_pp" DECIMAL,
    "vel_at_pp" DECIMAL,
    "pp_w_per_kg" DECIMAL,
    "peak_power_w" DECIMAL,
    "time_to_peak_s" DECIMAL,
    "rpd_max_w_per_s" DECIMAL,
    "time_to_rpd_max_s" DECIMAL,
    "rise_time_10_90_s" DECIMAL,
    "fwhm_s" DECIMAL,
    "auc_j" DECIMAL,
    "work_early_pct" DECIMAL,
    "decay_90_10_s" DECIMAL,
    "t_com_norm_0to1" DECIMAL,
    "skewness" DECIMAL,
    "kurtosis" DECIMAL,
    "spectral_centroid_hz" DECIMAL,
    "demographic" TEXT,

    CONSTRAINT "f_athletic_screen_cmj_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_athletic_screen_dj" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'athletic_screen',
    "source_athlete_id" VARCHAR(100),
    "trial_name" TEXT,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "jh_in" DECIMAL,
    "peak_power" DECIMAL,
    "pp_forceplate" DECIMAL,
    "force_at_pp" DECIMAL,
    "vel_at_pp" DECIMAL,
    "pp_w_per_kg" DECIMAL,
    "ct" DECIMAL,
    "rsi" DECIMAL,
    "peak_power_w" DECIMAL,
    "time_to_peak_s" DECIMAL,
    "rpd_max_w_per_s" DECIMAL,
    "time_to_rpd_max_s" DECIMAL,
    "rise_time_10_90_s" DECIMAL,
    "fwhm_s" DECIMAL,
    "auc_j" DECIMAL,
    "work_early_pct" DECIMAL,
    "decay_90_10_s" DECIMAL,
    "t_com_norm_0to1" DECIMAL,
    "skewness" DECIMAL,
    "kurtosis" DECIMAL,
    "spectral_centroid_hz" DECIMAL,
    "demographic" TEXT,

    CONSTRAINT "f_athletic_screen_dj_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_athletic_screen_slv" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'athletic_screen',
    "source_athlete_id" VARCHAR(100),
    "trial_name" TEXT,
    "side" TEXT,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "jh_in" DECIMAL,
    "pp_forceplate" DECIMAL,
    "force_at_pp" DECIMAL,
    "vel_at_pp" DECIMAL,
    "pp_w_per_kg" DECIMAL,
    "peak_power_w" DECIMAL,
    "time_to_peak_s" DECIMAL,
    "rpd_max_w_per_s" DECIMAL,
    "time_to_rpd_max_s" DECIMAL,
    "rise_time_10_90_s" DECIMAL,
    "fwhm_s" DECIMAL,
    "auc_j" DECIMAL,
    "work_early_pct" DECIMAL,
    "decay_90_10_s" DECIMAL,
    "t_com_norm_0to1" DECIMAL,
    "skewness" DECIMAL,
    "kurtosis" DECIMAL,
    "spectral_centroid_hz" DECIMAL,
    "demographic" TEXT,

    CONSTRAINT "f_athletic_screen_slv_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_athletic_screen_nmt" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'athletic_screen',
    "source_athlete_id" VARCHAR(100),
    "trial_name" TEXT,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "num_taps_10s" DECIMAL,
    "num_taps_20s" DECIMAL,
    "num_taps_30s" DECIMAL,
    "num_taps" DECIMAL,
    "demographic" TEXT,

    CONSTRAINT "f_athletic_screen_nmt_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_athletic_screen_ppu" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'athletic_screen',
    "source_athlete_id" VARCHAR(100),
    "trial_name" TEXT,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "jh_in" DECIMAL,
    "peak_power" DECIMAL,
    "pp_forceplate" DECIMAL,
    "force_at_pp" DECIMAL,
    "vel_at_pp" DECIMAL,
    "pp_w_per_kg" DECIMAL,
    "peak_power_w" DECIMAL,
    "time_to_peak_s" DECIMAL,
    "rpd_max_w_per_s" DECIMAL,
    "time_to_rpd_max_s" DECIMAL,
    "rise_time_10_90_s" DECIMAL,
    "fwhm_s" DECIMAL,
    "auc_j" DECIMAL,
    "work_early_pct" DECIMAL,
    "decay_90_10_s" DECIMAL,
    "t_com_norm_0to1" DECIMAL,
    "skewness" DECIMAL,
    "kurtosis" DECIMAL,
    "spectral_centroid_hz" DECIMAL,
    "demographic" TEXT,

    CONSTRAINT "f_athletic_screen_ppu_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_readiness_screen_i" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'readiness_screen',
    "source_athlete_id" VARCHAR(100),
    "trial_id" INTEGER,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "avg_force" DECIMAL,
    "avg_force_norm" DECIMAL,
    "max_force" DECIMAL,
    "max_force_norm" DECIMAL,
    "time_to_max" DECIMAL,

    CONSTRAINT "f_readiness_screen_i_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_readiness_screen_y" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'readiness_screen',
    "source_athlete_id" VARCHAR(100),
    "trial_id" INTEGER,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "avg_force" DECIMAL,
    "avg_force_norm" DECIMAL,
    "max_force" DECIMAL,
    "max_force_norm" DECIMAL,
    "time_to_max" DECIMAL,

    CONSTRAINT "f_readiness_screen_y_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_readiness_screen_t" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'readiness_screen',
    "source_athlete_id" VARCHAR(100),
    "trial_id" INTEGER,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "avg_force" DECIMAL,
    "avg_force_norm" DECIMAL,
    "max_force" DECIMAL,
    "max_force_norm" DECIMAL,
    "time_to_max" DECIMAL,

    CONSTRAINT "f_readiness_screen_t_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_readiness_screen_ir90" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'readiness_screen',
    "source_athlete_id" VARCHAR(100),
    "trial_id" INTEGER,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "avg_force" DECIMAL,
    "avg_force_norm" DECIMAL,
    "max_force" DECIMAL,
    "max_force_norm" DECIMAL,
    "time_to_max" DECIMAL,

    CONSTRAINT "f_readiness_screen_ir90_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_readiness_screen_cmj" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'readiness_screen',
    "source_athlete_id" VARCHAR(100),
    "trial_id" INTEGER,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "jump_height" DECIMAL,
    "peak_power" DECIMAL,
    "peak_force" DECIMAL,
    "pp_w_per_kg" DECIMAL,
    "pp_forceplate" DECIMAL,
    "force_at_pp" DECIMAL,
    "vel_at_pp" DECIMAL,

    CONSTRAINT "f_readiness_screen_cmj_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_readiness_screen_ppu" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL DEFAULT 'readiness_screen',
    "source_athlete_id" VARCHAR(100),
    "trial_id" INTEGER,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "jump_height" DECIMAL,
    "peak_power" DECIMAL,
    "peak_force" DECIMAL,
    "pp_w_per_kg" DECIMAL,
    "pp_forceplate" DECIMAL,
    "force_at_pp" DECIMAL,
    "vel_at_pp" DECIMAL,

    CONSTRAINT "f_readiness_screen_ppu_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "idx_f_athletic_screen_cmj_uuid" ON "public"."f_athletic_screen_cmj"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_athletic_screen_cmj_date" ON "public"."f_athletic_screen_cmj"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_athletic_screen_dj_uuid" ON "public"."f_athletic_screen_dj"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_athletic_screen_dj_date" ON "public"."f_athletic_screen_dj"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_athletic_screen_slv_uuid" ON "public"."f_athletic_screen_slv"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_athletic_screen_slv_date" ON "public"."f_athletic_screen_slv"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_athletic_screen_nmt_uuid" ON "public"."f_athletic_screen_nmt"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_athletic_screen_nmt_date" ON "public"."f_athletic_screen_nmt"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_athletic_screen_ppu_uuid" ON "public"."f_athletic_screen_ppu"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_athletic_screen_ppu_date" ON "public"."f_athletic_screen_ppu"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_readiness_screen_i_uuid" ON "public"."f_readiness_screen_i"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_readiness_screen_i_date" ON "public"."f_readiness_screen_i"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_readiness_screen_y_uuid" ON "public"."f_readiness_screen_y"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_readiness_screen_y_date" ON "public"."f_readiness_screen_y"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_readiness_screen_t_uuid" ON "public"."f_readiness_screen_t"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_readiness_screen_t_date" ON "public"."f_readiness_screen_t"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_readiness_screen_ir90_uuid" ON "public"."f_readiness_screen_ir90"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_readiness_screen_ir90_date" ON "public"."f_readiness_screen_ir90"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_readiness_screen_cmj_uuid" ON "public"."f_readiness_screen_cmj"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_readiness_screen_cmj_date" ON "public"."f_readiness_screen_cmj"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_readiness_screen_ppu_uuid" ON "public"."f_readiness_screen_ppu"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_readiness_screen_ppu_date" ON "public"."f_readiness_screen_ppu"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_mobility_assessment_date" ON "public"."f_mobility"("assessment_date");

-- CreateIndex
CREATE INDEX "idx_f_proteus_session_id" ON "public"."f_proteus"("session_id");

-- CreateIndex
CREATE INDEX "idx_f_proteus_exercise_id" ON "public"."f_proteus"("exercise_id");

-- AddForeignKey
ALTER TABLE "public"."f_athletic_screen_cmj" ADD CONSTRAINT "f_athletic_screen_cmj_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_athletic_screen_dj" ADD CONSTRAINT "f_athletic_screen_dj_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_athletic_screen_slv" ADD CONSTRAINT "f_athletic_screen_slv_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_athletic_screen_nmt" ADD CONSTRAINT "f_athletic_screen_nmt_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_athletic_screen_ppu" ADD CONSTRAINT "f_athletic_screen_ppu_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_readiness_screen_i" ADD CONSTRAINT "f_readiness_screen_i_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_readiness_screen_y" ADD CONSTRAINT "f_readiness_screen_y_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_readiness_screen_t" ADD CONSTRAINT "f_readiness_screen_t_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_readiness_screen_ir90" ADD CONSTRAINT "f_readiness_screen_ir90_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_readiness_screen_cmj" ADD CONSTRAINT "f_readiness_screen_cmj_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_readiness_screen_ppu" ADD CONSTRAINT "f_readiness_screen_ppu_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;
