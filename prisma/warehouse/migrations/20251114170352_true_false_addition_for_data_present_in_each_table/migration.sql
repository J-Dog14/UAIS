-- CreateSchema
CREATE SCHEMA IF NOT EXISTS "analytics";

-- CreateTable
CREATE TABLE "analytics"."d_athletes" (
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "name" TEXT NOT NULL,
    "normalized_name" TEXT NOT NULL,
    "date_of_birth" DATE,
    "age" DECIMAL,
    "age_at_collection" DECIMAL,
    "gender" TEXT,
    "height" DECIMAL,
    "weight" DECIMAL,
    "email" TEXT,
    "phone" TEXT,
    "notes" TEXT,
    "source_system" TEXT,
    "source_athlete_id" TEXT,
    "app_db_uuid" TEXT,
    "app_db_synced_at" TIMESTAMPTZ,
    "has_pitching_data" BOOLEAN NOT NULL DEFAULT false,
    "has_athletic_screen_data" BOOLEAN NOT NULL DEFAULT false,
    "has_pro_sup_data" BOOLEAN NOT NULL DEFAULT false,
    "has_readiness_screen_data" BOOLEAN NOT NULL DEFAULT false,
    "has_mobility_data" BOOLEAN NOT NULL DEFAULT false,
    "has_proteus_data" BOOLEAN NOT NULL DEFAULT false,
    "has_hitting_data" BOOLEAN NOT NULL DEFAULT false,
    "pitching_session_count" INTEGER NOT NULL DEFAULT 0,
    "athletic_screen_session_count" INTEGER NOT NULL DEFAULT 0,
    "pro_sup_session_count" INTEGER NOT NULL DEFAULT 0,
    "readiness_screen_session_count" INTEGER NOT NULL DEFAULT 0,
    "mobility_session_count" INTEGER NOT NULL DEFAULT 0,
    "proteus_session_count" INTEGER NOT NULL DEFAULT 0,
    "hitting_session_count" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "d_athletes_pkey" PRIMARY KEY ("athlete_uuid")
);

-- CreateTable
CREATE TABLE "public"."f_athletic_screen" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL,
    "source_athlete_id" VARCHAR(100),
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "f_athletic_screen_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_pro_sup" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL,
    "source_athlete_id" VARCHAR(100),
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "age" DECIMAL,
    "height" DECIMAL,
    "weight" DECIMAL,
    "injury_history" TEXT,
    "season_phase" VARCHAR(50),
    "dynomometer_score" VARCHAR(50),
    "comments" TEXT,
    "forearm_rom_0to10" DECIMAL,
    "forearm_rom_10to20" DECIMAL,
    "forearm_rom_20to30" DECIMAL,
    "forearm_rom" DECIMAL,
    "tot_rom_0to10" DECIMAL,
    "tot_rom_10to20" DECIMAL,
    "tot_rom_20to30" DECIMAL,
    "tot_rom" DECIMAL,
    "num_of_flips_0_10" DECIMAL,
    "num_of_flips_10_20" DECIMAL,
    "num_of_flips_20_30" DECIMAL,
    "num_of_flips" DECIMAL,
    "avg_velo_0_10" DECIMAL,
    "avg_velo_10_20" DECIMAL,
    "avg_velo_20_30" DECIMAL,
    "avg_velo" DECIMAL,
    "fatigue_index_10" DECIMAL,
    "fatigue_index_20" DECIMAL,
    "fatigue_index_30" DECIMAL,
    "total_fatigue_score" DECIMAL,
    "consistency_penalty" DECIMAL,
    "total_score" DECIMAL,

    CONSTRAINT "f_pro_sup_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_readiness_screen" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL,
    "source_athlete_id" VARCHAR(100),
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "f_readiness_screen_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_mobility" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL,
    "source_athlete_id" VARCHAR(100),
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "f_mobility_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_proteus" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL,
    "source_athlete_id" VARCHAR(100),
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "f_proteus_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_kinematics_pitching" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL,
    "source_athlete_id" VARCHAR(100),
    "metric_name" TEXT NOT NULL,
    "frame" INTEGER NOT NULL,
    "value" DECIMAL,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "f_kinematics_pitching_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."f_kinematics_hitting" (
    "id" SERIAL NOT NULL,
    "athlete_uuid" VARCHAR(36) NOT NULL,
    "session_date" DATE NOT NULL,
    "source_system" VARCHAR(50) NOT NULL,
    "source_athlete_id" VARCHAR(100),
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "f_kinematics_hitting_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "d_athletes_normalized_name_key" ON "analytics"."d_athletes"("normalized_name");

-- CreateIndex
CREATE INDEX "idx_d_athletes_normalized_name" ON "analytics"."d_athletes"("normalized_name");

-- CreateIndex
CREATE INDEX "idx_d_athletes_app_db_uuid" ON "analytics"."d_athletes"("app_db_uuid");

-- CreateIndex
CREATE INDEX "idx_d_athletes_source_system" ON "analytics"."d_athletes"("source_system", "source_athlete_id");

-- CreateIndex
CREATE INDEX "idx_f_athletic_screen_uuid" ON "public"."f_athletic_screen"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_athletic_screen_date" ON "public"."f_athletic_screen"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_pro_sup_uuid" ON "public"."f_pro_sup"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_pro_sup_date" ON "public"."f_pro_sup"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_readiness_uuid" ON "public"."f_readiness_screen"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_readiness_date" ON "public"."f_readiness_screen"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_mobility_uuid" ON "public"."f_mobility"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_mobility_date" ON "public"."f_mobility"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_proteus_uuid" ON "public"."f_proteus"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_proteus_date" ON "public"."f_proteus"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_pitching_uuid" ON "public"."f_kinematics_pitching"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_pitching_date" ON "public"."f_kinematics_pitching"("session_date");

-- CreateIndex
CREATE INDEX "idx_f_pitching_metric" ON "public"."f_kinematics_pitching"("metric_name");

-- CreateIndex
CREATE UNIQUE INDEX "idx_f_pitching_unique" ON "public"."f_kinematics_pitching"("athlete_uuid", "session_date", "metric_name", "frame");

-- CreateIndex
CREATE INDEX "idx_f_hitting_uuid" ON "public"."f_kinematics_hitting"("athlete_uuid");

-- CreateIndex
CREATE INDEX "idx_f_hitting_date" ON "public"."f_kinematics_hitting"("session_date");

-- AddForeignKey
ALTER TABLE "public"."f_athletic_screen" ADD CONSTRAINT "f_athletic_screen_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_pro_sup" ADD CONSTRAINT "f_pro_sup_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_readiness_screen" ADD CONSTRAINT "f_readiness_screen_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_mobility" ADD CONSTRAINT "f_mobility_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_proteus" ADD CONSTRAINT "f_proteus_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_kinematics_pitching" ADD CONSTRAINT "f_kinematics_pitching_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."f_kinematics_hitting" ADD CONSTRAINT "f_kinematics_hitting_athlete_uuid_fkey" FOREIGN KEY ("athlete_uuid") REFERENCES "analytics"."d_athletes"("athlete_uuid") ON DELETE CASCADE ON UPDATE CASCADE;
