# Prisma Schema Management

This directory contains Prisma schema files for managing the UAIS database schemas.

## Structure

- `warehouse/schema.prisma` - Warehouse database schema (fact and dimension tables)
- `app/schema.prisma` - App database schema (User table)

## Setup

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Set up environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

3. **Generate Prisma Client:**
   ```bash
   npm run prisma:warehouse:generate
   npm run prisma:app:generate
   ```

## Usage

### View Database in Prisma Studio

```bash
# Warehouse database
npm run prisma:warehouse:studio

# App database
npm run prisma:app:studio
```

### Create Migrations

```bash
# Warehouse database
npm run prisma:warehouse:migrate

# App database
npm run prisma:app:migrate
```

### Apply Migrations (Production)

```bash
npm run prisma:migrate:deploy
```

### Format Schema Files

```bash
npm run prisma:format
```

### Validate Schema

```bash
npm run prisma:validate
```

## Schema Files

### Warehouse Schema (`prisma/warehouse/schema.prisma`)

Contains:
- **Dimension Tables:**
  - `DAthletes` - Master athlete dimension table (analytics.d_athletes)

- **Fact Tables:**
  - `FAthleticScreen` - Athletic screen measurements
  - `FProSup` - Pro-Sup test results
  - `FReadinessScreen` - Readiness screen data
  - `FMobility` - Mobility assessments
  - `FProteus` - Proteus test data
  - `FKinematicsPitching` - Pitching kinematics (long format)
  - `FKinematicsHitting` - Hitting kinematics

### App Schema (`prisma/app/schema.prisma`)

Contains:
- `User` - Application user/athlete table (source of truth for UUIDs)

## Database URLs

The database URLs are configured via environment variables:
- `WAREHOUSE_DATABASE_URL` - Warehouse database connection
- `APP_DATABASE_URL` - App database connection

Format: `postgresql://user:password@host:port/database?schema=public`

## Notes

- Prisma uses camelCase for model names and field names in the generated client
- Database column names remain snake_case (mapped via `@map`)
- Relations are defined between fact tables and dimension tables
- Indexes and constraints are preserved from the original SQL schemas

