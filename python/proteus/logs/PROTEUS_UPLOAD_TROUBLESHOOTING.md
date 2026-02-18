# Proteus log upload troubleshooting (since 01/27)

## Summary

The last **successful** data write to the warehouse DB was from the **01/28 run** (which processed the 01/27 export). Since then, runs either fail before reaching the DB (login/download) or the ETL fails when writing to `f_proteus`.

---

## 1. Login / web step failures (most common)

When these happen, **no file is downloaded**, so the ETL never gets new data and nothing is uploaded.

| Error | Dates seen | Likely cause |
|-------|------------|--------------|
| **Timeout loading login page (60s)** | 01/29, 02/05, 02/08, 02/09, 02/12 | Network slow, machine asleep, or Proteus site slow/down |
| **Could not find password input / redirect to user-dashboard** | 01/30, 02/07 | Proteus UI or flow changed; automation lands on wrong page |
| **Could not find continue/next button** | 02/10 | UI change (button text or selector) |
| **Could not find email input** | 02/03 | UI change or page didn’t load |
| **Browser launch timeout (180s)** | 02/04 | System under load or Playwright/Chromium slow |

**What to do**

- Run the job when the machine and network are stable (e.g. not right after wake).
- Manually open https://kiosk.proteusmotion.com/login and confirm the login flow (email → next → password → next).
- If the flow or selectors changed, update `python/proteus/web/login.py` (and download.py if export page changed).
- Consider increasing timeouts in `web/login.py` or running with a visible browser (`PROTEUS_HEADLESS=false`) to debug.

---

## 2. ETL / DB write failures (when login + download succeed)

When login and download succeed (e.g. 02/02, 02/06, 02/11), the ETL can still fail before or during the write to the DB.

### 2a. Duplicate key on `f_proteus` (`f_proteus_pkey`)

- **Error:** `UniqueViolation: duplicate key value violates unique constraint "f_proteus_pkey"`, e.g. `Key (id)=(3187) already exists`.
- **Cause:** The Excel export includes an `id` column (Proteus row id). The ETL was sending that into `f_proteus`, which has its own serial `id`. Re-running the same file or overlapping data produces the same Proteus ids and conflicts with existing rows.
- **Fix:** Before writing to `f_proteus`, **drop the `id` column** from the DataFrame so the table’s serial generates new IDs. (Done in `etl_proteus.py`.)

### 2b. Athlete update: `COALESCE types text and double precision cannot be matched`

- **Error:** `gender = COALESCE(gender, 'NaN'::flo...` (or similar) when creating/updating athletes for Proteus.
- **Cause:** The Excel `sex` column can be missing or NaN. Pandas gives `float('nan')`, which is passed as `gender`. In SQL, that becomes a double precision value, but `analytics.d_athletes.gender` is text → type mismatch in COALESCE.
- **Fix:** In the Proteus ETL, **normalize** `sex` before calling `get_or_create_athlete`: if `pd.isna(sex)` or not a string, pass `None` for `gender`. (Done in `etl_proteus.py`.)

### 2c. Empty or no-data files

- **Error:** `File is empty` or `No rows with valid athlete_uuid after mapping`.
- **Cause:** Export for that date has no rows, or all rows are dropped (e.g. no baseball/softball, or all athletes failed the athlete step).
- **Action:** No code change needed; those files are skipped. If you expect data, check the export in the UI and the filters (sport, athlete mapping) in the ETL.

---

## 3. Date range selector (checkbox) warning

- **Log:** `Error setting date range: Input of type "checkbox" cannot be filled` when filling the end date.
- **Cause:** The “end date” selector on the export page is matching a checkbox instead of the end-date input.
- **Impact:** Download still continues (date range may be wrong). If you see wrong date ranges, fix the selectors in `python/proteus/web/download.py` (e.g. more specific selectors for start/end date inputs).

---

## 4. Quick reference: recent run outcomes

| Date   | Login | Download | ETL ran | DB write |
|--------|-------|----------|---------|----------|
| 01/27  | OK    | OK       | OK      | OK (01/26 export) |
| 01/28  | OK    | OK       | OK      | OK (01/27 export) – **last successful upload** |
| 01/29  | Timeout | -     | -       | - |
| 01/30  | No password field | - | - | - |
| 02/02  | OK    | OK       | OK      | UniqueViolation |
| 02/03  | No email field | - | - | - |
| 02/04  | Browser timeout | - | - | - |
| 02/05  | Timeout | -     | -       | - |
| 02/06  | OK    | OK       | OK      | UniqueViolation |
| 02/07  | No password field | - | - | - |
| 02/08  | Timeout | -     | -       | - |
| 02/09  | Timeout | -     | -       | - |
| 02/10  | No next button | - | - | - |
| 02/11  | OK    | OK       | OK      | UniqueViolation |
| 02/12  | Timeout | -     | -       | - |

After applying the ETL fixes (drop `id`, normalize gender), the next run that gets past login and download should be able to write to the DB without UniqueViolation and without the athlete gender COALESCE error.

---

## 5. Backfilling failed dates

To pull and insert data for dates that had failed runs (e.g. 2026-01-29 through 2026-02-11), run with an explicit date range. One run will download that range from Proteus (one export file) and ETL will process it.

From the project root (with venv active):

```bash
python python/proteus/main.py --start 2026-01-29 --end 2026-02-11
```

Or for a single day:

```bash
python python/proteus/main.py --start 2026-02-10 --end 2026-02-10
```

You can also set env vars instead of CLI args:

- `PROTEUS_START_DATE=2026-01-29`
- `PROTEUS_END_DATE=2026-02-11`
