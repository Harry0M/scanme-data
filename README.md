# ScanMe Regulatory Database

This repository hosts the offline regulatory ingredient database for the ScanMe Android app. It aggregates data from WHO/IARC, EFSA, California Proposition 65, and ECHA into a unified SQLite database.

## How It Works

```
GitHub Actions (every Monday) → Downloads CSVs → Python merges → SQLite DB → GitHub Release
                                                                                    ↓
Android App → Checks version.json → Downloads new DB if available → Scans products offline
```

## Running Locally

```bash
cd data_pipeline
pip install -r requirements.txt
python build_database.py
```

Output appears in `data_pipeline/output/`:
- `regulatory_data.db` — The SQLite database
- `version.json` — Version + SHA-256 checksum

## Data Sources

| Source | Type | What It Provides |
|--------|------|-----------------|
| WHO/IARC | Cancer classifications | Group 1/2A/2B/3/4 carcinogen status |
| EFSA OpenFoodTox | Food toxicology | ADI, NOAEL, hazard endpoints |
| CA Proposition 65 | Chemical list | Cancer + reproductive toxicity listings |
| ECHA | Chemical hazards | CLP classifications, STOT data |

## Database Schema

```sql
-- Flat table used by Android Room
CREATE TABLE regulatory_data (
    casNumber TEXT NOT NULL,
    eNumber TEXT,
    canonicalName TEXT NOT NULL,
    source TEXT NOT NULL,
    classification TEXT NOT NULL,
    riskLevel TEXT NOT NULL,
    healthCategories TEXT NOT NULL DEFAULT '[]',
    publicationDate TEXT,
    notes TEXT,
    PRIMARY KEY (casNumber, source)
);
```

## Maintenance Guide

See the [Documentation Site](https://harry0m.github.io/scanme-data/) for:
- Troubleshooting URL mismatches
- Adding new data sources
- Handling schema changes
- Full error resolution guide
