#!/usr/bin/env python3
"""
Regulatory Database Builder for ScanMe.

Downloads bulk data from WHO/IARC, EFSA, CA Prop 65, and ECHA sources,
normalizes using CAS Number as primary merge key, builds a relational SQLite
database, and generates version.json with checksum.

Designed for weekly execution via GitHub Actions.
"""

import hashlib
import json
import logging
import re
import sqlite3
import sys
from datetime import date, datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path(__file__).parent / "output"
DB_FILENAME = "regulatory_data.db"
VERSION_FILENAME = "version.json"

# TODO: Replace placeholder URLs with actual stable download endpoints.
# These URLs may change over time; verify before production use.
SOURCE_URLS = {
    "IARC": (
        "https://monographs.iarc.who.int/wp-content/uploads/2024/"
        "classifications.csv"
    ),
    "EFSA": (
        "https://zenodo.org/records/10070774/files/"
        "OpenFoodTox_dataset.csv"
    ),
    "CA_PROP_65": (
        "https://oehha.ca.gov/media/downloads/proposition-65/"
        "p65chemicalslist.csv"
    ),
    "ECHA": (
        "https://data.europa.eu/api/hub/store/data/"
        "echa-hazard-classification.csv"
    ),
}

# Request settings
REQUEST_TIMEOUT = 60  # seconds
MAX_RETRIES = 3

# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data Download
# ---------------------------------------------------------------------------


def download_source(name: str, url: str) -> str | None:
    """Download a CSV source file. Returns text content or None on failure."""
    logger.info(f"Downloading {name} from {url}")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            logger.info(
                f"  {name} downloaded OK ({len(resp.content)} bytes)"
            )
            return resp.text
        except requests.RequestException as e:
            logger.warning(
                f"  {name} attempt {attempt}/{MAX_RETRIES} failed: {e}"
            )
    logger.error(f"  {name} download failed after {MAX_RETRIES} attempts.")
    return None

# ---------------------------------------------------------------------------
# Source Parsers
# ---------------------------------------------------------------------------

CAS_PATTERN = re.compile(r"^\d{2,7}-\d{2}-\d$")


def _clean_cas(value) -> str | None:
    """Normalize a CAS number string. Returns None if invalid."""
    if pd.isna(value):
        return None
    s = str(value).strip()
    if CAS_PATTERN.match(s):
        return s
    return None


def _clean_text(value) -> str | None:
    """Strip and normalize text fields."""
    if pd.isna(value):
        return None
    s = str(value).strip()
    return s if s else None


def parse_iarc(csv_text: str) -> pd.DataFrame:
    """
    Parse WHO/IARC Monographs classification list.

    Expected columns (may vary): Agent, CAS No, Group, Volume, Year.
    """
    logger.info("Parsing IARC data...")
    try:
        df = pd.read_csv(StringIO(csv_text), dtype=str)
    except Exception as e:
        logger.error(f"  IARC CSV parse error: {e}")
        return pd.DataFrame()

    # Normalize column names to lowercase
    df.columns = [c.strip().lower() for c in df.columns]

    # Map expected columns (handle variations)
    cas_col = next(
        (c for c in df.columns if "cas" in c), None
    )
    agent_col = next(
        (c for c in df.columns if "agent" in c or "name" in c), None
    )
    group_col = next(
        (c for c in df.columns if "group" in c), None
    )
    year_col = next(
        (c for c in df.columns if "year" in c), None
    )

    records = []
    for _, row in df.iterrows():
        cas = _clean_cas(row.get(cas_col) if cas_col else None)
        name = _clean_text(row.get(agent_col) if agent_col else None)
        group = _clean_text(row.get(group_col) if group_col else None)
        year = _clean_text(row.get(year_col) if year_col else None)

        if not name:
            continue

        classification = f"IARC {group}" if group else "IARC Unknown"
        risk_level = _iarc_group_to_risk(group)
        pub_date = f"{year}-01-01" if year else None

        records.append({
            "cas_number": cas,
            "e_number": None,
            "canonical_name": name,
            "source": "WHO_IARC",
            "classification": classification,
            "risk_level": risk_level,
            "health_categories": json.dumps(["Cancer"]),
            "publication_date": pub_date,
            "notes": f"IARC Monographs {group}",
        })

    logger.info(f"  IARC: {len(records)} records parsed.")
    return pd.DataFrame(records)


def _iarc_group_to_risk(group: str | None) -> str:
    """Map IARC group classification to app risk level."""
    if not group:
        return "Moderate"
    g = group.strip().upper()
    if g in ("1", "GROUP 1"):
        return "Critical"
    elif g in ("2A", "GROUP 2A"):
        return "High"
    elif g in ("2B", "GROUP 2B"):
        return "Moderate"
    elif g in ("3", "GROUP 3"):
        return "Low"
    elif g in ("4", "GROUP 4"):
        return "Minimal"
    return "Moderate"


def parse_efsa(csv_text: str) -> pd.DataFrame:
    """
    Parse EFSA OpenFoodTox dataset.

    Expected columns: Substance name, CAS, E number, Hazard,
    Reference point, Year, etc.
    """
    logger.info("Parsing EFSA data...")
    try:
        df = pd.read_csv(StringIO(csv_text), dtype=str, low_memory=False)
    except Exception as e:
        logger.error(f"  EFSA CSV parse error: {e}")
        return pd.DataFrame()

    df.columns = [c.strip().lower() for c in df.columns]

    cas_col = next((c for c in df.columns if "cas" in c), None)
    name_col = next(
        (c for c in df.columns if "substance" in c or "name" in c), None
    )
    e_col = next((c for c in df.columns if "e number" in c or "e_number" in c), None)
    hazard_col = next((c for c in df.columns if "hazard" in c), None)
    year_col = next((c for c in df.columns if "year" in c), None)

    records = []
    for _, row in df.iterrows():
        cas = _clean_cas(row.get(cas_col) if cas_col else None)
        name = _clean_text(row.get(name_col) if name_col else None)
        e_number = _clean_text(row.get(e_col) if e_col else None)
        hazard = _clean_text(row.get(hazard_col) if hazard_col else None)
        year = _clean_text(row.get(year_col) if year_col else None)

        if not name:
            continue

        classification = hazard or "Evaluated"
        risk_level = _efsa_hazard_to_risk(hazard)
        pub_date = f"{year}-01-01" if year else None
        categories = _infer_health_categories(hazard)

        records.append({
            "cas_number": cas,
            "e_number": e_number,
            "canonical_name": name,
            "source": "EFSA",
            "classification": classification,
            "risk_level": risk_level,
            "health_categories": json.dumps(categories),
            "publication_date": pub_date,
            "notes": None,
        })

    logger.info(f"  EFSA: {len(records)} records parsed.")
    return pd.DataFrame(records)


def _efsa_hazard_to_risk(hazard: str | None) -> str:
    """Map EFSA hazard description to app risk level."""
    if not hazard:
        return "Low"
    h = hazard.lower()
    if any(w in h for w in ("carcinogenic", "mutagenic", "toxic to reproduction")):
        return "Critical"
    elif any(w in h for w in ("endocrine", "reprotoxic")):
        return "High"
    elif any(w in h for w in ("irritant", "sensitiser", "sensitizer")):
        return "Moderate"
    elif "not classified" in h:
        return "Minimal"
    return "Low"


def _infer_health_categories(hazard: str | None) -> list[str]:
    """Infer health categories from hazard text."""
    if not hazard:
        return []
    h = hazard.lower()
    categories = []
    mapping = {
        "Cancer": ["carcinogen", "mutagen"],
        "Reproductive Toxicity": ["reproduct", "fertility"],
        "Hormonal Effects": ["endocrine"],
        "Neurotoxicity": ["neurotox"],
        "Liver": ["hepato", "liver"],
        "Kidney": ["nephro", "kidney", "renal"],
        "Respiratory": ["respirat", "inhalat"],
        "Skin": ["skin", "dermal", "sensitis"],
        "Eye": ["eye", "ocular"],
        "Digestive System": ["gastro", "intestin", "digest"],
    }
    for category, keywords in mapping.items():
        if any(kw in h for kw in keywords):
            categories.append(category)
    return categories


def parse_ca_prop65(csv_text: str) -> pd.DataFrame:
    """
    Parse California Proposition 65 chemical list.

    Expected columns: Chemical, CAS No., Type of Toxicity,
    Listing Mechanism, Date Listed, etc.
    """
    logger.info("Parsing CA Prop 65 data...")
    try:
        df = pd.read_csv(StringIO(csv_text), dtype=str)
    except Exception as e:
        logger.error(f"  CA Prop 65 CSV parse error: {e}")
        return pd.DataFrame()

    df.columns = [c.strip().lower() for c in df.columns]

    cas_col = next((c for c in df.columns if "cas" in c), None)
    name_col = next(
        (c for c in df.columns if "chemical" in c or "name" in c), None
    )
    toxicity_col = next(
        (c for c in df.columns if "toxicity" in c or "type" in c), None
    )
    date_col = next((c for c in df.columns if "date" in c), None)

    records = []
    for _, row in df.iterrows():
        cas = _clean_cas(row.get(cas_col) if cas_col else None)
        name = _clean_text(row.get(name_col) if name_col else None)
        toxicity = _clean_text(row.get(toxicity_col) if toxicity_col else None)
        date_listed = _clean_text(row.get(date_col) if date_col else None)

        if not name:
            continue

        classification = f"Prop 65 Listed ({toxicity})" if toxicity else "Prop 65 Listed"
        risk_level = _prop65_toxicity_to_risk(toxicity)
        categories = _prop65_categories(toxicity)
        pub_date = _normalize_date(date_listed)

        records.append({
            "cas_number": cas,
            "e_number": None,
            "canonical_name": name,
            "source": "CA_PROP_65",
            "classification": classification,
            "risk_level": risk_level,
            "health_categories": json.dumps(categories),
            "publication_date": pub_date,
            "notes": f"Toxicity type: {toxicity}" if toxicity else None,
        })

    logger.info(f"  CA Prop 65: {len(records)} records parsed.")
    return pd.DataFrame(records)


def _prop65_toxicity_to_risk(toxicity: str | None) -> str:
    """Map Prop 65 toxicity type to app risk level."""
    if not toxicity:
        return "High"
    t = toxicity.lower()
    if "cancer" in t and "reproductive" in t:
        return "Critical"
    elif "cancer" in t:
        return "Critical"
    elif "reproductive" in t or "developmental" in t:
        return "High"
    return "High"


def _prop65_categories(toxicity: str | None) -> list[str]:
    """Map Prop 65 toxicity type to health categories."""
    if not toxicity:
        return []
    t = toxicity.lower()
    categories = []
    if "cancer" in t:
        categories.append("Cancer")
    if "reproductive" in t or "developmental" in t:
        categories.append("Reproductive Toxicity")
    return categories


def _normalize_date(date_str: str | None) -> str | None:
    """Attempt to normalize a date string to ISO format."""
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y", "%B %d, %Y", "%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_echa(csv_text: str) -> pd.DataFrame:
    """
    Parse ECHA Classification & Labelling data.

    Expected columns: EC Number, CAS Number, Substance Name,
    Hazard Class and Category, Hazard Statement, etc.
    """
    logger.info("Parsing ECHA data...")
    try:
        df = pd.read_csv(StringIO(csv_text), dtype=str, low_memory=False)
    except Exception as e:
        logger.error(f"  ECHA CSV parse error: {e}")
        return pd.DataFrame()

    df.columns = [c.strip().lower() for c in df.columns]

    cas_col = next((c for c in df.columns if "cas" in c), None)
    name_col = next(
        (c for c in df.columns if "substance" in c or "name" in c), None
    )
    hazard_col = next(
        (c for c in df.columns if "hazard class" in c or "classification" in c),
        None,
    )
    statement_col = next(
        (c for c in df.columns if "hazard statement" in c or "statement" in c),
        None,
    )

    records = []
    for _, row in df.iterrows():
        cas = _clean_cas(row.get(cas_col) if cas_col else None)
        name = _clean_text(row.get(name_col) if name_col else None)
        hazard_class = _clean_text(row.get(hazard_col) if hazard_col else None)
        statement = _clean_text(
            row.get(statement_col) if statement_col else None
        )

        if not name:
            continue

        classification = hazard_class or "ECHA Listed"
        risk_level = _echa_to_risk(hazard_class, statement)
        categories = _infer_health_categories(
            f"{hazard_class or ''} {statement or ''}"
        )

        records.append({
            "cas_number": cas,
            "e_number": None,
            "canonical_name": name,
            "source": "ECHA",
            "classification": classification,
            "risk_level": risk_level,
            "health_categories": json.dumps(categories),
            "publication_date": None,
            "notes": statement,
        })

    logger.info(f"  ECHA: {len(records)} records parsed.")
    return pd.DataFrame(records)


def _echa_to_risk(hazard_class: str | None, statement: str | None) -> str:
    """Map ECHA hazard class to app risk level."""
    text = f"{hazard_class or ''} {statement or ''}".lower()
    if any(w in text for w in ("carc.", "muta.", "repr.")):
        return "Critical"
    elif any(w in text for w in ("stot re", "acute tox. 1", "acute tox. 2")):
        return "High"
    elif any(w in text for w in ("stot se", "skin sens.", "acute tox. 3")):
        return "Moderate"
    elif any(w in text for w in ("eye irrit.", "skin irrit.", "acute tox. 4")):
        return "Low"
    return "Low"


# ---------------------------------------------------------------------------
# Data Normalization & Merging
# ---------------------------------------------------------------------------


def normalize_and_merge(
    dataframes: list[pd.DataFrame],
) -> pd.DataFrame:
    """
    Merge all source DataFrames into a single unified DataFrame.

    Merge strategy:
    - Primary key: CAS Number
    - Secondary key: E-Number
    - Fallback: canonical_name (lowercased, stripped)

    Each source row becomes a classification record. Multiple classifications
    per ingredient are preserved (one-to-many).
    """
    if not dataframes:
        logger.warning("No dataframes to merge.")
        return pd.DataFrame()

    combined = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Combined records before normalization: {len(combined)}")

    # Normalize canonical names for fallback matching
    combined["_name_key"] = (
        combined["canonical_name"]
        .str.lower()
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    # Log data quality issues
    _log_data_quality(combined)

    return combined


def _log_data_quality(df: pd.DataFrame) -> None:
    """Log merge conflicts, missing CAS numbers, and duplicates."""
    total = len(df)
    missing_cas = df["cas_number"].isna().sum()
    logger.info(f"Data quality report:")
    logger.info(f"  Total records: {total}")
    logger.info(f"  Missing CAS numbers: {missing_cas} ({100*missing_cas/max(total,1):.1f}%)")

    # Duplicate CAS + source combinations
    has_cas = df[df["cas_number"].notna()]
    dupes = has_cas.duplicated(subset=["cas_number", "source"], keep=False)
    dupe_count = dupes.sum()
    if dupe_count > 0:
        logger.warning(
            f"  Duplicate (CAS, source) entries: {dupe_count}"
        )
        # Log first few duplicates
        dupe_examples = has_cas[dupes].head(10)[
            ["cas_number", "canonical_name", "source"]
        ]
        for _, row in dupe_examples.iterrows():
            logger.warning(
                f"    Duplicate: CAS={row['cas_number']} "
                f"name='{row['canonical_name']}' source={row['source']}"
            )

    # Check for merge conflicts (same CAS, different canonical names)
    if not has_cas.empty:
        name_conflicts = (
            has_cas.groupby("cas_number")["_name_key"]
            .nunique()
            .reset_index()
        )
        conflicts = name_conflicts[name_conflicts["_name_key"] > 1]
        if not conflicts.empty:
            logger.warning(
                f"  CAS numbers with conflicting names: {len(conflicts)}"
            )
            for cas in conflicts["cas_number"].head(5):
                names = has_cas[has_cas["cas_number"] == cas][
                    "canonical_name"
                ].unique()
                logger.warning(
                    f"    CAS {cas}: {list(names[:3])}"
                )


# ---------------------------------------------------------------------------
# SQLite Database Builder
# ---------------------------------------------------------------------------


def build_database(df: pd.DataFrame, db_path: Path) -> None:
    """
    Build relational SQLite database from merged DataFrame.

    Creates:
    - ingredients table (unique ingredients by CAS/E-Number/name)
    - classifications table (one-to-many per ingredient)
    - regulatory_data table (flat table for Room entity compatibility)
    """
    logger.info(f"Building SQLite database at {db_path}")

    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Create schema
    cursor.executescript("""
        CREATE TABLE ingredients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cas_number TEXT UNIQUE,
            e_number TEXT,
            canonical_name TEXT NOT NULL
        );

        CREATE TABLE classifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ingredient_id INTEGER NOT NULL,
            cas_number TEXT NOT NULL,
            source TEXT NOT NULL,
            classification TEXT NOT NULL,
            risk_level TEXT NOT NULL,
            health_categories TEXT,
            publication_date TEXT,
            notes TEXT,
            FOREIGN KEY (ingredient_id) REFERENCES ingredients(id)
        );

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

        CREATE INDEX idx_ingredients_cas ON ingredients(cas_number);
        CREATE INDEX idx_ingredients_e_number ON ingredients(e_number);
        CREATE INDEX idx_ingredients_name ON ingredients(canonical_name);
        CREATE INDEX idx_classifications_ingredient ON classifications(ingredient_id);
        CREATE INDEX idx_classifications_cas ON classifications(cas_number);
        CREATE INDEX idx_regulatory_data_cas ON regulatory_data(casNumber);
        CREATE INDEX idx_regulatory_data_e_number ON regulatory_data(eNumber);
    """)

    # Build unique ingredient registry
    ingredient_map: dict[str, int] = {}  # key -> ingredient_id

    def _get_or_create_ingredient(row) -> int | None:
        """Get existing or create new ingredient. Returns ingredient_id."""
        cas = row.get("cas_number")
        e_num = row.get("e_number")
        name = row.get("canonical_name", "")
        name_key = row.get("_name_key", name.lower().strip())

        # Try CAS first
        if cas and cas in ingredient_map:
            return ingredient_map[cas]
        # Try E-Number
        if e_num and f"e:{e_num}" in ingredient_map:
            return ingredient_map[f"e:{e_num}"]
        # Try name
        if name_key and f"n:{name_key}" in ingredient_map:
            return ingredient_map[f"n:{name_key}"]

        # Create new ingredient
        cursor.execute(
            "INSERT INTO ingredients (cas_number, e_number, canonical_name) "
            "VALUES (?, ?, ?)",
            (cas, e_num, name),
        )
        ing_id = cursor.lastrowid

        # Register all keys
        if cas:
            ingredient_map[cas] = ing_id
        if e_num:
            ingredient_map[f"e:{e_num}"] = ing_id
        if name_key:
            ingredient_map[f"n:{name_key}"] = ing_id

        return ing_id

    # Insert data
    classifications_inserted = 0
    regulatory_inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        ing_id = _get_or_create_ingredient(row)
        if ing_id is None:
            skipped += 1
            continue

        cas = row.get("cas_number") or ""
        source = row.get("source", "")
        classification = row.get("classification", "")
        risk_level = row.get("risk_level", "Low")
        health_cats = row.get("health_categories", "[]")
        pub_date = row.get("publication_date")
        notes = row.get("notes")

        # Insert classification (one-to-many, allows duplicates per source)
        cursor.execute(
            "INSERT INTO classifications "
            "(ingredient_id, cas_number, source, classification, "
            "risk_level, health_categories, publication_date, notes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (ing_id, cas, source, classification, risk_level,
             health_cats, pub_date, notes),
        )
        classifications_inserted += 1

        # Insert into flat regulatory_data table (Room compatible)
        # Use INSERT OR REPLACE to handle duplicates by (casNumber, source)
        if cas:
            e_num = row.get("e_number") or None
            name = row.get("canonical_name", "")
            cursor.execute(
                "INSERT OR REPLACE INTO regulatory_data "
                "(casNumber, eNumber, canonicalName, source, classification, "
                "riskLevel, healthCategories, publicationDate, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (cas, e_num, name, source, classification, risk_level,
                 health_cats, pub_date, notes),
            )
            regulatory_inserted += 1

    conn.commit()
    conn.close()

    logger.info(f"Database built successfully:")
    logger.info(f"  Unique ingredients: {len(ingredient_map)}")
    logger.info(f"  Classifications: {classifications_inserted}")
    logger.info(f"  Regulatory data (flat): {regulatory_inserted}")
    logger.info(f"  Skipped records: {skipped}")


# ---------------------------------------------------------------------------
# Version & Checksum Generation
# ---------------------------------------------------------------------------


def compute_checksum(file_path: Path) -> str:
    """Compute SHA-256 checksum of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def generate_version_json(db_path: Path, output_dir: Path) -> None:
    """Generate version.json with ISO date version and SHA-256 checksum."""
    checksum = compute_checksum(db_path)
    version = date.today().isoformat()

    version_data = {
        "version": version,
        "checksum": f"sha256:{checksum}",
        "url": (
            "https://github.com/<user>/<repo>/releases/download/"
            f"v{version}/{DB_FILENAME}"
        ),
    }

    version_path = output_dir / VERSION_FILENAME
    with open(version_path, "w") as f:
        json.dump(version_data, f, indent=2)

    logger.info(f"version.json generated:")
    logger.info(f"  Version: {version}")
    logger.info(f"  Checksum: sha256:{checksum[:16]}...")
    logger.info(f"  Path: {version_path}")


# ---------------------------------------------------------------------------
# Main Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    """Main pipeline execution."""
    logger.info("=" * 60)
    logger.info("ScanMe Regulatory Database Builder")
    logger.info(f"Run date: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Download all sources
    raw_data: dict[str, str | None] = {}
    for name, url in SOURCE_URLS.items():
        raw_data[name] = download_source(name, url)

    # Step 2: Parse each source
    dataframes: list[pd.DataFrame] = []

    if raw_data.get("IARC"):
        df = parse_iarc(raw_data["IARC"])
        if not df.empty:
            dataframes.append(df)

    if raw_data.get("EFSA"):
        df = parse_efsa(raw_data["EFSA"])
        if not df.empty:
            dataframes.append(df)

    if raw_data.get("CA_PROP_65"):
        df = parse_ca_prop65(raw_data["CA_PROP_65"])
        if not df.empty:
            dataframes.append(df)

    if raw_data.get("ECHA"):
        df = parse_echa(raw_data["ECHA"])
        if not df.empty:
            dataframes.append(df)

    if not dataframes:
        logger.error("No source data could be parsed. Aborting.")
        sys.exit(1)

    # Step 3: Normalize and merge
    merged = normalize_and_merge(dataframes)
    if merged.empty:
        logger.error("Merged dataset is empty. Aborting.")
        sys.exit(1)

    # Step 4: Build SQLite database
    db_path = OUTPUT_DIR / DB_FILENAME
    build_database(merged, db_path)

    # Step 5: Generate version.json
    generate_version_json(db_path, OUTPUT_DIR)

    logger.info("=" * 60)
    logger.info("Pipeline completed successfully.")
    logger.info(f"Output: {OUTPUT_DIR}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
