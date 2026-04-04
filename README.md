# RNPDNO Scraper

Extraction and processing pipeline for Mexico's National Registry of Disappeared and Unlocated Persons (Registro Nacional de Personas Desaparecidas y No Localizadas, RNPDNO).

## Overview

This repository contains the web scraper, cleaning pipeline, and manual override files used to produce the municipality-level disappearance dataset published alongside:

> Escobar, M.A., Reyes-Guzman, G., & Sanchez Ruiz, A. (2026). A municipality-level, monthly dataset of disappearance cases and outcomes in Mexico from the National Registry of Disappeared Persons, 2015–2025. *Data in Brief*. [DOI pending]

**Associated dataset:** [OSF Repository](https://osf.io/5qmwr/)

## Contents

```
├── scripts/
│   ├── scrapers/
│   │   └── scrape_rnpdno_single_status.py   # Web scraper
│   └── processing/
│       ├── clean_pipeline.py                # Cleaning & geo-normalization pipeline
│       ├── normalize.py                     # Text normalization utilities
│       └── generate_figures.py              # Data in Brief overview figures
├── data/
│   ├── manual/                              # Git-tracked manual overrides
│   │   ├── geo_overrides.csv                # Municipality name mappings (35 entries)
│   │   ├── geo_state_corrections.csv        # B1 state corrections (25 entries)
│   │   ├── special_geo_cases.csv            # Sentinel code rules (3 entries)
│   │   └── unknown_municipality_patterns.csv # Unknown patterns (5 entries)
│   ├── external/
│   │   └── ageeml_catalog.csv               # INEGI AGEEML reference (2,478 municipalities)
│   ├── raw/rnpdno/consolidated/             # Input: consolidated Parquet files
│   └── processed/                           # Output: 4 processed CSV files
├── manuscript/
│   ├── dib.tex                              # Data in Brief manuscript
│   └── figures/                             # Generated figures
├── environment.yml                          # Conda environment
└── README.md
```

## Installation

```bash
conda env create -f environment.yml
conda activate rnpdno-scraper
```

## Usage

### 1. Web scraping

The scraper requires an active session cookie from the RNPDNO portal. Set the `BROWSER_COOKIE` environment variable in a `.env` file, then run one instance per status:

```bash
python scripts/scrapers/scrape_rnpdno_single_status.py --status 0   # Total
python scripts/scrapers/scrape_rnpdno_single_status.py --status 7   # Disappeared and not located
python scripts/scrapers/scrape_rnpdno_single_status.py --status 2   # Located alive
python scripts/scrapers/scrape_rnpdno_single_status.py --status 3   # Located deceased
```

Run in 4 terminals simultaneously for ~4x speedup. Each produces a consolidated Parquet file in `data/raw/rnpdno/consolidated/`.

### 2. Processing pipeline

With consolidated Parquet files in place:

```bash
python scripts/processing/clean_pipeline.py
```

This runs the full pipeline:
1. Load consolidated Parquets
2. Text-normalize municipality and state names
3. Build AGEEML geographic reference
4. Apply B1 state corrections (unique-nationwide heuristic)
5. Two-pass geographic join (standard + aggressive normalization)
6. Apply manual overrides
7. Assign sentinel geo-codes (unknown state, missing municipality)
8. Assign unresolved records to state 99
9. Remove duplicate rows
10. Write processed CSVs to `data/processed/`

Output: 4 CSV files with 12-column schema (see manuscript for details).

### 3. Generate figures

```bash
python scripts/processing/generate_figures.py
```

Produces `manuscript/figures/fig1_monthly_trend.pdf` and `manuscript/figures/fig2_state_distribution.pdf`.

## Data

The processed dataset is available on OSF: https://osf.io/5qmwr/

## License

MIT License - see [LICENSE](LICENSE)

## Citation

```bibtex
@article{escobar_2026_rnpdno,
  author = {Escobar, Marco A. and Reyes-Guzman, Gerardo and Sanchez Ruiz, Abraham},
  title = {A municipality-level, monthly dataset of disappearance cases and outcomes in Mexico from the National Registry of Disappeared Persons, 2015--2025},
  journal = {Data in Brief},
  year = {2026},
  note = {DOI pending}
}
```

## Contact

- Marco A. Escobar — Universidad La Salle Bajío, León (marcoescobar@ieee.org)
- Abraham Sanchez Ruiz — Universidad La Salle Bajío, Salamanca
- Gerardo Reyes-Guzman — Benemérita Universidad Autónoma de Puebla
