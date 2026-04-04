#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scrape_rnpdno_single_status.py
==============================

Single-status scraper for parallel execution.

Usage:
    python scrape_rnpdno_single_status.py --status 0   # Total
    python scrape_rnpdno_single_status.py --status 7   # Desaparecidas y No Localizadas
    python scrape_rnpdno_single_status.py --status 2   # Localizadas con Vida
    python scrape_rnpdno_single_status.py --status 3   # Localizadas sin Vida

Run in 4 terminals simultaneously for ~4x speedup.
"""

import argparse
import json
import time
import logging
import calendar
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os

load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "rnpdno"
LOG_DIR = PROJECT_ROOT / "logs" / "scrapers"

BASE_URL = "https://versionpublicarnpdno.segob.gob.mx/SocioDemografico/TablaDetalle"

STATUS_CONFIG = {
    0: {"name": "total", "titulo": "PERSONAS DESAPARECIDAS, NO LOCALIZADAS Y LOCALIZADAS"},
    7: {"name": "desaparecidas_no_localizadas", "titulo": "PERSONAS DESAPARECIDAS Y NO LOCALIZADAS"},
    2: {"name": "localizadas_con_vida", "titulo": "PERSONAS LOCALIZADAS CON VIDA"},
    3: {"name": "localizadas_sin_vida", "titulo": "PERSONAS LOCALIZADAS SIN VIDA"},
}

YEAR_START = 2015
YEAR_END = 2025

ESTADOS = {
    1: "Aguascalientes", 2: "Baja California", 3: "Baja California Sur",
    4: "Campeche", 5: "Coahuila", 6: "Colima", 7: "Chiapas", 8: "Chihuahua",
    9: "Ciudad de México", 10: "Durango", 11: "Guanajuato", 12: "Guerrero",
    13: "Hidalgo", 14: "Jalisco", 15: "México", 16: "Michoacán", 17: "Morelos",
    18: "Nayarit", 19: "Nuevo León", 20: "Oaxaca", 21: "Puebla", 22: "Querétaro",
    23: "Quintana Roo", 24: "San Luis Potosí", 25: "Sinaloa", 26: "Sonora",
    27: "Tabasco", 28: "Tamaulipas", 29: "Tlaxcala", 30: "Veracruz",
    31: "Yucatán", 32: "Zacatecas", 33:"Se desconoce"
}

# Cookie is loaded from .env (BROWSER_COOKIE=...) — never hardcode here
BROWSER_COOKIE = os.getenv("BROWSER_COOKIE", "")


def setup_logging(status_name: str):
    """Setup logging per status."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{status_name}] %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / f"scrape_{status_name}.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def build_payload(id_estado: int, year: int, month: int, id_estatus: int) -> dict:
    last_day = calendar.monthrange(year, month)[1]
    titulo = STATUS_CONFIG.get(id_estatus, {}).get("titulo", "TEST")
    
    return {
        "titulo": titulo,
        "subtitulo": "POR MUNICIPIO",
        "idEstatusVictima": str(id_estatus),
        "fechaInicio": f"{year:04d}-{month:02d}-01",
        "fechaFin": f"{year:04d}-{month:02d}-{last_day:02d}",
        "idEstado": str(id_estado),
        "idMunicipio": "0",
        "mostrarFechaNula": "0",
        "idColonia": "0",
        "idNacionalidad": "0",
        "edadInicio": "",
        "edadFin": "",
        "mostrarEdadNula": "0",
        "idHipotesis": "",
        "idMedioConocimiento": "",
        "idCircunstancia": "",
        "tieneDiscapacidad": "",
        "idTipoDiscapacidad": "0",
        "idEtnia": "0",
        "idLengua": "0",
        "idReligion": "",
        "esMigrante": "",
        "idEstatusMigratorio": "0",
        "esLgbttti": "",
        "esServidorPublico": "",
        "esDefensorDH": "",
        "esPeriodista": "",
        "esSindicalista": "",
        "esONG": "",
        "idHipotesisNoLocalizacion": "0",
        "idDelito": "0",
        "TipoDetalle": 3,
    }


def create_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json; charset=UTF-8",
        "origin": "https://versionpublicarnpdno.segob.gob.mx",
        "referer": "https://versionpublicarnpdno.segob.gob.mx/Dashboard/Sociodemografico",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
        "Cookie": BROWSER_COOKIE,
    })
    return s


def safe_post(session, payload, id_estado, year, month, id_estatus, logger, retries=4):
    """Fewer retries for parallel execution."""
    backoff = 0.5
    
    for i in range(retries):
        try:
            resp = session.post(BASE_URL, data=json.dumps(payload), timeout=45)
            
            if resp.status_code == 200 and len(resp.content) > 0:
                try:
                    js = resp.json()
                    if js.get("Html"):
                        return resp
                except json.JSONDecodeError:
                    pass
            
            logger.warning(f"Retry {i+1}/{retries} - estado {id_estado} {year}-{month:02d}")
            
        except requests.Timeout:
            logger.warning(f"Timeout - estado {id_estado}")
        except requests.RequestException as e:
            logger.warning(f"Error: {e}")
        
        time.sleep(backoff)
        backoff *= 1.5
    
    return None


def extract_rows(html: str, id_estado: int, year: int, month: int, id_estatus: int) -> list:
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        
        def to_int(x):
            x = x.strip().replace(",", "")
            return int(x) if x.isdigit() else 0
        
        municipio = tds[0].get_text(strip=True)
        hombres = to_int(tds[1].get_text())
        mujeres = to_int(tds[2].get_text())
        indeterminado = to_int(tds[3].get_text())
        
        if hombres == 0 and mujeres == 0 and indeterminado == 0:
            continue
        
        rows.append({
            "municipio": municipio,
            "hombres": hombres,
            "mujeres": mujeres,
            "indeterminado": indeterminado,
            "total_personas": hombres + mujeres + indeterminado,
            "id_estado": id_estado,
            "estado": ESTADOS.get(id_estado, ""),
            "anio": year,
            "mes": month,
            "id_estatus_victima": id_estatus,
        })
    
    return rows


def fetch_month(session, year, month, id_estatus, logger) -> pd.DataFrame:
    all_rows = []
    
    for id_estado in range(1, 34):
        payload = build_payload(id_estado, year, month, id_estatus)
        resp = safe_post(session, payload, id_estado, year, month, id_estatus, logger)
        
        if resp is None:
            continue
        
        try:
            js = resp.json()
            html = js.get("Html", "")
            if html:
                rows = extract_rows(html, id_estado, year, month, id_estatus)
                all_rows.extend(rows)
        except:
            continue
        
        time.sleep(0.5)  # Faster for parallel
    
    return pd.DataFrame(all_rows)


def scrape_single_status(id_estatus: int, years: Optional[list] = None, months: Optional[list] = None):
    config = STATUS_CONFIG[id_estatus]
    status_name = config["name"]
    output_dir = RAW_DIR / status_name
    output_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_logging(status_name)

    year_range = years if years else list(range(YEAR_START, YEAR_END + 1))
    month_range = months if months else list(range(1, 13))

    logger.info(f"=" * 50)
    logger.info(f"SCRAPING: {status_name} (id={id_estatus})")
    logger.info(f"  Years: {year_range}, Months: {month_range}")
    logger.info(f"=" * 50)

    session = create_session()

    for year in year_range:
        for month in month_range:
            out_path = output_dir / f"rnpdno_{status_name}_{year}_{month:02d}.csv"
            
            # Skip existing non-empty files
            if out_path.exists() and out_path.stat().st_size > 0:
                logger.info(f"Skip: {year}-{month:02d}")
                continue
            
            logger.info(f"Downloading: {year}-{month:02d}...")
            
            df = fetch_month(session, year, month, id_estatus, logger)
            
            if df.empty:
                logger.warning(f"Empty: {year}-{month:02d}")
                # DON'T create empty file - will retry on next run
            else:
                df.to_csv(out_path, index=False)
                logger.info(f"Saved: {out_path.name} ({len(df)} rows)")
            
            time.sleep(1.0)
    
    # Consolidate
    logger.info("Consolidating...")
    consolidated_dir = RAW_DIR / "consolidated"
    consolidated_dir.mkdir(parents=True, exist_ok=True)
    
    csv_files = sorted(output_dir.glob(f"rnpdno_{status_name}_*.csv"))
    dfs = []
    for fp in csv_files:
        if fp.stat().st_size > 0:
            try:
                dfs.append(pd.read_csv(fp))
            except:
                pass
    
    if dfs:
        df_all = pd.concat(dfs, ignore_index=True)
        out_parquet = consolidated_dir / f"rnpdno_{status_name}_{YEAR_START}_{YEAR_END}.parquet"
        df_all.to_parquet(out_parquet, index=False, compression="snappy")
        logger.info(f"Consolidated: {out_parquet.name} ({len(df_all):,} rows)")
    
    logger.info("DONE")


def main():
    parser = argparse.ArgumentParser(description="RNPDNO Single-Status Scraper")
    parser.add_argument("--status", type=int, required=True, choices=[0, 2, 3, 7],
                        help="Status ID: 0=Total, 7=Desap+NoLoc, 2=LocConVida, 3=LocSinVida")
    parser.add_argument("--years", type=int, nargs="+", default=None,
                        help="Specific years to scrape (default: all)")
    parser.add_argument("--months", type=int, nargs="+", default=None,
                        help="Specific months to scrape (default: all)")
    args = parser.parse_args()

    scrape_single_status(args.status, years=args.years, months=args.months)


if __name__ == "__main__":
    main()
