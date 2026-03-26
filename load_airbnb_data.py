import pandas as pd
import numpy as np
from pathlib import Path
from typing import Union
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway

load_dotenv()

SCRAPPING_DIR = Path("scraping/downloads")
OUTPUT_DIR = Path("data/cleaned")

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://localhost:9091")
JOB_NAME = "airbnb_scraping"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def get_csv_files_by_type():
    files = list(SCRAPPING_DIR.glob("*.csv"))
    return {
        "listings": [f for f in files if "Listings_Data" in f.name],
        "reviews": [f for f in files if "Reviews_Data" in f.name],
        "past_calendar": [f for f in files if "Past_Calendar_Rates" in f.name],
        "future_calendar": [f for f in files if "Future_Calendar_Rates" in f.name],
    }

def get_total_csv_files():
    return len(list(SCRAPPING_DIR.glob("*.csv")))

def load_and_deduplicate(csv_files, key_column: Union[str, list[str]] = "listing_id"):
    dfs = []
    for f in csv_files:
        try:
            df = pd.read_csv(f, on_bad_lines='skip')
            df["source_file"] = f.name
            dfs.append(df)
        except Exception as e:
            print(f"  Erreur sur {f.name}: {e}")
            continue
    
    if not dfs:
        return pd.DataFrame()
    
    combined = pd.concat(dfs, ignore_index=True)
    original_count = len(combined)
    
    combined = combined.drop_duplicates(subset=key_column, keep="last")
    deduplicated_count = len(combined)
    
    print(f"  Fichiers: {len(csv_files)}")
    print(f"  Lignes totales: {original_count}")
    print(f"  Après dédoublonnage: {deduplicated_count}")
    print(f"  Doublons supprimés: {original_count - deduplicated_count}")
    
    return combined

def load_listings():
    files = get_csv_files_by_type()["listings"]
    print("\n=== LISTINGS ===")
    return load_and_deduplicate(files, key_column="listing_id")

def load_reviews():
    files = get_csv_files_by_type()["reviews"]
    print("\n=== REVIEWS ===")
    return load_and_deduplicate(files, key_column=["listing_id", "date"])

def load_past_calendar():
    files = get_csv_files_by_type()["past_calendar"]
    print("\n=== PAST CALENDAR RATES ===")
    return load_and_deduplicate(files, key_column=["listing_id", "date"])

def load_future_calendar():
    files = get_csv_files_by_type()["future_calendar"]
    print("\n=== FUTURE CALENDAR RATES ===")
    return load_and_deduplicate(files, key_column=["listing_id", "date"])

def save_cleaned_data(listings_df, reviews_df, past_calendar_df, future_calendar_df):
    listings_df.to_csv(OUTPUT_DIR / "listings_cleaned.csv", index=False)
    reviews_df.to_csv(OUTPUT_DIR / "reviews_cleaned.csv", index=False)
    past_calendar_df.to_csv(OUTPUT_DIR / "past_calendar_cleaned.csv", index=False)
    future_calendar_df.to_csv(OUTPUT_DIR / "future_calendar_cleaned.csv", index=False)
    print(f"\nDonnées sauvegardées dans {OUTPUT_DIR}/")

def load_to_postgres():
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    database = os.getenv("POSTGRES_DB")
    
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
    
    print("\n=== INSERTION EN BASE PostgreSQL ===")
    
    listings_df.to_sql("listings", engine, if_exists="replace", index=False)
    print(f"  listings: {len(listings_df)} lignes insérées")
    
    reviews_df.to_sql("reviews", engine, if_exists="replace", index=False)
    print(f"  reviews: {len(reviews_df)} lignes insérées")
    
    past_calendar_df.to_sql("past_calendar", engine, if_exists="replace", index=False)
    print(f"  past_calendar: {len(past_calendar_df)} lignes insérées")
    
    future_calendar_df.to_sql("future_calendar", engine, if_exists="replace", index=False)
    print(f"  future_calendar: {len(future_calendar_df)} lignes insérées")
    
    engine.dispose()
    print(f"\nDonnées insérées dans PostgreSQL: {database}@{host}:{port}")

def send_prometheus_metrics(total_csv_files, listings_count, reviews_count, past_calendar_count, future_calendar_count):
    registry = CollectorRegistry()
    
    csv_files_collected = Gauge(
        'scraping_csv_files_collected',
        'Nombre de fichiers CSV collectés',
        registry=registry
    )
    
    data_loaded = Gauge(
        'scraping_data_loaded_total',
        'Nombre de lignes chargées en base par type',
        ['table'],
        registry=registry
    )
    
    scraping_success = Gauge(
        'scraping_success',
        'Indique si le scraping est terminé avec succès',
        registry=registry
    )
    
    csv_files_collected.set(total_csv_files)
    data_loaded.labels(table='listings').set(listings_count)
    data_loaded.labels(table='reviews').set(reviews_count)
    data_loaded.labels(table='past_calendar').set(past_calendar_count)
    data_loaded.labels(table='future_calendar').set(future_calendar_count)
    scraping_success.set(1)
    
    try:
        push_to_gateway(PUSHGATEWAY_URL, job=JOB_NAME, registry=registry)
        print(f"\nMétriques envoyées à Prometheus via Pushgateway: {PUSHGATEWAY_URL}")
    except Exception as e:
        print(f"\nErreur envoi métriques Prometheus: {e}")

if __name__ == "__main__":
    print("=" * 50)
    print("CHARGEMENT ET DÉDOUBLONNAGE DES DONNÉES AIRBNB")
    print("=" * 50)
    
    total_csv_files = get_total_csv_files()
    print(f"\nFichiers CSV collectés: {total_csv_files}")
    
    listings_df = load_listings()
    reviews_df = load_reviews()
    past_calendar_df = load_past_calendar()
    future_calendar_df = load_future_calendar()
    
    print("\n" + "=" * 50)
    print("RÉSUMÉ")
    print("=" * 50)
    print(f"Listings: {len(listings_df)} lignes")
    print(f"Reviews: {len(reviews_df)} lignes")
    print(f"Past Calendar: {len(past_calendar_df)} lignes")
    print(f"Future Calendar: {len(future_calendar_df)} lignes")
    
    save_cleaned_data(listings_df, reviews_df, past_calendar_df, future_calendar_df)
    load_to_postgres()
    
    send_prometheus_metrics(
        total_csv_files=total_csv_files,
        listings_count=len(listings_df),
        reviews_count=len(reviews_df),
        past_calendar_count=len(past_calendar_df),
        future_calendar_count=len(future_calendar_df)
    )
    
    print("\n" + "=" * 50)
    print("FONCTIONS DISPONIBLES")
    print("=" * 50)
    print("- listings_df, reviews_df, past_calendar_df, future_calendar_df (DataFrames)")
    print("- save_cleaned_data() - sauvegarde les CSV dédoublonnés")
    print("- load_to_postgres() - charge tout en PostgreSQL")
    print("- send_prometheus_metrics() - envoie métriques à Prometheus")
