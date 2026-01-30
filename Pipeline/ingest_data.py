#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

def ingest_zones(pg_user, pg_pass, pg_host, pg_db, pg_port, target_table='zones'):
    """Ingest NYC taxi zones lookup."""
    print("📡 Streaming zones data...")
    zones_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    
    connection_string = f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    engine = create_engine(connection_string)
    
    # Zones are tiny (~263 rows), no chunking needed
    # We use storage_options just in case of 403 errors
    df_zones = pd.read_csv(zones_url, storage_options={'User-Agent': 'Pandas/2.0'})
    
    df_zones.to_sql(name=target_table, con=engine, if_exists='replace', index=False)
    print("✅ Zones table loaded!")
    # REMOVED: ingest_zones(...) <--- This was causing infinite recursion


def run(
    pg_user: str = 'root',
    pg_pass: str = 'root',
    pg_host: str = 'localhost',
    pg_db: str = 'ny_taxi',
    pg_port: str = '5432',
    year: int = 2021,
    month: int = 1,
    target_table: str = 'yellow_taxi_data',
    chunksize: int = 100000,
):
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}yellow_tripdata_{year:04d}-{month:02d}.csv.gz'
    
    print(f"📡 Establishing stream to {url}...")

    connection_string = f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    engine = create_engine(connection_string)

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
        storage_options={'User-Agent': 'Pandas/2.0'}
    )

    first = True

    for df_chunk in tqdm(df_iter, desc="Ingesting trips"):
        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace', index=False)
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists='append', index=False)

    print("✅ Trips ingestion finished!")


@click.command()
@click.option('--pg-user', default='root', help='Postgres user name')
@click.option('--pg-pass', default='root', help='Postgres password')
@click.option('--pg-host', default='localhost', help='Postgres host')
@click.option('--pg-db', default='ny_taxi', help='Postgres database name')
@click.option('--pg-port', default='5432', help='Postgres port')
@click.option('--year', default=2021, type=int, help='Data year')
@click.option('--month', default=1, type=int, help='Data month')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='CSV chunksize for ingestion')
def main(pg_user, pg_pass, pg_host, pg_db, pg_port, year, month, target_table, chunksize):
    """CLI entrypoint for data ingestion."""
    
    # 1. Ingest Trips
    run(
        pg_user=pg_user,
        pg_pass=pg_pass,
        pg_host=pg_host,
        pg_db=pg_db,
        pg_port=pg_port,
        year=year,
        month=month,
        target_table=target_table,
        chunksize=chunksize,
    )

    # 2. Ingest Zones (This was missing!)
    ingest_zones(
        pg_user=pg_user,
        pg_pass=pg_pass,
        pg_host=pg_host,
        pg_db=pg_db,
        pg_port=pg_port
    )

if __name__ == '__main__':
    main()