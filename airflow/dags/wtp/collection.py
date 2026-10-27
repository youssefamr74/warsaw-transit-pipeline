"""Gold table with data collection stats per hour, so dashboards never read bronze or silver."""


def build_collection_hourly(lake: str, day: str) -> int:
    """Polls, records, vehicles and ghost share per hour for one UTC day. Idempotent."""
    import duckdb
    from deltalake import write_deltalake

    con = duckdb.connect()
    con.execute("SET extension_directory = '/tmp/duckdb_extensions'")
    con.execute("SET memory_limit = '2GB'")
    con.execute("SET TimeZone = 'UTC'")
    con.execute("INSTALL delta; LOAD delta;")

    bronze = f"delta_scan('{lake}/bronze/vehicle_positions')"
    silver = f"delta_scan('{lake}/silver/vehicle_positions')"

    df = con.execute(f"""
        WITH b AS (
            SELECT date_trunc('hour', kafka_timestamp) AS hour_utc,
                   count(DISTINCT poll_id) AS polls,
                   count(*) AS records,
                   count(DISTINCT "VehicleNumber") AS vehicles_reporting,
                   100.0 * count(*) FILTER (
                       WHERE epoch(TRY_CAST(ingested_at AS TIMESTAMPTZ))
                           - epoch(timezone('Europe/Warsaw', TRY_CAST("Time" AS TIMESTAMP))) >= 120
                   ) / count(*) AS ghost_pct
            FROM {bronze}
            WHERE ingest_date = DATE '{day}'
            GROUP BY 1
        ),
        s AS (
            SELECT date_trunc('hour', ingested_ts) AS hour_utc,
                   count(DISTINCT vehicle_number) AS live_vehicles
            FROM {silver}
            WHERE event_date = DATE '{day}'
            GROUP BY 1
        )
        SELECT DATE '{day}' AS event_date, b.hour_utc, b.polls, b.records,
               b.vehicles_reporting, coalesce(s.live_vehicles, 0) AS live_vehicles,
               round(b.ghost_pct, 2) AS ghost_pct
        FROM b LEFT JOIN s USING (hour_utc)
        ORDER BY hour_utc
    """).df()

    if df.empty:
        return 0

    df["hour_utc"] = df["hour_utc"].astype("datetime64[us, UTC]")
    # Replace only this day, so reruns never duplicate it (same idea as replaceWhere in Spark)
    write_deltalake(
        f"{lake}/gold/collection_hourly", df,
        mode="overwrite", predicate=f"event_date = '{day}'", partition_by=["event_date"],
    )
    return len(df)