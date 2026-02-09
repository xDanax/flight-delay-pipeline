import pandas as pd
from pathlib import Path

INPUT_PATH = Path("C:/Users/User/OneDrive/flight-delay-pipeline/data/processed")
OUTPUT_PATH = Path("C:/Users/User/OneDrive/flight-delay-pipeline/data/processed")

def clean():
    latest_file = sorted(INPUT_PATH.glob("flights_ingested_*.csv"))[-1]
    df = pd.read_csv(latest_file)

    df["FL_DATE"] = pd.to_datetime(df["FL_DATE"], errors="coerce")

    for col in ["DEP_DELAY", "ARR_DELAY", "DISTANCE"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["ORIGIN_CITY_NAME", "DEST_CITY_NAME"]:
        df[col] = df[col].str.strip().str.title()

    df = df.drop_duplicates()

    output_file = OUTPUT_PATH / "flights_cleaned.csv"
    df.to_csv(output_file, index=False)

    print(f"Cleaned data saved to {output_file}")
    print("Rows:", len(df))

    return df

if __name__ == "__main__":
    clean()

def count_arr_delay_nulls(df):
    """
    Counts ARR_DELAY nulls and separates cancelled vs non-cancelled flights.
    """
    total_nulls = df["ARR_DELAY"].isna().sum()
    cancelled_nulls = df.loc[
        (df["ARR_DELAY"].isna()) & (df["CANCELLED"] == 1)
    ].shape[0]

    non_cancelled_nulls = total_nulls - cancelled_nulls
    null_stats = count_arr_delay_nulls(df)
    print(null_stats)

    return {
        "total_nulls": total_nulls,
        "cancelled_flights": cancelled_nulls,
        "non_cancelled_flights": non_cancelled_nulls
    }


