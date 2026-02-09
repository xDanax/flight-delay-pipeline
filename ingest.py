import pandas as pd
from pathlib import Path
from datetime import datetime

RAW_PATH = Path("C:/Users/User/OneDrive/flight-delay-pipeline/data/raw/flights_raw.csv")
PROCESSED_PATH = Path("C:/Users/User/OneDrive/flight-delay-pipeline/data/processed")

def ingest():
    df = pd.read_csv(RAW_PATH)

    print("Rows:", len(df))
    print("Columns:", list(df.columns))

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = PROCESSED_PATH / f"flights_ingested_{timestamp}.csv"

    df.to_csv(output_file, index=False)
    print(f"Ingested data saved to {output_file}")

    return df

if __name__ == "__main__":
    ingest()
