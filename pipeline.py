from ingest import ingest
from clean import clean
from transform import city_level_metrics, day_of_week_metrics
from load import load_to_sqlite


def run_pipeline():
    # Ingest
    ingest()

    # Clean
    df_clean = clean()

    # Transform
    city_metrics = city_level_metrics(df_clean)
    dow_metrics = day_of_week_metrics(df_clean)

    # Load
    load_to_sqlite(city_metrics, "agg_city_metrics")
    load_to_sqlite(dow_metrics, "agg_day_of_week_metrics")


if __name__ == "__main__":
    run_pipeline()
