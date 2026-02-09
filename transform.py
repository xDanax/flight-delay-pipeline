import pandas as pd
from pathlib import Path

INPUT_PATH = Path("C:/Users/User/OneDrive/flight-delay-pipeline/data/processed")
OUTPUT_PATH = Path("C:/Users/User/OneDrive/flight-delay-pipeline/data/processed")


def city_level_metrics(df):
    """
    Aggregates flight performance metrics by origin city.
    Excludes non-cancelled flights with missing arrival delays.
    """

    valid_df = df[
        ~((df["ARR_DELAY"].isna()) & (df["CANCELLED"] == 0))
    ]

    city_metrics = (
        valid_df
        .groupby("ORIGIN_CITY_NAME")
        .agg(
            total_flights=("OP_CARRIER_FL_NUM", "count"),
            avg_dep_delay=("DEP_DELAY", "mean"),
            avg_arr_delay=("ARR_DELAY", "mean"),
            cancellation_rate=("CANCELLED", "mean"),
            diversion_rate=("DIVERTED", "mean"),
            avg_distance=("DISTANCE", "mean")
        )
        .reset_index()
        .rename(columns={"ORIGIN_CITY_NAME": "origin_city"})
        .sort_values("total_flights", ascending=False)
    )

    return city_metrics


def day_of_week_metrics(df):
    """
    Aggregates flight performance metrics by day of week.
    Excludes non-cancelled flights with missing arrival delays.
    """

    df = df.copy()
    df["day_of_week"] = df["FL_DATE"].dt.day_name()

    valid_df = df[
        ~((df["ARR_DELAY"].isna()) & (df["CANCELLED"] == 0))
    ]

    dow_metrics = (
        valid_df
        .groupby("day_of_week")
        .agg(
            total_flights=("OP_CARRIER_FL_NUM", "count"),
            avg_dep_delay=("DEP_DELAY", "mean"),
            avg_arr_delay=("ARR_DELAY", "mean"),
            cancellation_rate=("CANCELLED", "mean"),
            diversion_rate=("DIVERTED", "mean")
        )
        .reset_index()
    )

    weekday_order = [
        "Monday", "Tuesday", "Wednesday",
        "Thursday", "Friday", "Saturday", "Sunday"
    ]

    dow_metrics["day_of_week"] = pd.Categorical(
        dow_metrics["day_of_week"],
        categories=weekday_order,
        ordered=True
    )

    return dow_metrics.sort_values("day_of_week")
