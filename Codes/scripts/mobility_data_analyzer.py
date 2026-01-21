import os
import glob
import pandas as pd


class MobilityDataAnalyzer:
    def __init__(self, raw_dir: str, output_path: str):
        self.raw_dir = raw_dir
        self.output_path = output_path
        self.df = None

    def load_and_merge(self):
        files = glob.glob(os.path.join(self.raw_dir, "*.csv"))
        if not files:
            raise FileNotFoundError("No CSV files found in raw directory")

        frames = []
        for f in files:
            df = pd.read_csv(f)
            df.columns = [c.strip() for c in df.columns]

            # Normalize column naming inconsistency
            if "RatecodeID" in df.columns:
                df.rename(columns={"RatecodeID": "RateCodeID"}, inplace=True)

            df["source_file"] = os.path.basename(f)
            frames.append(df)

        self.df = pd.concat(frames, ignore_index=True)
        return self

    def clean(self):
        df = self.df

        # Parse timestamps
        df["tpep_pickup_datetime"] = pd.to_datetime(
            df["tpep_pickup_datetime"], errors="coerce"
        )
        df["tpep_dropoff_datetime"] = pd.to_datetime(
            df["tpep_dropoff_datetime"], errors="coerce"
        )

        # Drop invalid rows
        df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
        df = df[df["tpep_dropoff_datetime"] >= df["tpep_pickup_datetime"]]
        df = df[df["passenger_count"] > 0]
        df = df[df["trip_distance"] > 0]

        money_cols = [
            "fare_amount", "extra", "mta_tax", "tip_amount",
            "tolls_amount", "total_amount"
        ]

        for c in money_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=money_cols)

        self.df = df
        return self

    def engineer_features(self):
        df = self.df

        df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
        df["pickup_day"] = df["tpep_pickup_datetime"].dt.day_name()
        df["pickup_month"] = df["tpep_pickup_datetime"].dt.month
        df["pickup_quarter"] = df["tpep_pickup_datetime"].dt.quarter

        df["trip_duration_min"] = (
            (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
            .dt.total_seconds() / 60
        )

        df["revenue_per_mile"] = df["total_amount"] / df["trip_distance"]
        df["tip_percentage"] = (df["tip_amount"] / df["fare_amount"]) * 100

        df["is_weekend"] = df["tpep_pickup_datetime"].dt.weekday >= 5
        df["is_peak_hour"] = df["pickup_hour"].between(7, 10) | df["pickup_hour"].between(16, 19)
        df["fare_per_minute"] = df["fare_amount"] / df["trip_duration_min"]

        df = df.replace([float("inf"), float("-inf")], pd.NA)
        df = df.dropna(subset=["trip_duration_min", "fare_per_minute"])

        self.df = df
        return self

    def save(self):
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        self.df.to_csv(self.output_path, index=False)
        print(f"Cleaned dataset saved to: {self.output_path}")




