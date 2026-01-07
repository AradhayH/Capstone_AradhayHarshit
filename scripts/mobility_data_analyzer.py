import pandas as pd
import numpy as np


class MobilityDataAnalyzer:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = None

    def load_data(self):
        self.df = pd.read_csv(self.file_path)
        print(f"Data loaded: {self.df.shape}")
        return self.df

    def clean_data(self):
        df = self.df.copy()
        df["tpep_pickup_datetime"] = pd.to_datetime(
            df["tpep_pickup_datetime"], errors="coerce"
        )
        df["tpep_dropoff_datetime"] = pd.to_datetime(
            df["tpep_dropoff_datetime"], errors="coerce"
        )

        df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

        df = df[df["passenger_count"] > 0]

        df = df[df["trip_distance"] > 0]

        money_cols = [
            "fare_amount", "extra", "mta_tax", "tip_amount",
            "tolls_amount", "improvement_surcharge", "total_amount"
        ]

        for col in money_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=money_cols)

        self.df = df
        print(f"Data cleaned: {self.df.shape}")
        return self.df

    def feature_engineering(self):
        df = self.df.copy()


        df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
        df["pickup_day"] = df["tpep_pickup_datetime"].dt.day_name()
        df["pickup_month"] = df["tpep_pickup_datetime"].dt.month
        df["pickup_quarter"] = df["tpep_pickup_datetime"].dt.quarter

        df["trip_duration_min"] = (
            (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
            .dt.total_seconds() / 60
        )

        df["revenue_per_mile"] = df["total_amount"] / df["trip_distance"]

        df["tip_percentage"] = np.where(
            df["fare_amount"] > 0,
            (df["tip_amount"] / df["fare_amount"]) * 100,
            0
        )

        self.df = df
        print("Feature engineering completed")
        return self.df

    def export_clean_data(self, output_path):
        self.df.to_csv(output_path, index=False)
        print(f"Clean data exported to {output_path}")
