from mobility_data_analyzer import MobilityDataAnalyzer


analyzer = MobilityDataAnalyzer(
    raw_dir="data/raw",
    output_path="data/cleaned/yellow_tripdata_cleaned.csv"
)

(
    analyzer
    .load_and_merge()
    .clean()
    .engineer_features()
    .save()
)