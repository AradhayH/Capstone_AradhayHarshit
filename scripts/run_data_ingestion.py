from mobility_data_analyzer import MobilityDataAnalyzer

INPUT_PATH = "data/raw/yellow_tripdata_2016-01.csv"
OUTPUT_PATH = "data/cleaned/yellow_tripdata_2016-01_cleaned.csv"

analyzer = MobilityDataAnalyzer(INPUT_PATH)

analyzer.load_data()
analyzer.clean_data()
analyzer.feature_engineering()
analyzer.export_clean_data(OUTPUT_PATH)
