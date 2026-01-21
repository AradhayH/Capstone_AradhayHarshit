## Intelligent Urban Mobility Analytics & GenAI Insights Platform

# Overview

This project is an end-to-end data engineering and analytics system built on the "January 2015-2016 NYC Yellow Taxi dataset"
For our use case we have picked 20k rows from each file.

It demonstrates how raw, large-scale transportation data can be transformed into:

* Clean, query-optimized datasets
* Business-ready KPIs using SQL
* Scalable analytics workflows
* GenAI-powered natural-language insights for stakeholders

---

# Prerequisites and Requirements
* Python - >= 3.10.*
* Java - JDK 17
* Spark - 3.4.*
Dependencies are listed under Requirements.txt
install using "pip install -r requirements.txt"


# Additional features
* Dataset and Outputs from Spark SQL are converted to Parquet for better storage 
* Additional KPIs are calculated to ensure that the GenAI chatbot has better and wider context awareness


# Replication Methodology
* Create a venv " python -m venv .venv "
* Activate the Virutal environment
* Install requirements.txt "pip install -r requirements.txt
* Create an .env file with "GEMINI_API_KEY="
* Run python Codes/scripts/run_data_ingestion.py to get cleaned data under data/cleaned
* Run the Codes/notebooks/kpi_exploratory_analysis.ipynb notebook to save KPIs under outputs/kpis_output and outputs/visuals
* Run python Codes/run_sql_analytics.py to create a sqlite3 database, upload the data and run queries(and store them in outputs under sqloutputs)
* Run python Codes/pyspark_pipline.py to convert the dataset into parquet format.
* Run streamlit run Codes/genai_assistant/app.py to run the Chatbot UI


# Cleaned Data
outputs/cleaned/yellow_tripdata_cleaned.csv

---

## Performance benefits of Parquet
To handle large-scale datasets (100GB+), this platform transitions from row-based formats (CSV) to Apache Parquet, a columnar storage format.
1. Efficient Columnar Storage: Unlike CSVs, Parquet stores data column-wise. This allows the PySpark ETL to read only the specific columns required for KPI computation, significantly reducing Disk I/O and memory usage.
2. Predicate Pushdown: Parquet files store metadata (min/max values) for data blocks. This enables Spark to "skip" irrelevant data during the read phase, making queries much faster.
3. Advanced Data Compression: Because data in a column is of the same type, Parquet achieves much higher compression ratios than text files. This reduces the storage footprint and accelerates network data transfer.
4. Seamless Schema Evolution: Parquet stores the schema within the file metadata, ensuring data integrity and allowing for easy additions of new features/KPIs without rewriting existing datasets.
5.  Optimized for Distributed Processing: Parquet is designed for parallel processing in Spark clusters, allowing large datasets to be split and processed across multiple nodes simultaneously.


## Scalability Strategy (100GB+ Data)
1. Storage (S3/ADLS/Parquet): Moves from local CSVs to cloud-based object storage using Apache Parquet. This columnar format minimizes I/O by reading only required columns and enables partition pruning for massive datasets.
2. Processing (Spark/Databricks): Leverages distributed computing clusters to handle 100GB+ workloads. Spark’s DAG (Directed Acyclic Graph) optimizes transformations, ensuring efficient execution across multiple worker nodes.
3. Indexing (Vector DB): Uses FAISS or Pinecone to index high-dimensional mobility patterns. This allows the system to perform fast similarity searches to find relevant historical context or anomalies.
4. Retrieval (RAG): Implements Retrieval-Augmented Generation over aggregated mobility metrics. Instead of raw data, the LLM receives precisely retrieved "KPI context," ensuring accurate insights while staying within token limits.

