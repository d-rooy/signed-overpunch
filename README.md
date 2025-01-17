# **Signed Overpunch Data Converter**

## **Table of Contents**
1. [Overview](#overview)
2. [Features](#features)
3. [File Structure](#file-structure)
4. [Usage](#usage)
5. [Example](#example)
6. [Future Enhancements](#future-enhancements)

---

## **Overview**
This project is a Python and PySpark-based pipeline designed to decode and process legacy fixed-width files that use signed overpunch characters for numeric data. Signed overpunch is a compact encoding technique often found in mainframe systems and legacy healthcare data, particularly in **prescription benefit manager (PBM) datasets**. The project transforms these legacy data formats into modern, queryable formats like Delta Lake or Parquet, while showcasing end-to-end data engineering skills.

---

## **Features**
- **Signed Overpunch Decoding**:
  - Converts numeric fields with signed overpunch characters into standard numeric values.
  - Handles both positive and negative encodings.
- **PySpark Data Pipeline**:
  - Processes large fixed-width files efficiently using PySpark.
  - Includes user-defined functions (UDFs) for seamless transformations.
- **Delta Lake Integration**:
  - Saves processed data into Delta Lake tables for scalable storage and querying.
- **SQL Analytics**:
  - Provides SQL queries for aggregating and analyzing the cleaned data.
- **Reusability**:
  - Core decoding logic implemented as a standalone Python function.

---

## **File Structure**
```
SignedOverpunchConverter/
├── notebooks/
│   ├── 01_data_ingestion.py      # Ingests fixed-width data into PySpark
│   ├── 02_overpunch_decoder.py   # Decodes signed overpunch fields using a PySpark UDF
│   ├── 03_save_to_delta.py       # Saves cleaned data to Delta Lake
│   ├── 04_sql_queries.py         # SQL queries for analysis
├── src/
│   ├── decode_overpunch.py       # Core Python function for signed overpunch decoding
│   ├── process_pipeline.py       # Main pipeline combining all steps
├── data/
│   ├── input_data.txt            # Sample fixed-width input file
│   ├── delta_table/              # Output directory for Delta Lake files
├── README.md                     # Project overview
├── requirements.txt              # Python dependencies
```

---

## **Usage**

#### 1. Upload Your Input File
- Path: `/dbfs/FileStore/tables/input_data.txt`

#### 2. Run the Pipeline
1. Open the Databricks workspace.
2. Execute the notebooks in order:
   - `01_data_ingestion.py`: Load and preview the fixed-width data.
   - `02_overpunch_decoder.py`: Decode signed overpunch fields.
   - `03_save_to_delta.py`: Save the cleaned data into Delta Lake.
   - `04_sql_queries.py`: Analyze the data using SQL queries.

#### 3. Query and Analyze
- Use SQL scripts in `04_sql_queries.py` to aggregate and explore the processed data.

---

## **Example**

#### Input Data (Fixed-Width Format):
```
12345A|SampleName|12345C
67890B|AnotherName|67890L
```

#### Output Data (Delta Lake Table):
| col1   | col2         | decoded_value |
|--------|--------------|---------------|
| 12345  | SampleName   | 12345.3       |
| 67890  | AnotherName  | -67890.3      |

---

## **Future Enhancements**
- Add support for custom fixed-width layouts via configuration files.
- Enable integration with cloud storage (e.g., AWS S3, Azure Blob).
- Include dashboards for data visualization.

