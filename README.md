# Melbourne IoT Sensor Data Analysis

## Project Overview

This project processes IoT sensor data collected from various regions of Melbourne. The data is related to air quality, energy consumption, and traffic conditions. The goal is to compute key metrics such as:
- **Average Air Quality Index** by location
- **Average Energy Consumption** by location
- **Average Traffic Density** by location

The project utilizes **Azure Databricks** for data processing and storage in **Delta tables** and **CSV** formats. The final results help in providing insights into pollution levels, energy consumption trends, and peak traffic times in Melbourne, which can be useful for city planners.

## Data Sources

The data used in this project comes from the following CSV files stored in **Azure Blob Storage**:
- Air Quality Data: [Melbourne_Air_Quality_Data.csv](https://datastorageuxklz.blob.core.windows.net/iot-sensordata/Melbourne_Air_Quality_Data.csv)
- Energy Consumption Data: [Melbourne_Energy_Consumption_Data.csv](https://datastorageuxklz.blob.core.windows.net/iot-sensordata/Melbourne_Energy_Consumption_Data.csv)
- Traffic Data: [Melbourne_Traffic_Data.csv](https://datastorageuxklz.blob.core.windows.net/iot-sensordata/Melbourne_Traffic_Data.csv)

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/Melbourne-IoT-Data-Analysis.git
   cd Melbourne-IoT-Data-Analysis




