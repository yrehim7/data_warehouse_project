# 🗃️ Data-Warehouse Project

This project shows a complete data warehousing and analytics solution, starting from building a data warehouse and helping you get useful insights. It's built using the best practices in data engineering and analytics

##  Data Architecture

This project follows the Medallion Architecture, organizing data into three layers:

1. **Bronze** Layer: Stores raw data directly from source systems. In this project, data is ingested from CSV files into a SQL Server database
2. **Silver Layer**: Performs data cleansing, standardization, and normalization, ensure the data is structured and ready for analysis
3. **Gold Layer**: Contains business ready data modeled into star schema, optimized for reporting and analytics

## Project Overview
This project involves:
1. **Data Architecture**: Designing a modern data warehouse using medallion architecture Bronze, Silver, and Gold layers
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse
3. **Data Modeling**: Develop fact and dimension tables optimized for analytical queries
4. **Reporting & Analytics**: Creating SQL-based reports and dashboards for actionable insights

## Project Requirements
#### Building the Data Warehouse ####
#### Objective ####
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making

#### Specifications ####
#### Data Pipeline Overview  ####

- **Data Sources**: Import data from two source systems (**ERP** and **CRM**) provided as CSV files  
- **Data Quality**: Cleaning and resolve data quality issues before analysis
- **Integration**: Combine both sources into a single, user-friendly data model optimized for analytical queries
- **Scope**: Focus on the latest dataset only, historization of data is not required
- **Documentation**: Provide clear documentation of the data model


## BI: Analytics & Reporting
#### Objective ####
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior**  
- **Performance of Product**  
- **Sales Trends**

These insights give the stakeholders with key business metrics, enabling strategic decision making

##  License
This project is licensed under the MIT License. You are free to use, modify, and share this project with proper attribution
