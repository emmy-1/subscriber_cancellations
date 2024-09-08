
# Cademycode Subscriber Cancellations Data Pipeline
![Static Badge](https://img.shields.io/badge/TechStack%20-Green)   ![Static Badge](https://img.shields.io/badge/dbt-orange)
![Static Badge](https://img.shields.io/badge/Language-SQL-blue)  ![Static Badge](https://img.shields.io/badge/Language-python-blue)  ![Static Badge](https://img.shields.io/badge/airflow-red)


![Static Badge](https://img.shields.io/badge/Lakehouse-Databricks-red) ![Static Badge](https://img.shields.io/badge/Cloud%20Storage-Azure%20ADLS2-blue)  ![Static Badge](https://img.shields.io/badge/Cloud%20Platform%20-%20Mircosoft%20Azure%20-%20blue) 

## :bulb: Overview
The primary objective of this project is to create a data engineering pipeline that will consistently clean up an untidy database and turn it into a reliable source of data for an analytics team. This project makes use of Databricks, Delta Live Tables, Airflow, and dbt (data build tool) to handle and transform data into a data warehouse located in Databricks. Each file mentioned below has been orchestrated in Databricks and Airflow to run on a daily basis.
## :scroll: Scenario
A mock database of long-term canceled subscribers for Cademycode ( A Educational Company) Will be used. This database is regularly updated from multiple sources and needs to be cleaned and transformed into usable shapes with as little human intervention as possible.
## :bar_chart: Data
The Dataset used will be based on a frictional education company called Cademycode.
## :building_construction: Architecture
![Architecture - page 1 (2)](https://github.com/user-attachments/assets/c711cd82-ab2d-48e1-ab1a-ada3758e2ae8)


## File Descriptions
<details>
    <summary> Database connection.py </summary>
