
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


## :mag_right: File Descriptions
<details>
    <summary> Database connection.py </summary>

Overview
The Database connection.py file is responsible for establishing a connection to an SQLite database, loading environment variables, and performing various data transformation and extraction tasks related to student data.
Key Components
Imports:
The file imports necessary libraries such as os, pandas, numpy, json, dotenv, sqlite3, and logging.
2. Load Environment Variables:
The load_dotenv() function loads environment variables from a .env file, allowing the script to access configuration settings like the working directory and Azure storage account key.
Change Working Directory:
The change_directory(file_path) function attempts to change the current working directory to the specified file_path. If successful, it returns the new working directory; otherwise, it prints an error message.
Database Connection Functions:
create_connection(db_name): This function creates a connection to the specified SQLite database. It returns the connection object or prints an error message if the connection fails.
connect_to_database(): This function calls create_connection() with a hardcoded database name (cademycode.db) and handles any exceptions.
Reading Tables:
read_tables(table_name, con_name): This function reads a specified table from the database and returns it as a Pandas DataFrame. It handles exceptions and returns an empty DataFrame in case of an error.
Data Transformation Functions:
parse_contact_info(df, column_name, char1, char2): Parses JSON-like strings in a specified column and creates new columns based on the parsed data.
get_missing_rows(df, column): Returns a DataFrame containing rows with missing values in a specified column.
drop_missing_rows(df, column): Returns a DataFrame with rows that do not have missing values in a specified column.
concat_into_db(df1, df2): Concatenates two DataFrames along the rows and returns the result.
fill_np_zero(dataset, column_name): Replaces NaN values in a specified column with 0.
drop_colums(dataset, column_name): Drops a specified column from the DataFrame.
not_applicable(dataset, career_id, career_name, hours): Adds a new row to the DataFrame with specified values.
Transformation Logic:
Run_Transfomer(Dataset): This function orchestrates the data transformation process. It applies various transformation functions to clean and prepare the student data, handling missing values and creating a final dataset.
Database Updates:
The script updates the courses and jobs tables by calling the not_applicable() function and dropping duplicates from the jobs DataFrame.
Writing to SQL:
The write_to_sql(dataframe, table_name, connection) function writes a DataFrame to the specified SQL table, replacing existing data if necessary.
Creating Spark DataFrames:
The script creates Spark DataFrames from the Pandas DataFrames for further processing or analysis.
Writing to CSV:
The write_to_csv(dataframe, path) function writes a Spark DataFrame to a specified CSV file path.
12. Copying CSV Files:
The script defines functions to copy CSV files from source to destination paths, iterating over datasets to perform the copy operation.
Summary
Overall, the Database connection.py file serves as a comprehensive script for connecting to an SQLite database, performing data extraction and transformation, and managing the data flow between different formats (Pandas DataFrames, Spark DataFrames, and CSV files). It is designed to facilitate data processing for student-related information, ensuring that the data is clean, structured, and ready for analysis.
