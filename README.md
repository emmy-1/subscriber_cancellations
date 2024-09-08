
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

### Database connection.py

## Database Connection Script

The `Database connection.py` file is responsible for establishing connections to a SQLite database and performing various data transformation tasks. Below is a breakdown of its key components and functionalities:

### Key Functionalities

1. **Environment Setup**
   - Loads environment variables from a `.env` file using the `dotenv` library.
   - Sets the working directory based on the `WORKING_DIR` environment variable.
   - Retrieves the Azure storage account key from environment variables for potential use in data storage.

2. **Database Connection Functions**
   - **`change_directory(file_path)`**: Changes the current working directory to the specified file path. It handles exceptions and returns the current directory or `None` if an error occurs.
   - **`create_connection(db_name)`**: Creates a connection to the specified SQLite database. It returns the connection object or `None` if an error occurs.
   - **`connect_to_database()`**: Connects to the SQLite database named `cademycode.db` and returns the connection object.

3. **Data Reading Functions**
   - **`read_tables(table_name, con_name)`**: Reads a specified table from the database and returns it as a Pandas DataFrame. It handles exceptions and returns an empty DataFrame if an error occurs.

4. **Data Transformation Functions**
   - **`parse_contact_info(df, column_name, char1, char2)`**: Parses JSON-like strings in a specified column and creates new columns in the DataFrame.
   - **`get_missing_rows(df, column)`**: Returns a DataFrame containing rows with missing values in a specified column.
   - **`drop_missing_rows(df, column)`**: Returns a DataFrame with rows that do not have missing values in a specified column.
   - **`concat_into_db(df1, df2)`**: Concatenates two DataFrames along the rows and handles exceptions.
   - **`fill_np_zero(dataset, column_name)`**: Replaces NaN values in a specified column with 0.
   - **`drop_colums(dataset, column_name)`**: Drops a specified column from the DataFrame.
   - **`not_applicable(dataset, career_id, career_name, hours)`**: Adds a new row to the DataFrame with specified values.

5. **Data Processing Logic**
   - The script runs a transformation function `Run_Transfomer(Dataset)` that processes the student data, handling missing values and preparing the final dataset for analysis.
   - It updates the courses and jobs tables with new information.

6. **Data Writing Functions**
   - **`write_to_sql(dataframe, table_name, connection)`**: Writes a DataFrame to a specified SQL table, replacing existing data if necessary.
   - **`write_to_csv(dataframe, path)`**: Writes a DataFrame to a CSV file.

7. **Data Export**
   - The script exports processed DataFrames to both SQL and CSV formats, ensuring that the data is stored and accessible for further analysis.

### Summary
This script serves as a crucial component for managing database connections, reading and transforming data, and exporting the results for analysis. It leverages Pandas for data manipulation and SQLite for data storage, making it a versatile tool for data processing tasks.
