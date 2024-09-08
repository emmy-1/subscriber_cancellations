
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


### How to Run the Codebase

#### 1. Set Up Your Environment
- Ensure you have Python installed (preferably version 3.8 or higher).
- Install necessary dependencies using `requirements.txt`:
  ```bash
  pip install -r requirements.txt
  ```

#### 2. Set Up Environment Variables
- Create a `.env` file in the root directory of your project and add the following variables:
  ```
  WORKING_DIR=/path/to/your/working/directory            \\where the WORKING_DIR is path to the `cademycode.db` database
  AZURE_STORAGE_ACCOUNT_KEY=your_azure_storage_account_key
  ```

#### 3. Run the Database Connection Script
- Execute the `Database connection.py` script to establish connections and perform data transformations. please note you will have to change the copy directory so that the file fits your usecase:
  ```bash
  python Database/dev/Database\ connection.py
  ```

#### 4. Run the Airflow DAG
- Start Apache Airflow using the Astronomer CLI:
  ```bash
  astro dev start
  ```
- Access the Airflow UI at `http://localhost:8080/` and log in with the default credentials (`admin` for both username and password).
- Trigger the DAG from the Airflow UI to start the dbt pipeline.

#### 5. Run dbt Models
- Ensure that your dbt project is correctly set up in the `airflow/dags/dbt/subcriber_pipline` directory.
- Run dbt commands to build your models:
  ```bash
  dbt run
  ```

#### 6. Check Logs and Outputs
- Monitor the logs in the Airflow UI to ensure that tasks are running successfully.
- Check the output in your specified Azure storage account or local database for the results of your transformations.

#### 7. Testing
- Run tests to ensure everything is functioning as expected:
  ```bash
  pytest
  ```

By following these steps, you should be able to run the codebase successfully. Make sure to adjust paths and configurations according to your local setup.


## :mag_right: File Descriptions
<details>
    <summary> Database connection.py </summary>

### Database connection.py

## Database Connection Script

The `Database connection.py` file which should be run on Databricks, establishes connections to a SQLite database called `cademycode.db` and performs various data transformation tasks. Below is a breakdown of its key components and functionalities:

### Key Functionalities

1. **Environment Setup**
   - Loads environment variables from a `.env` file using the `dotenv` library.
   - Sets the working directory based on the `WORKING_DIR` environment variable to the location where the `cademycode.db` database is located. 
   - Retrieves the Azure storage account key from environment variables for potential use in data storage.
    

### Instructions

1. **WORKING_DIR**: Replace `/path/to/your/working/directory` with the actual path where your project files are located. For example:
   ```
   WORKING_DIR=/Workspace/Users/your_email@example.com/subscriber_cancellations/Database/dev/
   ```
   **Important**: To access the data, you will need to change the directory to where the `cademycode.db` file is located. In my case, I created a `.env` file and added the directory of where the file is located.

2. **AZURE_STORAGE_ACCOUNT_KEY**: Replace `your_azure_storage_account_key` with your actual Azure Storage Account Key. Ensure that this key is kept confidential and not shared publicly.

### Important Note
- Make sure to add the `.env` file to your `.gitignore` to prevent it from being tracked by Git:
  ```
  # .gitignore
  .env
  ```

By following these steps, you can ensure that your sensitive information remains secure while allowing your project to function correctly.

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


### Note

- The `write_to_csv` function handles the writing process, and you only need to provide the path where you want the CSV file to be saved.
-Spark usually copies the CSV files with random names and files. Every time the script runs, it gives a different name. That's why I created the `copy_to_csv` and `find_csv_file` functions. Their main job is to find any file that ends with .csv in a given path, and then copy the file to another destination.

</details>

<details>
    <summary> Subscriber cancellation dlt.sql </summary>

### DELTA LIVE TABLE

# Explanation of the DLT Live Table File

This file is a Databricks Delta Live Tables (DLT) script that defines a series of streaming tables as part of a Medallion Architecture. The Medallion Architecture typically consists of three layers: Bronze, Silver, and Gold, which represent raw data, cleaned data, and business-level data, respectively.

## Breakdown of the Script

1. **Bronze Tables**:
   - The first section creates and refreshes streaming tables for raw data from CSV files stored in Azure Data Lake Storage (ADLS). 
   - Tables created:
     - `students`
     - `courses`
     - `jobs`
     - `incomplete_students`

2. **Silver Tables**:
   - The second section creates streaming tables that transform the raw data into a more structured format.
   - Each table includes constraints to ensure data quality (e.g., non-null student IDs).
   - Tables created:
     - `sliver_student`
     - `sliver_incomplete_student`
     - `sliver_courses`
     - `sliver_jobs`

3. **Gold Tables**:
   - The final section creates streaming tables that represent the final, business-ready datasets.
   - These tables are derived from the Silver tables and are intended for reporting and analysis.
   - Tables created:
     - `students_database`
     - `courses_database`
     - `jobs_database`
     - `not_enrolled_students_database`

## Key Features
- **Data Quality Constraints**: Each table has constraints to drop rows that violate certain conditions (e.g., null values).
- **Streaming Data**: The use of `CREATE OR REFRESH STREAMING TABLE` indicates that these tables are designed to handle streaming data, allowing for real-time updates.
- **Comments**: Each table creation includes comments that describe the purpose of the table within the Medallion Architecture.
##NOTE
You will need to change the cloud_file directory to the location where you wrote your CSV files in the `Database Connection.py`

</details>


<details>
    <summary> Dbt Models</summary>


# DBT Models Explanation

This repository contains several DBT models related to student performance and course completion in a subscriber pipeline. Below is a brief explanation of each model:

## 1. `course_completion.sql`
- **Path:** `airflow/dags/dbt/subcriber_pipline/models/example/course_completion.sql`
- **Description:** This model calculates whether students have completed their courses based on the time they spent on the course compared to the hours required to complete it. It joins student information with course completion data and outputs a table indicating if each student has completed their course.

## 2. `Subscriber_cancellation_database.sql`
- **Path:** `airflow/dags/dbt/subcriber_pipline/models/example/Subscriber_cancellation_database.sql`
- **Description:** This model aggregates student information, including demographics and course data. It extracts relevant fields from the students and courses databases, providing a comprehensive view of each student, including their age, sex, mailing address, and the number of courses taken.

## 3. `top_performing_student.sql`
- **Path:** `airflow/dags/dbt/subcriber_pipline/models/example/top_performing_student.sql`
- **Description:** This model identifies the top-performing students by calculating the total number of courses taken and the average time spent on courses. It groups the data by student ID and name, ordering the results by average time spent in descending order.

## 4. `demographic_table.sql`
- **Path:** `airflow/dags/dbt/subcriber_pipline/models/example/demographic_table.sql`
- **Description:** This model summarizes student demographics by counting the number of students in each demographic group (age, sex, city, state, zip code). It provides insights into the distribution of students across different demographic categories.

## 5. `no_of_student_per_careerpath.sql`
- **Path:** `airflow/dags/dbt/subcriber_pipline/models/example/no_of_student_per_careerpath.sql`
- **Description:** This model analyzes the number of students enrolled in each career path. It calculates the total number of students, the average number of courses taken, and the average time spent on courses for each career path, providing insights into student engagement across different fields of study.
- 
# DBT Models Explanation

This repository contains several DBT models related to student performance and course completion in a subscriber pipeline. Below is a brief explanation of each model:

## 1. `course_completion.sql`
- **Path:** `airflow/dags/dbt/subcriber_pipline/models/example/course_completion.sql`
- **Description:** This model calculates whether students have completed their courses based on the time they spent on the course compared to the hours required to complete it. It joins student information with course completion data and outputs a table indicating if each student has completed their course.

## 2. `Subscriber_cancellation_database.sql`
- **Path:** `airflow/dags/dbt/subcriber_pipline/models/example/Subscriber_cancellation_database.sql`
- **Description:** This model aggregates student information, including demographics and course data. It extracts relevant fields from the students and courses databases, providing a comprehensive view of each student, including their age, sex, mailing address, and the number of courses taken.

## 3. `top_performing_student.sql`
- **Path:** `airflow/dags/dbt/subcriber_pipline/models/example/top_performing_student.sql`
- **Description:** This model identifies the top-performing students by calculating the total number of courses taken and the average time spent on courses. It groups the data by student ID and name, ordering the results by average time spent in descending order.

## 4. `demographic_table.sql`
- **Path:** `airflow/dags/dbt/subcriber_pipline/models/example/demographic_table.sql`
- **Description:** This model summarizes student demographics by counting the number of students in each demographic group (age, sex, city, state, zip code). It provides insights into the distribution of students across different demographic categories.

## 5. `no_of_student_per_careerpath.sql`
- **Path:** `airflow/dags/dbt/subcriber_pipline/models/example/no_of_student_per_careerpath.sql`
- **Description:** This model analyzes the number of students enrolled in each career path. It calculates the total number of students, the average number of courses taken, and the average time spent on courses for each career path, providing insights into student engagement across different fields of study.

## Configuration Requirements

Before running the DBT models, ensure that you have configured DBT and Databricks properly. Here are some resources to help you with the setup:

1. **Install DBT Core:** [Installation Overview](https://docs.getdbt.com/docs/core/installation-overview)  
   Note: You will need to install the Databricks adapter.

2. **Connecting to DBT Core:** [Connecting to DBT Core](https://docs.databricks.com/en/partners/prep/dbt.html)

3. **Create and Run DBT Models Locally with Databricks:** [DBT Core Tutorial](https://docs.databricks.com/en/integrations/dbt-core-tutorial.html)

  </details>

<details>
    <summary> dbt_model.py </summary>
    
##  Overview of the Airflow DAG for dbt Integration.

This file defines an Apache Airflow DAG (Directed Acyclic Graph) that orchestrates a dbt (data build tool) pipeline using Databricks. The DAG is configured to run daily and connects to a Databricks environment using a token-based authentication method.

## Key Components of the File

1. **Profile Configuration**: 
   - Sets up a connection to Databricks using `DatabricksTokenProfileMapping`, which maps Airflow connections to dbt profiles.
   - Specifies connection details such as the database and schema.

2. **DbtDag Definition**:
   - Creates a `DbtDag` instance with configurations for the dbt project, execution settings, and scheduling.
   - The DAG is set to run daily, starting from a specified date, and does not catch up on missed runs.

## Installation and Setup Instructions

To install and run Apache Airflow with dbt integration, follow these steps:

1. **Install Airflow**:
   - Follow the [Getting Started with Apache Airflow](https://www.astronomer.io/docs/learn/get-started-with-airflow) guide to set up Airflow on your local machine or server.

2. **Configure Airflow for dbt**:
   - Refer to the [Orchestrate dbt Core with Airflow](https://www.astronomer.io/docs/learn/airflow-dbt) documentation to understand how to integrate dbt with Airflow.

3. **Set Up Databricks Connection**:
   - You will need to configure Airflow to connect with Databricks using a Databricks token. Follow the instructions provided in the [Databricks Token Profile documentation](https://astronomer.github.io/astronomer-cosmos/profiles/DatabricksToken.html).

4. **Run the DAG**:
   - Once everything is set up, run `astro dev start` and trigger the DAG from the Airflow UI to start the dbt pipeline.

## Additional Resources

- For a practical guide, check out the [Code Along: Build an ETL Pipeline in 1 Hour (dbt, Snowflake, and Airflow)](https://www.youtube.com/watch?v=OLXkGB7krGo) video tutorial.

By following these steps and utilizing the provided resources, you will be able to successfully set up and run an Airflow DAG that integrates with dbt and Databricks.
</details>

## :mag_right: Learning Outcome
One of the most valuable takeaways from this project is the crucial role of documentation in computer programming. Documentation provides essential insights into the operation of a product, making it the cornerstone of software development. Before this project, I had little knowledge of running dbt and Airflow. However, I was able to construct dbt models and choreograph them with Airflow because I mastered the skill of searching for and understanding the installation documentation for these tools.

I also grasped the significance of writing tests for programs. Programs may not always function as intended, so it is vital to create tests to catch any potential failures.

## :mag_right: Future Work
There are some few changes and implementation i would work on in the future, hiwever i will mention them here and tell you why. this future work are based on how the program is build 

1. ocretration are triggeredd in 2 places: in databricks :that is the file `Datebase connection.py` and dbt models in airflow. in future work i plan to move all orchestration into airflow. so that a centrailsed view of the pipline will be done.

