# Databricks notebook source
# MAGIC %md
# MAGIC # Import Necessary Libaries 

# COMMAND ----------

import os
import pandas as pd
import numpy as np
import json
from dotenv import load_dotenv
import sqlite3

# Load environment variables from a .env file
load_dotenv()

# Set the working directory from environment variable
working_Dir = os.getenv("WORKING_DIR")

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Connection Functions

# COMMAND ----------

def change_directory(file_path):
    """Change the current working directory to the specified file path."""
    try:
        os.chdir(file_path)
        return os.getcwd()
    except Exception as e:
        print(f"Error changing directory: {e}")
        return None

# Change the working directory
current_dir = change_directory(working_Dir)

def create_connection(db_name):
    """Create a connection to the SQLite database."""
    try:
        connection = sqlite3.connect(db_name)
        return connection
    except sqlite3.Error as e:
        print(f"Error creating connection: {e}")
        return None
    

def connect_to_database():
    """Connect to the SQLite database and return the connection."""
    try:
        return create_connection("cademycode.db")
    except Exception as e:
        print(f"Error: {e}")
        return None

    
def read_tables(table_name, con_name):
    """Read the specified table from the database and return a Pandas DataFrame."""
    try:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", con_name)
    except Exception as e:
        print(f"Error reading table {table_name}: {e}")
        return pd.DataFrame()

# Create a single connection to the database
connection = connect_to_database()

# Read tables using the same connection
students = read_tables("cademycode_students", connection)
courses = read_tables("cademycode_courses", connection)
jobs = read_tables("cademycode_student_jobs", connection)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation Function

# COMMAND ----------

def parse_contact_info(df, column_name, char1, char2):
    """Parse JSON-like strings in the specified column and create new columns."""
    try:
        df[[char1, char2]] = df[column_name].apply(lambda x: pd.Series(json.loads(x)))
        return df
    except Exception as e:
        print(f"Error parsing contact info: {e}")
        return df
    

def get_missing_rows(df, column):
    """
    Returns a DataFrame with rows that have missing values in the specified column.

    """
    try:
        return df[df[column].isnull()]
    except Exception as e:
        print(f"Error getting missing rows: {e}")
    return df

def drop_missing_rows(df, column):
    """
    Returns a DataFrame with rows that do not have missing values in the specified column.
    """
    try:
        return df.dropna(subset=[column])
    except Exception as e:
        print(f"Error dropping missing rows: {e}")
    return df


def concat_into_db(df1, df2):
    """
    Concatenate two DataFrames along the rows.
    """
    try:
        return pd.concat([df1, df2], ignore_index=True)
    except Exception as e:
        print(f"Error concatenating DataFrames. Ensure both dataframes have the same columns: {e}")
        return pd.DataFrame() # Return an empty DataFrame in case of an error

def fill_np_zero(dataset, column_name):
    """ Replace NaN values in the specified column with 0.
    """
    try:
        dataset[column_name] = np.where(dataset[column_name].isnull(), 0, dataset[column_name]) 
    except Exception as e:
        print(f"There are no NaN values in the tables: {e}")
    return dataset

def drop_colums(dataset, column_name):
    return dataset.drop(column_name, axis=1)

def not_applicable(dataset, career_id, career_name, hours):
    try:
        dictionary = {'career_path_id':career_id,  'career_path_name': career_name, 'hours_to_complete': hours}
        dataset.loc[len(dataset)] = dictionary
        return dataset
    except Exception as e:
        print(e)
        return dataset

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation Logic

# COMMAND ----------

def Run_Transfomer(Dataset):
    Change_contact_info = parse_contact_info(Dataset, 'contact_info', 'mailing_address', 'email')
    # Get the missing rows and cleaned DataFrame for 'num_course_taken'
    missing_students = get_missing_rows(Change_contact_info, 'num_course_taken')
    cleaned_students = drop_missing_rows(Change_contact_info, 'num_course_taken')
    # Get the missing rows and cleaned DataFrame for 'job_id'
    missing_job_id = get_missing_rows(cleaned_students, 'job_id')
    cleaned_student_id = drop_missing_rows(cleaned_students, 'job_id')
    # join the two DataFrames for mising values
    join_missing_tables = concat_into_db(missing_students, missing_job_id)

    # Apply 0 the 'current_career_path_id' column. where 0 means stduent has chossen a career path
    cleaned_students_carerid = fill_np_zero(cleaned_student_id, 'current_career_path_id')

    # Apply 0 to the 'time_spent_hrs' column. where 0 means student has taken any courses
    cleaned_students_timespent = fill_np_zero(cleaned_students_carerid, 'time_spent_hrs')

    final_dataset = drop_colums(cleaned_students_timespent, 'contact_info')

    return join_missing_tables, final_dataset

# Run the transformer and get all three DataFrames
join_missing_tables, final_dataset = Run_Transfomer(students)

# Update the courses table
Courese_updated = not_applicable(courses, 0, 'Not Applicable', 0)

# Update the jobs table
jobs.drop_duplicates(inplace=True)


# Display the DataFrames
display(join_missing_tables)
display(final_dataset)
display(Courese_updated)
display(jobs)

# COMMAND ----------

New_SQllitconnection = sqlite3.connect('cademycode_cleaned.db')
join_missing_tables.to_sql('join_missing_tables', New_SQllitconnection, if_exists='replace', index=False)
final_dataset.to_sql('cleaned_students_timespent', New_SQllitconnection, if_exists='replace', index=False)
Courese_updated.to_sql('Courese_updated', New_SQllitconnection, if_exists='replace', index=False)
jobs.to_sql('jobs', New_SQllitconnection, if_exists='replace', index=False)


# COMMAND ----------

Test1 = pd.read_sql_query("SELECT * FROM jobs", New_SQllitconnection)
display(Test1)
