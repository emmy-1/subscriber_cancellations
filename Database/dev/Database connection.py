# Databricks notebook source
import sqlite3
import os
import pandas as pd
import numpy as np
import json
working_Dir = "/Workspace/Users/ade@gentechorg.onmicrosoft.com/subscriber_cancellations/Database/dev/"

# COMMAND ----------

import sqlite3
import os
import pandas as pd
import numpy as np
import json

# Set the working directory
working_Dir = "/Workspace/Users/ade@gentechorg.onmicrosoft.com/subscriber_cancellations/Database/dev/"

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

def apply_transformations(df, transformations):
    """Apply a list of transformation functions to a DataFrame."""
    try:
        for transform in transformations:
            df = transform(df)
        return df
    except Exception as e:
        print(f"Error applying transformations: {e}")
        return df

def replace_na(df, value_to_replace):
    """Replace NA values in a DataFrame with the specified value."""
    try:
        return df.fillna(value=value_to_replace)
    except Exception as e:
        print(f"Error replacing NA values: {e}")
        return df

def parse_contact_info(df, column_name, char1, char2):
    """Parse JSON-like strings in the specified column and create new columns."""
    try:
        df[[char1, char2]] = df[column_name].apply(lambda x: pd.Series(json.loads(x)))
        return df
    except Exception as e:
        print(f"Error parsing contact info: {e}")
        return df

# Create a single connection to the database
connection = connect_to_database()

# Read tables using the same connection
students = read_tables("cademycode_students", connection)
courses = read_tables("cademycode_courses", connection)
jobs = read_tables("cademycode_student_jobs", connection)

# Define the transformations to be applied
transformations = [
    lambda df: replace_na(df, {'job_id': 0}),
    lambda df: df.drop_duplicates(),
    lambda df: parse_contact_info(df, 'contact_info', 'mailing_address', 'email'),
    lambda df: df.drop(['contact_info'], axis=1),
    lambda df: replace_na(df, {'num_course_taken': 0}),
    lambda df: replace_na(df, {'current_career_path_id': 0}),
    lambda df: replace_na(df, {'time_spent_hrs': 0})
]

# Apply the transformations to the students DataFrame
processed_students = apply_transformations(students, transformations)

# Display the processed DataFrame
display(processed_students)

# COMMAND ----------

def get_missing_rows(df, column):
    """
    Returns a DataFrame with rows that have missing values in the specified column.
    """
    return df[df[column].isnull()]

def drop_missing_rows(df, column):
    """
    Returns a DataFrame with rows that do not have missing values in the specified column.
    """
    return df.dropna(subset=[column])

# Get the missing rows and cleaned DataFrame for 'num_course_taken'
missing_students = get_missing_rows(students, 'num_course_taken')
cleaned_students = drop_missing_rows(students, 'num_course_taken')

# Get the missing rows and cleaned DataFrame for 'job_id'
missing_job_id = get_missing_rows(cleaned_students, 'job_id')
cleaned_student_id = drop_missing_rows(cleaned_students, 'job_id')

# Display the cleaned DataFrame and the DataFrame with missing values
display(cleaned_student_id)

# COMMAND ----------

cleaned_student_id.info()

# COMMAND ----------

job_idm = students[students[['job_id']].isnull().any(axis=1)]

# COMMAND ----------

def fill_np_zero(dataset, column_name):
    dataset[column_name] = np.where(dataset[column_name].isnull(), 0, dataset[column_name]) 
    return dataset

# Apply the function to the 'current_career_path_id' column
cleaned_students_carerid = fill_np_zero(cleaned_student_id, 'current_career_path_id')

# Apply the function to the 'time_spent_hrs' column
cleaned_students_timespent = fill_np_zero(cleaned_students_carerid, 'time_spent_hrs')

# Display the updated DataFrame
display(cleaned_students_timespent)

# COMMAND ----------

cleaned_students_timespent.info()

# COMMAND ----------

def concat_into(dataset, column_name):
    return pd.concat([dataset, column_name], ignore_index=True)
# Concatenate the missing job_id rows to missingDB
missingDB = concat_into(missing_students, job_idm)

# COMMAND ----------

#creating a dictionary and append.
not_applicatble = {'career_path_id':0, 'career_path_name':'Not Applicable', 'hours_to_complete':0}
courses.loc[len(courses)] = not_applicatble

# COMMAND ----------

display(courses)

# COMMAND ----------

jobs.drop_duplicates(inplace=True)
display(jobs)
