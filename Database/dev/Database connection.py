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
