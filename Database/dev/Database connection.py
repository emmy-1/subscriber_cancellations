# Databricks notebook source
import sqlite3
import os
import pandas as pd
import numpy as np
import json
working_Dir = "/Workspace/Users/ade@gentechorg.onmicrosoft.com/subscriber_cancellations/Database/dev/"

# COMMAND ----------

# Function to change the current working directory
def change_directory(file_path):
    return os.chdir(file_path)
change_directory(working_Dir)

# Function to create a connection to the SQLite database
def create_connection(db_name):
    return sqlite3.connect(db_name)

# Connect To Database
con = create_connection("cademycode.db")
cur = con.cursor()

def read_tables(table_name, con_name):
    return pd.read_sql_query(f"SELECT * FROM {table_name}",con_name)

def replace_na(df, value_to_replace):
    return df.na.fill(value='value_to_replace')

# Load the first 50 Rows of a dataset
def firshead(df):
    return df.head(60)

# Function to parse JSON-like strings in the specified column
def parse_contact_info(df, column_name,char1, char2):
    # Parse the JSON-like strings and create new columns
    df[[char1, char2]] = df[column_name].apply(lambda x: pd.Series(json.loads(x)))
    return df



students =read_tables("cademycode_students", con)
courses = read_tables("cademycode_courses", con)
jobs = read_tables("cademycode_student_jobs", con)

# COMMAND ----------

Update_student = students.fillna({'job_id': 0})
check_dupliactedb = Update_student.drop_duplicates()
split_coldb = parse_contact_info(check_dupliactedb, 'contact_info', 'mailing_address', 'email') 
dropcolumdb = split_coldb.drop(['contact_info'], axis=1)
no_course = dropcolumdb.fillna({'num_course_taken': 0})
update_career_path = no_course.fillna({'current_career_path_id': 0})
update_hourspent = update_career_path.fillna({'time_spent_hrs': 0})

