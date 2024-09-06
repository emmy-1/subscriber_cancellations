-- Databricks notebook source
-- MAGIC %md
-- MAGIC # The Medallion Architecture (BRONZ TABLES)
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE students
AS SELECT * FROM cloud_files("abfss://pcpart@neweggdb.dfs.core.windows.net/subcriber_calculation/students_bronz/", "csv");

CREATE OR REFRESH STREAMING TABLE courses
AS SELECT * FROM cloud_files("abfss://pcpart@neweggdb.dfs.core.windows.net/subcriber_calculation/courses_bronz/", "csv");

CREATE OR REFRESH STREAMING TABLE jobs
AS SELECT * FROM cloud_files("abfss://pcpart@neweggdb.dfs.core.windows.net/subcriber_calculation/jobs_bronz/", "csv");

CREATE OR REFRESH STREAMING TABLE incomplete_students
AS SELECT * FROM cloud_files("abfss://pcpart@neweggdb.dfs.core.windows.net/subcriber_calculation/incomplete_bronz/", "csv");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # The Medallion Architecture (SLIVER TABLES)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sliver_student (
CONSTRAINT valid_student_id EXPECT (student_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Bronz Student Table(Madaillion Architecture)"
AS SELECT 
CAST(uuid AS INT) AS student_id,
name as student_name,
CAST(dob as DATE) as date_of_brith,
sex,
CAST(job_id AS INT) as job_id,
CAST(num_course_taken AS INT) as num_course_taken,
CAST(current_career_path_id AS INT) as current_career_path_id,
CAST(time_spent_hrs AS INT) as timespent_on_course_in_hrs,
mailing_address,
email
FROM STREAM(live.students);



-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sliver_incomplete_student (
CONSTRAINT valid_student_id EXPECT (student_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Bronz incompletee Student Table(Madaillion Architecture)"
AS SELECT 
CAST(uuid AS INT) AS student_id,
name as student_name,
CAST(dob as DATE) as date_of_brith,
sex,
CAST(job_id AS INT) as job_id,
CAST(num_course_taken AS INT) as num_course_taken,
CAST(current_career_path_id AS INT) as current_career_path_id,
CAST(time_spent_hrs AS INT) as timespent_on_course_in_hrs,
mailing_address,
email
FROM STREAM(live.incomplete_students)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sliver_courses (
CONSTRAINT valid_course_id EXPECT (current_career_path_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Bronz Course Table(Madaillion Architecture)"
AS SELECT
CAST(career_path_id as INT) AS current_career_path_id,
career_path_name,
CAST(hours_to_complete as INT) AS Hours_to_complete
FROM STREAM(live.courses)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sliver_jobs(
CONSTRAINT valid_job_id EXPECT (job_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Bronz Job Table(Madaillion Architecture)"
AS SELECT 
CAST(job_id as INT) AS job_id,
job_category,
CAST(avg_salary as INT) AS avg_salary
FROM STREAM(live.jobs)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # The Medallion Architecture (SLIVER TABLES)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE students_database
COMMENT "gold Student Table(Madaillion Architecture)"
AS SELECT * FROM STREAM(live.sliver_student);

CREATE OR REFRESH STREAMING TABLE courses_database
COMMENT "gold Course Table(Madaillion Architecture)"
AS SELECT * FROM STREAM(live.sliver_courses);

CREATE OR REFRESH STREAMING TABLE jobs_database
COMMENT "gold Job Table(Madaillion Architecture)"
AS SELECT * FROM STREAM(live.sliver_jobs);

CREATE OR REFRESH STREAMING TABLE not_enrolled_students_database
COMMENT "golde Student Table(Madaillion Architecture)"
AS SELECT * FROM STREAM(live.sliver_incomplete_student);
