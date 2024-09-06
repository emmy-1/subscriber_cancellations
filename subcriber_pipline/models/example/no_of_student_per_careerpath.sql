{{config(materialized='table', file_format='delta')}}

with job_analysis as(
    select current_career_path_id,
    career_path_name
    from hive_metastore.subcriber_cancellation.courses_database
),

student_job_info as(
    select career_path,
    student_id,
    student_name,
    age,
    sex
    from hive_metastore.subcriber_cancellation.subscriber_cancellation_database
),

total_number_of_student_in_each_career_path as(
    select job_analysis.career_path_name,
    count(student_job_info.student_id) as total_student
    from job_analysis
    left join student_job_info 
    on job_analysis.career_path_name = student_job_info.career_path
    group by job_analysis.career_path_name
)

select * from total_number_of_student_in_each_career_path


