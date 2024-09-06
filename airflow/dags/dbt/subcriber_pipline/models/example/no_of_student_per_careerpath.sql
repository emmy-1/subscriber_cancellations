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
    sex,
    num_course_taken,
    timespent_on_course_in_hrs
    from hive_metastore.subcriber_cancellation.subscriber_cancellation_database
),

total_number_of_student_in_each_career_path as(
    select job_analysis.career_path_name,
    count(student_job_info.student_id) as total_student,
    round(avg(student_job_info.num_course_taken),2) as avg_num_course_taken,
    round(avg(student_job_info.timespent_on_course_in_hrs),2) as avg_timespent_on_course_in_hrs
    from job_analysis
    left join student_job_info 
    on job_analysis.career_path_name = student_job_info.career_path
    group by job_analysis.career_path_name
    order by total_student desc
)

select * from total_number_of_student_in_each_career_path


