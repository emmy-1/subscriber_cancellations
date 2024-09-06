{{ config(materialized='table', file_format='delta') }}

with student_infomation as(
    select student_id,
    student_name,
    getdate() as CurrentDate, year(getdate())-year(date_of_brith) as age,
    sex,
    mailing_address,
    SPLIT(mailing_address, ',')[0] as street,
    SPLIT(mailing_address, ',')[1] as city,
    SPLIT(mailing_address, ',')[2] as state,
    SPLIT(mailing_address, ',')[3] as zip_code,
    email,current_career_path_id, job_id,
    num_course_taken,
    timespent_on_course_in_hrs
    from hive_metastore.subcriber_cancellation.students_database
),

Course_infomation as(
    select 
    current_career_path_id,
    career_path_name,
    Hours_to_complete
    from hive_metastore.subcriber_cancellation.courses_database
),

jobs_infomation as(
    select job_id,
    job_category,
    avg_salary
    from hive_metastore.subcriber_cancellation.jobs_database
),

final_dataset as(
    select
    student_infomation.student_id,
    student_infomation.student_name,
    student_infomation.age,
    student_infomation.sex,
    student_infomation.street,
    student_infomation.city,
    student_infomation.state,
    student_infomation.zip_code,
    student_infomation.email,
    Course_infomation.career_path_name as career_path,
    jobs_infomation.job_category as job_title,
    student_infomation.num_course_taken,
    student_infomation.timespent_on_course_in_hrs
    from student_infomation
    inner join Course_infomation on student_infomation.current_career_path_id = Course_infomation.current_career_path_id
    inner join jobs_infomation on student_infomation.job_id = jobs_infomation.job_id
)

select * from final_dataset

