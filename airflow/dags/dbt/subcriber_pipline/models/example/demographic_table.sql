{{config(materialized='table', file_format='delta')}}

with student_demographic as(
    select
    count(student_id) as num_students,
    age,
    sex,
    city,
    state,
    zip_code
    from hive_metastore.subcriber_cancellation.subscriber_cancellation_database
    group by age, sex, city, state, zip_code
)
    

select * from student_demographic 