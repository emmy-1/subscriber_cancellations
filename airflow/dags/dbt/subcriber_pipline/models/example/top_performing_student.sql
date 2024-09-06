{{config(materialized='table', file_format='delta')}}

with student_performance as(
    select student_id,
    student_name,
    sum(num_course_taken) as total_course_taken,
    avg(timespent_on_course_in_hrs) as avg_time_spent_in_hrs
    from hive_metastore.subcriber_cancellation.subscriber_cancellation_database
    group by student_id, student_name
    order by avg_time_spent_in_hrs desc
)

select * from student_performance

