{{config(materialized='table', file_format='delta')}}

with course_completion as(
    select 
    current_career_path_id,
    career_path_name,
    Hours_to_complete
    from hive_metastore.subcriber_cancellation.courses_database
),

student_course_info as(
    select
    student_id,
    student_name,
    current_career_path_id,
    timespent_on_course_in_hrs
    from hive_metastore.subcriber_cancellation.students_database
),

course_completion_info as(
    select
    student_course_info.student_id as student_id,
    student_course_info.student_name as student_name,
    course_completion.career_path_name as courese,
    course_completion.Hours_to_complete as Hours_to_complete,
    student_course_info.timespent_on_course_in_hrs as timespent_on_course_in_hrs,
    case when student_course_info.timespent_on_course_in_hrs >= course_completion.Hours_to_complete then 'Yes' else 'No' end as is_course_completed
    from student_course_info
    left join course_completion
    on student_course_info.current_career_path_id = course_completion.current_career_path_id
    group by student_course_info.student_id, student_course_info.student_name, course_completion.career_path_name, course_completion.Hours_to_complete, student_course_info.timespent_on_course_in_hrs

)    

select * from course_completion_info