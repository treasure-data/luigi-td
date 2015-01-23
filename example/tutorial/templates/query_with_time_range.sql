SELECT count(1) cnt
FROM www_access
WHERE td_time_range(time, td_time_add('{{ task.target_date }}', '-1d'), '{{ task.target_date }}')
