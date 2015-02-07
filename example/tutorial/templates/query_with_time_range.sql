SELECT
  td_time_format(time, 'yyyy-MM') month,
  count(1) cnt
FROM
  nasdaq
WHERE
  td_time_range(time, '{{ task.year }}-01-01', '{{ task.year + 1 }}-01-01')
GROUP BY
  td_time_format(time, 'yyyy-MM')
