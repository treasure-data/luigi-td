SELECT count(1) cnt
FROM   www_access
WHERE  code = {{ task.status_code }}
