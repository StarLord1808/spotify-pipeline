{% test no_nulls(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NULL
{% endtest %}
