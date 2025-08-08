{% test col_gte_compared(model, column_name, compared_to) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} <= {{ compared_to }}
{% endtest %} 