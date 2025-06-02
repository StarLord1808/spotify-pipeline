{% macro print_row_counts() %}
{% for node in graph.nodes.values() if node.resource_type == 'model' %}
    {% set relation = adapter.get_relation(
        database=node.database,
        schema=node.schema,
        identifier=node.name
    ) %}
    {% if relation %}
        {% set row_count = run_query("SELECT COUNT(*) FROM " ~ relation).columns[0].values()[0] %}
        {{ log("Model: " ~ node.name ~ " → " ~ row_count ~ " rows", info=True) }}
    {% endif %}
{% endfor %}
{% endmacro %}
