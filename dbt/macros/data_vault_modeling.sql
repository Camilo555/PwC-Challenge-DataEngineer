

{% macro test_data_vault_integrity(hub_table, satellites, links) %}

-- Test 1: Hub integrity - no null business keys
with hub_integrity as (
    select 
        'hub_null_business_keys' as test_type,
        {{ hub_table.hub_key }},
        {% for column in hub_table.business_keys %}
        '{{ column }}' as business_key_column,
        {{ column }} as business_key_value{% if not loop.last %}
    from {{ ref('hub_' + hub_table.name) }}
    where {{ column }} is null
    
    union all
    
    select 
        'hub_null_business_keys' as test_type,
        {{ hub_table.hub_key }},{% endif %}
        {% endfor %}
    from {{ ref('hub_' + hub_table.name) }}
    where {% for column in hub_table.business_keys %}
        {{ column }} is null{% if not loop.last %} or {% endif %}
        {% endfor %}
),

-- Test 2: Satellite integrity - no orphaned satellites
satellite_integrity as (
    {% for satellite in satellites %}
    select 
        'orphaned_satellite_{{ satellite.name }}' as test_type,
        s.{{ satellite.parent_key }} as satellite_key,
        null as business_key_column,
        null as business_key_value
    from {{ ref('sat_' + satellite.name) }} s
    left join {{ ref('hub_' + satellite.parent_hub) }} h
        on s.{{ satellite.parent_key }} = h.{{ satellite.parent_key }}
    where h.{{ satellite.parent_key }} is null
    
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
),

-- Test 3: Link integrity - all hub keys exist
link_integrity as (
    {% for link in links %}
    {% for hub_ref in link.hub_references %}
    select 
        'orphaned_link_{{ link.name }}_{{ hub_ref.hub_name }}' as test_type,
        l.{{ hub_ref.hub_name }}_hk as satellite_key,
        '{{ hub_ref.hub_name }}_hk' as business_key_column,
        l.{{ hub_ref.hub_name }}_hk as business_key_value
    from {{ ref('link_' + link.name) }} l
    left join {{ ref('hub_' + hub_ref.hub_name) }} h
        on l.{{ hub_ref.hub_name }}_hk = h.{{ hub_ref.hub_name }}_hk
    where h.{{ hub_ref.hub_name }}_hk is null
    
    {% if not loop.last or not (loop.index == link.hub_references|length and loop.index0 == links|length - 1) %}
    union all
    {% endif %}
    {% endfor %}
    {% endfor %}
),

-- Test 4: Hash key uniqueness
hash_key_duplicates as (
    select 
        'duplicate_hub_hash_key' as test_type,
        {{ hub_table.hub_key }} as satellite_key,
        'hub_hash_key' as business_key_column,
        {{ hub_table.hub_key }} as business_key_value
    from {{ ref('hub_' + hub_table.name) }}
    group by {{ hub_table.hub_key }}
    having count(*) > 1
    
    {% for link in links %}
    union all
    
    select 
        'duplicate_link_hash_key_{{ link.name }}' as test_type,
        {{ link.link_key }} as satellite_key,
        'link_hash_key' as business_key_column,
        {{ link.link_key }} as business_key_value
    from {{ ref('link_' + link.name) }}
    group by {{ link.link_key }}
    having count(*) > 1
    {% endfor %}
)

select * from hub_integrity
union all
select * from satellite_integrity
union all  
select * from link_integrity
union all
select * from hash_key_duplicates

{% endmacro %}