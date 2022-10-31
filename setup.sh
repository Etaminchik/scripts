#!/usr/bin/env bash

script_path=`readlink -m $0`
script_path=`dirname ${script_path}`

args=""
for arg in "$@"
do
    args="${args} ${arg}"
done

psql ${args} --quiet --tuples-only -o /tmp/oim_script.sql -c "
with table_info as (
    select
        not table_name ~ '_[0-9]+$'   as is_root_table,
        table_name                    as table_name
    from (
        select
            tablename::text as table_name,
            case
                when tablename like 'aaa_session_events%' then 'asev'
            end as prefix
        from pg_tables
        where
            schemaname = 'oimc'
            and (
                tablename like 'aaa\_session\_events%' escape '\'
            )
    ) a
)
select stmt
from (
    select
        0                                       as stmt_order,
        true                                    as is_root_table,
        'table_name'                            as table_name,
        'set client_min_messages to warning;'   as stmt
    union all
    select
        1 as stmt_order,
        a.is_root_table,
        a.table_name,
        'do \$\$ begin alter table oimc.' || a.table_name || ' add asev_telco_code integer; exception when duplicate_column then null; end \$\$;' as stmt
    from
        table_info a
    union all
    select
        2 as stmt_order,
        a.is_root_table,
        a.table_name,
        'comment on column oimc.' || a.table_name || '.asev_telco_code is ''TELCO-код'';' as stmt
    from
        table_info a
    union all
    select
        3 as stmt_order,
        a.is_root_table,
        a.table_name,
        'do \$\$ begin alter table oimc.' || a.table_name || ' add asev_session_id varchar(64); exception when duplicate_column then null; end \$\$;' as stmt
    from
        table_info a
    union all
    select
        4 as stmt_order,
        a.is_root_table,
        a.table_name,
        'comment on column oimc.' || a.table_name || '.asev_session_id is ''Идентификатор сессии'';' as stmt
    from
        table_info a
    --
    union all
    select
        10 as stmt_order,
        a.is_root_table,
        a.table_name,
        'do \$\$ begin alter table oimc.' || a.table_name || ' add constraint asev_telco_code_ck check (asev_telco_code >= 0 AND asev_telco_code <= 65535) not valid; exception when duplicate_object then null; end \$\$;' as stmt
    from
        table_info a
    --
    union all
    select
        20 as stmt_order,
        true,
        'aaa_sessions',
        'do \$\$ begin create trigger bi_aaas before insert on oimc.aaa_sessions for each row execute procedure oimc.trig_deprecate_data_insertion(); exception when duplicate_object then null; end \$\$;' as stmt
    --
    union all
    select
        30 as stmt_order,
        a.is_root_table,
        a.table_name,
        'update ' ||
        '    oimc.' || a.table_name || ' '
        'set ' ||
        '    asev_telco_code = 1,' ||
        '    asev_session_id = asev_aaas_id ' ||
        'where ' ||
        '    asev_telco_code is null;' as stmt
    from
        table_info a
    where
        not a.is_root_table
    --
    union all
    select
        40 as stmt_order,
        a.is_root_table,
        a.table_name,
        'alter table oimc.' || a.table_name || ' alter asev_telco_code set not null;' as stmt
    from
        table_info a
    union all
    select
        41 as stmt_order,
        a.is_root_table,
        a.table_name,
        'alter table oimc.' || a.table_name || ' alter asev_session_id set not null;' as stmt
    from
        table_info a
    union all
    select
        42 as stmt_order,
        a.is_root_table,
        a.table_name,
        'alter table oimc.' || a.table_name || ' alter asev_aaas_id drop not null;' as stmt
    from
        table_info a
) a
order by
    stmt_order,
    is_root_table desc,
    table_name;
"

psql ${args} --quiet -f /tmp/oim_script.sql
rm -f /tmp/oim_script.sql

psql ${args} --quiet -f ${script_path}/oims.sql
psql ${args} --quiet -f ${script_path}/oimc.sql
psql ${args} --quiet -f ${script_path}/oimm.sql
