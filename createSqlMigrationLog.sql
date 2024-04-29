CREATE TABLE public.ddl_log (
	id int4 NOT NULL,
	username text NULL,
	object_tag text NULL,
	ddl_command text NULL,
	"timestamp" timestamp NULL,
	ddl_revert_command text NULL,
	column1 int4 NULL,
	CONSTRAINT ddl_log_pkey PRIMARY KEY (id)
);
CREATE INDEX ddl_log_id_idx ON public.ddl_log USING btree (id);

-- DROP FUNCTION public.generate_create_table_statement(varchar);

CREATE OR REPLACE FUNCTION public.generate_create_table_statement(p_table_name character varying)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_table_ddl   text;
    column_record record;
BEGIN
    FOR column_record IN 
        SELECT 
            b.nspname as schema_name,
            b.relname as table_name,
            a.attname as column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type,
            CASE WHEN 
                (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                 FROM pg_catalog.pg_attrdef d
                 WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) IS NOT NULL THEN
                'DEFAULT '|| (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                              FROM pg_catalog.pg_attrdef d
                              WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
            ELSE
                ''
            END as column_default_value,
            CASE WHEN a.attnotnull = true THEN 
                'NOT NULL'
            ELSE
                'NULL'
            END as column_not_null,
            a.attnum as attnum,
            e.max_attnum as max_attnum
        FROM 
            pg_catalog.pg_attribute a
            INNER JOIN 
             (SELECT c.oid,
                n.nspname,
                c.relname
              FROM pg_catalog.pg_class c
                   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relname ~ ('^('||p_table_name||')$')
                AND pg_catalog.pg_table_is_visible(c.oid)
              ORDER BY 2, 3) b
            ON a.attrelid = b.oid
            INNER JOIN 
             (SELECT 
                  a.attrelid,
                  max(a.attnum) as max_attnum
              FROM pg_catalog.pg_attribute a
              WHERE a.attnum > 0 
                AND NOT a.attisdropped
              GROUP BY a.attrelid) e
            ON a.attrelid=e.attrelid
        WHERE a.attnum > 0 
          AND NOT a.attisdropped
        ORDER BY a.attnum
    LOOP
        IF column_record.attnum = 1 THEN
            v_table_ddl:='CREATE TABLE '||column_record.schema_name||'.'||column_record.table_name||' (';
        ELSE
            v_table_ddl:=v_table_ddl||',';
        END IF;
        IF column_record.attnum <= column_record.max_attnum THEN
            v_table_ddl:=v_table_ddl||chr(10)||
                     '    '||column_record.column_name||' '||column_record.column_type||' '||column_record.column_default_value||' '||column_record.column_not_null;
        END IF;
    END LOOP;

    v_table_ddl:=v_table_ddl||');';
    RETURN v_table_ddl;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.log_ddl_changes()
 RETURNS event_trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
INSERT INTO ddl_log
(
id,
username,
object_tag,
ddl_command,
ddl_revert_command,
Timestamp
)
VALUES
(
nextval('ddl_log_seq'),
current_user,
tg_tag,
current_query(),
revertsqlstatement(current_query()),
current_timestamp
);
END;
$function$
;

CREATE OR REPLACE FUNCTION public.revertsqlstatement(sqlin text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE resultStr text;
declare part text;
declare tableStr text;
declare columnName text;
declare columnName2 text;
declare datatypeStr text;
declare command text;
declare funcStr text;
declare funcSrcStr text;
declare posNum int;
declare argtypes text;
declare argtypesStr text;
declare firstargtype boolean;
declare indexdefStr text;
declare argpart text;
begin
	if LOWER(left(sqlin,12)) = 'create table' then
		return CONCAT('DROP TABLE ',pg_catalog.split_part(sqlin, ' ', 3)); 
	elsif LOWER(left(sqlin,11)) = 'alter table' then
		tableStr = pg_catalog.split_part(sqlin, ' ', 3);
		if POSITION('rename column' in lower(sqlin))  != 0 then 
			part = substring(lower(sqlin),POSITION('rename column' in lower(sqlin)));
			columnName = pg_catalog.split_part(part, ' ', 3);
			columnName2 = replace(pg_catalog.split_part(part, ' ', 5),'"','');
			resultStr = CONCAT('ALTER TABLE ',tablestr, ' RENAME COLUMN "', columnName2, '" TO "', columnName, '"');
			return resultStr;
		elsif lower(pg_catalog.split_part(sqlin, ' ', 4)) = 'add' then 
			columnName = pg_catalog.split_part(sqlin, ' ', 5);
			resultStr = CONCAT('ALTER TABLE ',tablestr, ' DROP COLUMN "', columnName, '"');
			return resultStr;
		elsif POSITION('drop column' in lower(sqlin))  != 0 then 
			part = substring(lower(sqlin),POSITION('drop column' in lower(sqlin)));
			columnName = pg_catalog.split_part(part, ' ', 3);
			tableStr := REPLACE(tableStr,CONCAT(current_schema(),'.'),'');
			select udt_name into datatypeStr  from information_schema.columns where table_name = tableStr and column_name = columnName;
			raise notice 'The number of actors: %', datatypeStr	;
			resultStr = CONCAT('ALTER TABLE ',tablestr, ' ADD "', columnName, '" ',datatypeStr);
			return resultStr;
		elsif POSITION('alter column' in lower(sqlin)) != 0 then 
			part = substring(lower(sqlin),POSITION('alter column' in lower(sqlin)));
			columnName = pg_catalog.split_part(part, ' ', 3);
			tableStr := REPLACE(tableStr,CONCAT(current_schema(),'.'),'');
			--raise notice 'ColumnName: %', columnName;
			--raise notice 'Table: %', tableStr;
			select information_schema.columns.udt_name into datatypeStr from information_schema.columns where table_name = tableStr and column_name = columnName; 
			--raise notice 'The number of actors: %', datatypeStr;	
			resultStr = CONCAT('ALTER TABLE ',tablestr, ' ALTER COLUMN "', columnName, '" TYPE ', datatypeStr, ' USING ', columnName,'::',datatypeStr);
			return resultStr;
		else
			return 'Cannot find revertParsing Please Parse Revert manually';
		end if;
	elsif LOWER(left(sqlin,10)) = 'drop table' then
		tableStr = pg_catalog.split_part(sqlin, ' ', 3);
		--
		if POSITION('.' in lower(sqlin)) != 0 then 
			tableStr = lower(pg_catalog.split_part(tableStr, '.', 2));
		end if;
		return generate_create_table_statement(tableStr);
	elsif LOWER(left(sqlin,13)) = 'drop function' then
		funcStr = pg_catalog.split_part(sqlin, ' ', 3);
		funcStr := REPLACE(funcStr, CONCAT(current_schema(),'.'),'');
		select prosrc into funcSrcStr from pg_proc join pg_namespace on pronamespace=pg_namespace.oid where proname like funcStr and nspname = current_schema();
		select "proargtypes" into argtypes from pg_proc join pg_namespace on pronamespace=pg_namespace.oid where proname like funcStr and nspname = current_schema();
		raise notice 'FSTR: %', funcStr;
		firstargtype = true;
		argtypesStr = '';
	 	raise notice 'Argtyopes: %', argtypes ;
		if argtypes is not null then
			foreach part in array string_to_array(argtypes, ' ')
			   loop
				  raise notice 'Part: %', part;
				  if firstargtype = true then 
			      	firstargtype = false;
				  	argtypesStr = CONCAT(argtypesStr,part);
				  else 
				  	argtypesStr = CONCAT(argtypesStr,',',part);
			      end if;
				   -- do something with part
			   end loop;
		end if;
		select pg_catalog.pg_get_functiondef(CONCAT(funcStr,'(',argtypesStr,')')::regprocedure::oid) into funcSrcStr;
		if funcSrcStr is not null then
			return funcSrcStr;	
		else 
			resultStr = CONCAT('DROP function ', funcStr);
			return resultStr;
		end if;
		return funcSrcStr;
	elsif LOWER(left(sqlin,15)) = 'create function' then
		funcStr = pg_catalog.split_part(sqlin, ' ', 3);
		resultStr = 'DROP function ' + funcStr;
		return resultStr;
	elsif LOWER(left(sqlin,26)) = 'create or replace function' then
		funcStr = pg_catalog.split_part(sqlin, ' ', 5);
		funcStr = pg_catalog.split_part(funcStr , '(', 1);
		funcStr := REPLACE(funcStr, CONCAT(current_schema(),'.'),'');
		select prosrc into funcSrcStr from pg_proc join pg_namespace on pronamespace=pg_namespace.oid where proname like funcStr and nspname = current_schema();
		select "proargtypes" into argtypes from pg_proc join pg_namespace on pronamespace=pg_namespace.oid where proname like funcStr and nspname = current_schema();
		raise notice 'FSTR: %', funcStr;
		firstargtype = true;
		argtypesStr = '';
	 	raise notice 'Argtyopes: %', argtypes ;
		if argtypes is not null then
			foreach part in array string_to_array(argtypes, ' ')
			   loop
				  raise notice 'Part: %', part;
				  if firstargtype = true then 
			      	firstargtype = false;
				  	argtypesStr = CONCAT(argtypesStr,part);
				  else 
				  	argtypesStr = CONCAT(argtypesStr,',',part);
			      end if;
				   -- do something with part
			   end loop;
		end if;
		select pg_catalog.pg_get_functiondef(CONCAT(funcStr,'(',argtypesStr,')')::regprocedure::oid) into funcSrcStr;
		if funcSrcStr is not null then
			return funcSrcStr;	
		else 
			resultStr = CONCAT('DROP function ', funcStr);
			return resultStr;
		end if;
		return funcSrcStr;
	elsif LOWER(left(sqlin,14)) = 'drop procedure' then
		funcStr = pg_catalog.split_part(sqlin, ' ', 3);
		funcStr := REPLACE(funcStr, CONCAT(current_schema(),'.'),'');
		select prosrc into funcSrcStr from pg_proc join pg_namespace on pronamespace=pg_namespace.oid where proname like funcStr and nspname = current_schema();
		select "proargtypes" into argtypes from pg_proc join pg_namespace on pronamespace=pg_namespace.oid where proname like funcStr and nspname = current_schema();
		raise notice 'FSTR: %', funcStr;
		firstargtype = true;
		argtypesStr = '';
	 	raise notice 'Argtyopes: %', argtypes ;
		if argtypes is not null then
			foreach part in array string_to_array(argtypes, ' ')
			   loop
				  raise notice 'Part: %', part;
				  if firstargtype = true then 
			      	firstargtype = false;
				  	argtypesStr = CONCAT(argtypesStr,part);
				  else 
				  	argtypesStr = CONCAT(argtypesStr,',',part);
			      end if;
				   -- do something with part
			   end loop;			
		end if;
		select pg_catalog.pg_get_functiondef(CONCAT(funcStr,'(',argtypesStr,')')::regprocedure::oid) into funcSrcStr;
		raise notice 'funcSrcStr: %', funcSrcStr;
		if funcSrcStr is not null then
			return funcSrcStr;	
		else 
			resultStr = CONCAT('DROP1 procedure ', funcStr);
			return resultStr;
		end if;
		return funcSrcStr;
	elsif LOWER(left(sqlin,16)) = 'create procedure' then
		funcStr = pg_catalog.split_part(sqlin, ' ', 3);
		resultStr = CONCAT('DROP procedure ',funcStr);
		return resultStr;
	elsif LOWER(left(sqlin,27)) = 'create or replace procedure' then
		funcStr = pg_catalog.split_part(sqlin, ' ', 5);
		funcStr := REPLACE(funcStr, CONCAT(current_schema(),'.'),'');
		select prosrc into funcSrcStr from pg_proc join pg_namespace on pronamespace=pg_namespace.oid where proname like funcStr and nspname = current_schema();
		select "proargtypes" into argtypes from pg_proc join pg_namespace on pronamespace=pg_namespace.oid where proname like funcStr and nspname = current_schema();
		raise notice 'FSTR: %', funcStr;
		firstargtype = true;
		argtypesStr = '';
	 	raise notice 'Argtyopes: %', argtypes ;
		if argtypes is not null then
			foreach part in array string_to_array(argtypes, ' ')
			   loop
				  raise notice 'Part: %', part;
				  if firstargtype = true then 
			      	firstargtype = false;
				  	argtypesStr = CONCAT(argtypesStr,part);
				  else 
				  	argtypesStr = CONCAT(argtypesStr,',',part);
			      end if;				   -- do something with part
			   end loop;
		end if;
		select pg_catalog.pg_get_functiondef(CONCAT(funcStr,'(',argtypesStr,')')::regprocedure::oid) into funcSrcStr;
		if funcSrcStr is not null then
			return funcSrcStr;	
		else 
			resultStr = CONCAT('DROP procedure ', funcStr);
			return resultStr;
		end if;
		return funcSrcStr;
	elsif LOWER(left(sqlin,12)) = 'create index' then
		funcStr = pg_catalog.split_part(sqlin, ' ', 3);
		resultStr = CONCAT('DROP index ',funcStr);
		return resultStr;
	elsif LOWER(left(sqlin,10)) = 'drop index' then
		funcStr = pg_catalog.split_part(sqlin, ' ', 3);
		select pg_indexes.indexdef into indexdefStr from pg_indexes where schemaname = current_schema() and indexname=funcStr;
		resultStr = indexdefStr;
		return resultStr;
	else 
		return 'Cannot find revertParsing Please Parse Revert manually';
	END IF;
END; $function$
;


CREATE EVENT TRIGGER log_ddl_trigger ON ddl_command_start
	EXECUTE FUNCTION public.log_ddl_changes();