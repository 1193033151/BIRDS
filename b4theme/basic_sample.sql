CREATE OR REPLACE VIEW public.v AS 
SELECT __dummy__.COL0 AS X, __dummy__.COL1 AS Y FROM (SELECT v_a2_0.COL0 AS COL0, v_a2_0.COL1 AS COL1 FROM (SELECT s2_a2_0.X AS COL0, s2_a2_0.Y AS COL1 FROM public.s2 AS s2_a2_0    UNION SELECT s1_a2_0.X AS COL0, s1_a2_0.Y AS COL1 FROM public.s1 AS s1_a2_0   ) AS v_a2_0   ) AS __dummy__   ;

CREATE EXTENSION IF NOT EXISTS plsh;

CREATE TABLE IF NOT EXISTS public.__dummy__v_detected_deletions (txid int, LIKE public.v );
CREATE INDEX IF NOT EXISTS idx__dummy__v_detected_deletions ON public.__dummy__v_detected_deletions (txid);
CREATE TABLE IF NOT EXISTS public.__dummy__v_detected_insertions (txid int, LIKE public.v );
CREATE INDEX IF NOT EXISTS idx__dummy__v_detected_insertions ON public.__dummy__v_detected_insertions (txid);

CREATE OR REPLACE FUNCTION public.v_get_detected_update_data(txid int)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
  DECLARE
  deletion_data text;
  insertion_data text;
  json_data text;
  BEGIN
    insertion_data := (SELECT (array_to_json(array_agg(t)))::text FROM public.__dummy__v_detected_insertions as t where t.txid = $1);
    IF insertion_data IS NOT DISTINCT FROM NULL THEN
        insertion_data := '[]';
    END IF;
    deletion_data := (SELECT (array_to_json(array_agg(t)))::text FROM public.__dummy__v_detected_deletions as t where t.txid = $1);
    IF deletion_data IS NOT DISTINCT FROM NULL THEN
        deletion_data := '[]';
    END IF;
    IF (insertion_data IS DISTINCT FROM '[]') OR (deletion_data IS DISTINCT FROM '[]') THEN
        -- calcuate the update data
        json_data := concat('{"view": ' , '"public.v"', ', ' , '"insertions": ' , insertion_data , ', ' , '"deletions": ' , deletion_data , '}');
        -- clear the update data
        --DELETE FROM public.__dummy__v_detected_deletions;
        --DELETE FROM public.__dummy__v_detected_insertions;
    END IF;
    RETURN json_data;
  END;
$$;

CREATE OR REPLACE FUNCTION public.v_run_shell(text) RETURNS text AS $$
#!/bin/sh
echo "true"
$$ LANGUAGE plsh;


CREATE OR REPLACE FUNCTION public.v_delta_action()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
  DECLARE
  text_var1 text;
  text_var2 text;
  text_var3 text;
  deletion_data text;
  insertion_data text;
  json_data text;
  result text;
  user_name text;
  xid int;
  delta_ins_size int;
  delta_del_size int;
  array_delta_del public.v[];
  array_delta_ins public.v[];
  temprec_delta_del_s1 public.s1%ROWTYPE;
            array_delta_del_s1 public.s1[];
temprec_delta_del_s2 public.s2%ROWTYPE;
            array_delta_del_s2 public.s2[];
temprec_delta_ins_s1 public.s1%ROWTYPE;
            array_delta_ins_s1 public.s1[];
  BEGIN
    IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'v_delta_action_flag') THEN
        -- RAISE LOG 'execute procedure v_delta_action';
        CREATE TEMPORARY TABLE v_delta_action_flag ON COMMIT DROP AS (SELECT true as finish);
        IF EXISTS (SELECT WHERE false )
        THEN
          RAISE check_violation USING MESSAGE = 'Invalid view update: constraints on the view are violated';
        END IF;
        IF EXISTS (SELECT WHERE false )
        THEN
          RAISE check_violation USING MESSAGE = 'Invalid view update: constraints on the source relations are violated';
        END IF;
        SELECT array_agg(tbl) INTO array_delta_ins FROM __tmp_delta_ins_v AS tbl;
        SELECT array_agg(tbl) INTO array_delta_del FROM __tmp_delta_del_v as tbl;
        select count(*) INTO delta_ins_size FROM __tmp_delta_ins_v;
        select count(*) INTO delta_del_size FROM __tmp_delta_del_v;
        
            WITH __tmp_delta_del_v_ar AS (SELECT * FROM unnest(array_delta_del) as array_delta_del_alias limit delta_del_size),
            __tmp_delta_ins_v_ar as (SELECT * FROM unnest(array_delta_ins) as array_delta_ins_alias limit delta_ins_size)
            SELECT array_agg(tbl) INTO array_delta_del_s1 FROM (SELECT (ROW(COL0,COL1) :: public.s1).*
            FROM (SELECT delta_del_s1_a2_0.COL0 AS COL0, delta_del_s1_a2_0.COL1 AS COL1 FROM (SELECT s1_a2_0.X AS COL0, s1_a2_0.Y AS COL1 FROM public.s1 AS s1_a2_0 WHERE NOT EXISTS ( SELECT * FROM (SELECT v_a2_0.X AS COL0, v_a2_0.Y AS COL1 FROM public.v AS v_a2_0 WHERE NOT EXISTS ( SELECT * FROM __tmp_delta_del_v_ar AS __tmp_delta_del_v_ar_a2 WHERE __tmp_delta_del_v_ar_a2.Y = v_a2_0.Y AND __tmp_delta_del_v_ar_a2.X = v_a2_0.X )   UNION SELECT __tmp_delta_ins_v_ar_a2_0.X AS COL0, __tmp_delta_ins_v_ar_a2_0.Y AS COL1 FROM __tmp_delta_ins_v_ar AS __tmp_delta_ins_v_ar_a2_0   ) AS new_v_a2 WHERE new_v_a2.COL1 = s1_a2_0.Y AND new_v_a2.COL0 = s1_a2_0.X )  ) AS delta_del_s1_a2_0   ) AS delta_del_s1_extra_alias) AS tbl;


            WITH __tmp_delta_del_v_ar AS (SELECT * FROM unnest(array_delta_del) as array_delta_del_alias limit delta_del_size),
            __tmp_delta_ins_v_ar as (SELECT * FROM unnest(array_delta_ins) as array_delta_ins_alias limit delta_ins_size)
            SELECT array_agg(tbl) INTO array_delta_del_s2 FROM (SELECT (ROW(COL0,COL1) :: public.s2).*
            FROM (SELECT delta_del_s2_a2_0.COL0 AS COL0, delta_del_s2_a2_0.COL1 AS COL1 FROM (SELECT s2_a2_0.X AS COL0, s2_a2_0.Y AS COL1 FROM public.s2 AS s2_a2_0 WHERE NOT EXISTS ( SELECT * FROM (SELECT v_a2_0.X AS COL0, v_a2_0.Y AS COL1 FROM public.v AS v_a2_0 WHERE NOT EXISTS ( SELECT * FROM __tmp_delta_del_v_ar AS __tmp_delta_del_v_ar_a2 WHERE __tmp_delta_del_v_ar_a2.Y = v_a2_0.Y AND __tmp_delta_del_v_ar_a2.X = v_a2_0.X )   UNION SELECT __tmp_delta_ins_v_ar_a2_0.X AS COL0, __tmp_delta_ins_v_ar_a2_0.Y AS COL1 FROM __tmp_delta_ins_v_ar AS __tmp_delta_ins_v_ar_a2_0   ) AS new_v_a2 WHERE new_v_a2.COL1 = s2_a2_0.Y AND new_v_a2.COL0 = s2_a2_0.X )  ) AS delta_del_s2_a2_0   ) AS delta_del_s2_extra_alias) AS tbl;


            WITH __tmp_delta_del_v_ar AS (SELECT * FROM unnest(array_delta_del) as array_delta_del_alias limit delta_del_size),
            __tmp_delta_ins_v_ar as (SELECT * FROM unnest(array_delta_ins) as array_delta_ins_alias limit delta_ins_size)
            SELECT array_agg(tbl) INTO array_delta_ins_s1 FROM (SELECT (ROW(COL0,COL1) :: public.s1).*
            FROM (SELECT delta_ins_s1_a2_0.COL0 AS COL0, delta_ins_s1_a2_0.COL1 AS COL1 FROM (SELECT new_v_a2_0.COL0 AS COL0, new_v_a2_0.COL1 AS COL1 FROM (SELECT v_a2_0.X AS COL0, v_a2_0.Y AS COL1 FROM public.v AS v_a2_0 WHERE NOT EXISTS ( SELECT * FROM __tmp_delta_del_v_ar AS __tmp_delta_del_v_ar_a2 WHERE __tmp_delta_del_v_ar_a2.Y = v_a2_0.Y AND __tmp_delta_del_v_ar_a2.X = v_a2_0.X )   UNION SELECT __tmp_delta_ins_v_ar_a2_0.X AS COL0, __tmp_delta_ins_v_ar_a2_0.Y AS COL1 FROM __tmp_delta_ins_v_ar AS __tmp_delta_ins_v_ar_a2_0   ) AS new_v_a2_0 WHERE NOT EXISTS ( SELECT * FROM public.s1 AS s1_a2 WHERE s1_a2.Y = new_v_a2_0.COL1 AND s1_a2.X = new_v_a2_0.COL0 ) AND NOT EXISTS ( SELECT * FROM public.s2 AS s2_a2 WHERE s2_a2.Y = new_v_a2_0.COL1 AND s2_a2.X = new_v_a2_0.COL0 )  ) AS delta_ins_s1_a2_0   ) AS delta_ins_s1_extra_alias) AS tbl; 


            IF array_delta_del_s1 IS DISTINCT FROM NULL THEN
                FOREACH temprec_delta_del_s1 IN array array_delta_del_s1  LOOP
                   DELETE FROM public.s1 WHERE X = temprec_delta_del_s1.X AND Y = temprec_delta_del_s1.Y;
                END LOOP;
            END IF;


            IF array_delta_del_s2 IS DISTINCT FROM NULL THEN
                FOREACH temprec_delta_del_s2 IN array array_delta_del_s2  LOOP
                   DELETE FROM public.s2 WHERE X = temprec_delta_del_s2.X AND Y = temprec_delta_del_s2.Y;
                END LOOP;
            END IF;


            IF array_delta_ins_s1 IS DISTINCT FROM NULL THEN
                INSERT INTO public.s1 (SELECT * FROM unnest(array_delta_ins_s1) as array_delta_ins_s1_alias) ;
            END IF;
    END IF;
    RETURN NULL;
  EXCEPTION
    WHEN object_not_in_prerequisite_state THEN
        RAISE object_not_in_prerequisite_state USING MESSAGE = 'no permission to insert or delete or update to source relations of public.v';
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS text_var1 = RETURNED_SQLSTATE,
                                text_var2 = PG_EXCEPTION_DETAIL,
                                text_var3 = MESSAGE_TEXT;
        RAISE SQLSTATE 'DA000' USING MESSAGE = 'error on the trigger of public.v ; error code: ' || text_var1 || ' ; ' || text_var2 ||' ; ' || text_var3;
        RETURN NULL;
  END;
$$;

CREATE OR REPLACE FUNCTION public.v_materialization()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
  DECLARE
  text_var1 text;
  text_var2 text;
  text_var3 text;
  BEGIN
    IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__tmp_delta_ins_v' OR table_name = '__tmp_delta_del_v')
    THEN
        -- RAISE LOG 'execute procedure v_materialization';
        CREATE TEMPORARY TABLE __tmp_delta_ins_v ( LIKE public.v ) WITH OIDS ON COMMIT DROP;
        CREATE CONSTRAINT TRIGGER __tmp_v_trigger_delta_action_ins
        AFTER INSERT OR UPDATE OR DELETE ON
            __tmp_delta_ins_v DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW EXECUTE PROCEDURE public.v_delta_action();

        CREATE TEMPORARY TABLE __tmp_delta_del_v ( LIKE public.v ) WITH OIDS ON COMMIT DROP;
        CREATE CONSTRAINT TRIGGER __tmp_v_trigger_delta_action_del
        AFTER INSERT OR UPDATE OR DELETE ON
            __tmp_delta_del_v DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW EXECUTE PROCEDURE public.v_delta_action();
    END IF;
    RETURN NULL;
  EXCEPTION
    WHEN object_not_in_prerequisite_state THEN
        RAISE object_not_in_prerequisite_state USING MESSAGE = 'no permission to insert or delete or update to source relations of public.v';
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS text_var1 = RETURNED_SQLSTATE,
                                text_var2 = PG_EXCEPTION_DETAIL,
                                text_var3 = MESSAGE_TEXT;
        RAISE SQLSTATE 'DA000' USING MESSAGE = 'error on the trigger of public.v ; error code: ' || text_var1 || ' ; ' || text_var2 ||' ; ' || text_var3;
        RETURN NULL;
  END;
$$;

DROP TRIGGER IF EXISTS v_trigger_materialization ON public.v;
CREATE TRIGGER v_trigger_materialization
    BEFORE INSERT OR UPDATE OR DELETE ON
      public.v FOR EACH STATEMENT EXECUTE PROCEDURE public.v_materialization();

CREATE OR REPLACE FUNCTION public.v_update()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
  DECLARE
  text_var1 text;
  text_var2 text;
  text_var3 text;
  BEGIN
    -- RAISE LOG 'execute procedure v_update';
    IF TG_OP = 'INSERT' THEN
      -- RAISE LOG 'NEW: %', NEW;
      IF (SELECT count(*) FILTER (WHERE j.value = jsonb 'null') FROM  jsonb_each(to_jsonb(NEW)) j) > 0 THEN
        RAISE check_violation USING MESSAGE = 'Invalid update on view: view does not accept null value';
      END IF;
      DELETE FROM __tmp_delta_del_v WHERE ROW(X,Y) = NEW;
      INSERT INTO __tmp_delta_ins_v SELECT (NEW).*;
    ELSIF TG_OP = 'UPDATE' THEN
      IF (SELECT count(*) FILTER (WHERE j.value = jsonb 'null') FROM  jsonb_each(to_jsonb(NEW)) j) > 0 THEN
        RAISE check_violation USING MESSAGE = 'Invalid update on view: view does not accept null value';
      END IF;
      DELETE FROM __tmp_delta_ins_v WHERE ROW(X,Y) = OLD;
      INSERT INTO __tmp_delta_del_v SELECT (OLD).*;
      DELETE FROM __tmp_delta_del_v WHERE ROW(X,Y) = NEW;
      INSERT INTO __tmp_delta_ins_v SELECT (NEW).*;
    ELSIF TG_OP = 'DELETE' THEN
      -- RAISE LOG 'OLD: %', OLD;
      DELETE FROM __tmp_delta_ins_v WHERE ROW(X,Y) = OLD;
      INSERT INTO __tmp_delta_del_v SELECT (OLD).*;
    END IF;
    RETURN NULL;
  EXCEPTION
    WHEN object_not_in_prerequisite_state THEN
        RAISE object_not_in_prerequisite_state USING MESSAGE = 'no permission to insert or delete or update to source relations of public.v';
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS text_var1 = RETURNED_SQLSTATE,
                                text_var2 = PG_EXCEPTION_DETAIL,
                                text_var3 = MESSAGE_TEXT;
        RAISE SQLSTATE 'DA000' USING MESSAGE = 'error on the trigger of public.v ; error code: ' || text_var1 || ' ; ' || text_var2 ||' ; ' || text_var3;
        RETURN NULL;
  END;
$$;

DROP TRIGGER IF EXISTS v_trigger_update ON public.v;
CREATE TRIGGER v_trigger_update
    INSTEAD OF INSERT OR UPDATE OR DELETE ON
      public.v FOR EACH ROW EXECUTE PROCEDURE public.v_update();

CREATE OR REPLACE FUNCTION public.v_propagate_updates ()
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
  BEGIN
    SET CONSTRAINTS __tmp_v_trigger_delta_action_ins, __tmp_v_trigger_delta_action_del IMMEDIATE;
    SET CONSTRAINTS __tmp_v_trigger_delta_action_ins, __tmp_v_trigger_delta_action_del DEFERRED;
    DROP TABLE IF EXISTS v_delta_action_flag;
    DROP TABLE IF EXISTS __tmp_delta_del_v;
    DROP TABLE IF EXISTS __tmp_delta_ins_v;
    RETURN true;
  END;
$$;

