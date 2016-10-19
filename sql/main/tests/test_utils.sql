CREATE SCHEMA IF NOT EXISTS test_utils;

CREATE OR REPLACE FUNCTION test_utils.create_namespace(name text, project_id bigint, topic text, partitioning_field text)
RETURNS namespaces
AS
$BODY$
DECLARE
  ns namespaces%rowtype;
BEGIN
  INSERT INTO namespaces(name, project_id, partitioning_field) VALUES (name, project_id, partitioning_field) 
    RETURNING * INTO STRICT ns;
  INSERT INTO kafka_storage_records(namespace_id, topic, start_offset, end_offset) VALUES (ns.namespace_id, topic, 0, 0);
  return ns;
END
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION test_utils.create_namespace_field(namespace_id bigint, name text, field_type text, is_distinct boolean)
RETURNS namespace_fields
AS
$BODY$
DECLARE
  nsf namespace_fields;
BEGIN
  INSERT INTO namespace_fields VALUES (namespace_id, field_type, name) RETURNING * INTO STRICT nsf;
  IF is_distinct THEN
    INSERT INTO namespace_labels(namespace_id, name, value) VALUES(namespace_id, name||':distinct', 'true');
  END IF;
  return nsf;
END
$BODY$
LANGUAGE plpgsql;




