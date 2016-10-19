--load 'auto_explain';
--set auto_explain.log_nested_statements = true;
--set auto_explain.log_min_duration = 0;

--BEGIN TRANSACTION;
SELECT * FROM unit_tests.begin(6);
--ROLLBACK TRANSACTION;
