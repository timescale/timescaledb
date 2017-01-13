BEGIN;
\COPY "33_testNs" FROM 'import_data/batch1_dev1.tsv' NULL AS '';
\COPY "33_testNs" FROM 'import_data/batch1_dev2.tsv' NULL AS '';
\COPY "33_testNs" FROM 'import_data/batch2_dev1.tsv' NULL AS '';
COMMIT;
