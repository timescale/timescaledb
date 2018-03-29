-------------------------------------------
-- Basic even test, integer.
-------------------------------------------
-- ARRANGE
CREATE TABLE "mediantest_even"(val int);
INSERT INTO "mediantest_even" (val) VALUES
    (1),
    (3),
    (1),
    (3),
    (2);
-- ASCERTAIN
SELECT median(val) FROM mediantest_even;

-------------------------------------------
-- Basic odd test, integer.
-------------------------------------------
-- ARRANGE
CREATE TABLE "mediantest_odd"(val int);
INSERT INTO "mediantest_odd" VALUES
    (3),
    (1),
    (3),
    (2);
-- ASCERTAIN
SELECT median(val) FROM mediantest_odd;

-------------------------------------------
-- Repeated values.
-------------------------------------------
-- ARRANGE
CREATE TABLE "mediantest_repeated"(val int);
INSERT INTO "mediantest_repeated"
SELECT 3 FROM generate_series(0, 1000) as T(i);
-- ASCERTAIN
SELECT median(val) FROM mediantest_repeated;

-------------------------------------------
-- Float test.
-------------------------------------------
-- ARRANGE
CREATE TABLE "mediantest_float"(val float);
INSERT INTO "mediantest_float" VALUES
    (3.0),
    (1.3),
    (90000.0),
    (-3),
    (15.123141),
    (-134141);
-- ASCERTAIN
SELECT median(val::numeric) FROM mediantest_float;

-------------------------------------------
-- Empty table.
-------------------------------------------
-- ARRANGE
CREATE TABLE "mediantest_empty"(val float);
-- ASCERTAIN
SELECT median(val::numeric) FROM mediantest_empty;

-------------------------------------------
-- Gigantic odd table
-------------------------------------------
-- ARRANGE
CREATE TABLE "mediantest_gigantic_odd"(val int);
INSERT INTO mediantest_gigantic_odd (val)
SELECT i FROM generate_series(0, 10001) as T(i);
-- ASCERTAIN
SELECT median(val::numeric) FROM mediantest_gigantic_odd;

-------------------------------------------
-- Gigantic even table
-------------------------------------------
-- ARRANGE
CREATE TABLE "mediantest_gigantic_even"(val int);
INSERT INTO mediantest_gigantic_even (val)
SELECT i FROM generate_series(0, 10000) as T(i);
-- ASCERTAIN
SELECT median(val::numeric) FROM mediantest_gigantic_even;

-------------------------------------------
-- Multiple Selects
-------------------------------------------
-- ARRANGE
CREATE TABLE "mediantest_multiple"(a int, b int);
INSERT INTO mediantest_multiple (a, b)
SELECT i, 1000000 - i FROM generate_series(0, 10000) as T(i);
-- ASCERTAIN
SELECT median(a), median(b) FROM mediantest_multiple;
