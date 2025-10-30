-- DROP TABLES IF EXIST
-- CREATE SCHEMA FOR LOCAL TESTS
CREATE SCHEMA test;

DROP TABLE IF EXISTS test.dim_user;
DROP TABLE IF EXISTS test.stg_user;

-- CREATE TABLES
CREATE TABLE test.dim_user
(
    user_sk      UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_bk      TEXT,
    name         TEXT, -- SCD1 attribute
    country      TEXT, -- SCD2 attribute
    city         TEXT, -- SCD2 attribute
    _valid_from  TIMESTAMP,
    _valid_to    TIMESTAMP
);

CREATE TABLE test.stg_user
(
    user_bk      TEXT PRIMARY KEY,
    name         TEXT,
    country      TEXT,
    city         TEXT
);

-- INSERT INITIAL VALUES
INSERT INTO test.dim_user (user_bk, name, country, city, _valid_from, _valid_to)
VALUES
    ('788d58fb', 'Myles', 'Canada', 'Torronto', '1000-01-01', '9999-12-31 23:59:59'),
    ('23bef18a', 'Neo', 'Ukraine', 'Ternopil', '1000-01-01', '9999-12-31 23:59:59'),
    ('6a94d22b', 'Kim', 'Poland', 'Warsaw', '1000-01-01', '9999-12-31 23:59:59'),
    ('8e7e4f9a', 'Ovan', 'France', 'Paris', '1000-01-01', '2023-05-05 23:59:59'),
    ('8e7e4f9a', 'Ovan', 'France', 'Leon', '2023-05-06', '9999-12-31 23:59:59');

-- INSERT NEW PORTION OF DATA (STAGING)
INSERT INTO test.stg_user (user_bk, name, country, city)
VALUES
    ('5da53bcd', 'Vasyl', 'USA', 'Los-Angeles'),
    ('8e7e4f9a', 'Ovaness', 'France', 'Nice'),
    ('23bef18a', 'Leopold', 'Ukraine', 'Ternopil'),
    ('6a94d22b', 'Kim', 'Poland', 'Warsaw'),
    ('788d58fb', 'Melisa', 'USA', 'New York');

-- TASK: Prepare SQL statements to correctly add new portion of data into the dim_user table
-- Explanations: We expect dim_user to have a valid historical records according to the specified in the table declaration attribute's SCD types

-- for consistent CURRENT_TIMESTAMP
BEGIN;

-- closing old SCD2 records and updating SCD1 attributes.
MERGE INTO test.dim_user T
USING test.stg_user S
ON T.user_bk = S.user_bk AND T._valid_to = '9999-12-31 23:59:59' -- assuming that this 'magic' date used to mark active records

-- an SCD2 attribute changed(marking record as historical with setting _valid_to = CURRENT_TIMESTAMP)
WHEN MATCHED AND (T.country <> S.country OR T.city <> S.city) THEN
  UPDATE SET _valid_to = CURRENT_TIMESTAMP

-- an SCD1 attribute changed and the SCD2 attributes did NOT change(overwriting)
WHEN MATCHED AND (T.country = S.country AND T.city = S.city) AND (T.name <> S.name) THEN
  UPDATE SET name = S.name;

-- inserting new users(not present in dim_user) and records with new versions of SCD2 attributes
INSERT INTO test.dim_user (user_bk, name, country, city, _valid_from, _valid_to)
SELECT
    S.user_bk,
    S.name,
    S.country,
    S.city,
    CURRENT_TIMESTAMP AS _valid_from,
    -- setting the "magic" date as _valid_to
    '9999-12-31 23:59:59' AS _valid_to
FROM test.stg_user S
LEFT JOIN test.dim_user T
  ON S.user_bk = T.user_bk
  AND T._valid_to = '9999-12-31 23:59:59'
WHERE
  -- inserting new users from source
  T.user_bk IS NULL
  OR
  -- inserting records where an SCD2 attribute was changed
  (T.user_bk IS NOT NULL AND (S.country <> T.country OR S.city <> T.city));

COMMIT;
