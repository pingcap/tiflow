USE `test`;

-- update_pk --

BEGIN; -- Note: multi-row exchange
UPDATE update_pk SET id = 3 WHERE id = 1;
UPDATE update_pk SET id = 1 WHERE id = 2;
UPDATE update_pk SET id = 2 WHERE id = 3;
COMMIT;

BEGIN; -- Note: multi-row update with no order dependency
UPDATE update_pk SET id = 30 WHERE id = 10;
UPDATE update_pk SET id = 40 WHERE id = 20;
COMMIT;

BEGIN; -- Single row update
UPDATE update_pk SET id = 200 WHERE id = 100;
COMMIT;

-- Normal update
UPDATE update_pk SET pad='example1001' WHERE id = 1000;

-- update_uk --
BEGIN; -- Note: multi-row exchange
UPDATE update_uk SET uk = 3 WHERE uk = 1;
UPDATE update_uk SET uk = 1 WHERE uk = 2;
UPDATE update_uk SET uk = 2 WHERE uk = 3;
COMMIT;

BEGIN; -- Note: multi-row update with no order dependency
UPDATE update_uk SET uk = 30 WHERE uk = 10;
UPDATE update_uk SET uk = 40 WHERE uk = 20;
COMMIT;

BEGIN; -- Single row update
UPDATE update_uk SET uk = 200 WHERE uk = 100;
COMMIT;

-- Normal update
UPDATE update_uk SET pad='example1001' WHERE uk = 1000;