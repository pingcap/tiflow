USE `test`;

INSERT INTO test_update (id, uk, other)
VALUES
    (1, 'uk1', 'other1'),
    (2, 'uk2', 'other2'),
    (3, 'uk3', 'other3'),
    (4, 'uk4', 'other4');

-- update pk
UPDATE test_update SET id = 100 WHERE id = 1;

-- update uk
UPDATE test_update SET uk = 'new_uk' WHERE id = 2;

-- update other column
UPDATE test_update SET other = 'new_other' WHERE id = 3;

-- update pk and uk
UPDATE test_update SET id = 200, uk = 'new_uk4' WHERE id = 4;
