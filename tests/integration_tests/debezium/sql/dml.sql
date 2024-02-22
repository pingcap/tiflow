CREATE TABLE foo(
  PK INT PRIMARY KEY,
  COL INT
);

INSERT INTO foo VALUES (1, 1);

INSERT INTO foo VALUES (2, 2);

INSERT INTO foo VALUES (3, 3);

/* Update PK */
UPDATE foo SET PK = 5, COL = 5 WHERE COL = 3;

/* Update Multiple Rows */
UPDATE foo SET COL = 4;

/* Update Single Row */
UPDATE foo SET COL = 1 WHERE PK = 5;

/* Update No Rows */
UPDATE foo SET COL = 1 WHERE PK = 100;

DELETE FROM foo WHERE PK = 3;
