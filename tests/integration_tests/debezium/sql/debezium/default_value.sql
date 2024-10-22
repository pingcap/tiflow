CREATE TABLE UNSIGNED_TINYINT_TABLE (
  id int PRIMARY KEY,
  A TINYINT UNSIGNED NULL DEFAULT 0,
  B TINYINT UNSIGNED NULL DEFAULT '10',
  C TINYINT UNSIGNED NULL,
  D TINYINT UNSIGNED NOT NULL,
  E TINYINT UNSIGNED NOT NULL DEFAULT 0,
  F TINYINT UNSIGNED NOT NULL DEFAULT '0',
  G TINYINT UNSIGNED NULL DEFAULT '100'
);
INSERT INTO UNSIGNED_TINYINT_TABLE VALUES (1, DEFAULT, DEFAULT, 0, 1, DEFAULT, DEFAULT, NULL);

CREATE TABLE UNSIGNED_SMALLINT_TABLE (
  id int PRIMARY KEY,
  A SMALLINT UNSIGNED NULL DEFAULT 0,
  B SMALLINT UNSIGNED NULL DEFAULT '10',
  C SMALLINT UNSIGNED NULL,
  D SMALLINT UNSIGNED NOT NULL,
  E SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  F SMALLINT UNSIGNED NOT NULL DEFAULT '0',
  G SMALLINT UNSIGNED NULL DEFAULT '100'
);
INSERT INTO UNSIGNED_SMALLINT_TABLE VALUES (1, 1, 1, 1, 0, 1, 1, NULL);

CREATE TABLE UNSIGNED_MEDIUMINT_TABLE (
  id int PRIMARY KEY,
  A MEDIUMINT UNSIGNED NULL DEFAULT 0,
  B MEDIUMINT UNSIGNED NULL DEFAULT '10',
  C MEDIUMINT UNSIGNED NULL,
  D MEDIUMINT UNSIGNED NOT NULL,
  E MEDIUMINT UNSIGNED NOT NULL DEFAULT 0,
  F MEDIUMINT UNSIGNED NOT NULL DEFAULT '0',
  G MEDIUMINT UNSIGNED NULL DEFAULT '100'
);
INSERT INTO UNSIGNED_MEDIUMINT_TABLE VALUES (1, 1, 1, 1, 0, 1, 1, NULL);

CREATE TABLE UNSIGNED_INT_TABLE (
  id int PRIMARY KEY,
  A INT UNSIGNED NULL DEFAULT 0,
  B INT UNSIGNED NULL DEFAULT '10',
  C INT UNSIGNED NULL,
  D INT UNSIGNED NOT NULL,
  E INT UNSIGNED NOT NULL DEFAULT 0,
  F INT UNSIGNED NOT NULL DEFAULT '0',
  G INT UNSIGNED NULL DEFAULT '100'
);
INSERT INTO UNSIGNED_INT_TABLE VALUES (1, 1, 1, 1, 0, 1, 1, NULL);

CREATE TABLE UNSIGNED_BIGINT_TABLE (
  id int PRIMARY KEY,
  A BIGINT UNSIGNED NULL DEFAULT 0,
  B BIGINT UNSIGNED NULL DEFAULT '10',
  C BIGINT UNSIGNED NULL,
  D BIGINT UNSIGNED NOT NULL,
  E BIGINT UNSIGNED NOT NULL DEFAULT 0,
  F BIGINT UNSIGNED NOT NULL DEFAULT '0',
  G BIGINT UNSIGNED NULL DEFAULT '100'
);
INSERT INTO UNSIGNED_BIGINT_TABLE VALUES (1, 1, 1, 1, 0, 1, 1, NULL);

CREATE TABLE STRING_TABLE (
  id int PRIMARY KEY,
  A CHAR(1) NULL DEFAULT 'A',
  B CHAR(1) NULL DEFAULT 'b',
  C VARCHAR(10) NULL DEFAULT 'CC',
  D NCHAR(10) NULL DEFAULT '10',
  E NVARCHAR(10) NULL DEFAULT '0',
  F CHAR(1) DEFAULT NULL,
  G VARCHAR(10) DEFAULT NULL,
  H NCHAR(10) DEFAULT NULL,
  I VARCHAR(10) NULL DEFAULT '100'
);
INSERT INTO STRING_TABLE
VALUES (1, DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT, NULL);

CREATE TABLE BIT_TABLE (
  id int PRIMARY KEY,
  A BIT(1) NULL DEFAULT NULL,
  B BIT(1) DEFAULT 0,
  C BIT(1) DEFAULT 1,
  D BIT(1) DEFAULT b'0',
  E BIT(1) DEFAULT b'1',
  F BIT(1) DEFAULT TRUE,
  G BIT(1) DEFAULT FALSE,
  H BIT(10) DEFAULT b'101000010',
  I BIT(10) DEFAULT NULL,
  J BIT(25) DEFAULT b'10110000100001111',
  K BIT(25) DEFAULT b'10110000100001111'
);
INSERT INTO BIT_TABLE
VALUES (1, false ,DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT, DEFAULT ,NULL ,DEFAULT, NULL);

CREATE TABLE BOOLEAN_TABLE (
  id int PRIMARY KEY,
  A BOOL NULL DEFAULT 0,
  B BOOLEAN NOT NULL DEFAULT '1',
  C BOOLEAN NOT NULL DEFAULT '1',
  D BOOLEAN NOT NULL DEFAULT TRUE,
  E BOOLEAN DEFAULT NULL,
  F BOOLEAN DEFAULT TRUE
);
INSERT INTO BOOLEAN_TABLE
VALUES (1, TRUE ,TRUE ,TRUE ,DEFAULT ,TRUE, NULL);

CREATE TABLE NUMBER_TABLE (
  id int PRIMARY KEY,
  A TINYINT NULL DEFAULT 10,
  B SMALLINT NOT NULL DEFAULT '5',
  C INTEGER NOT NULL DEFAULT 0,
  D BIGINT NOT NULL DEFAULT 20,
  E INT NULL DEFAULT NULL,
  F INT NULL DEFAULT 30,
  G TINYINT(1) NOT NULL DEFAULT TRUE,
  H INT(1) NOT NULL DEFAULT TRUE
);
INSERT INTO NUMBER_TABLE
VALUES (1, DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT, NULL, DEFAULT, DEFAULT);

CREATE TABLE FlOAT_DOUBLE_TABLE (
  id int PRIMARY KEY,
  F FLOAT NULL DEFAULT 0,
  G DOUBLE NOT NULL DEFAULT 1.0,
  H DOUBLE NULL DEFAULT 3.0
);
INSERT INTO FlOAT_DOUBLE_TABLE
VALUES (1, DEFAULT, DEFAULT, NULL);

CREATE TABLE REAL_TABLE (
  id int PRIMARY KEY,
  A REAL NOT NULL DEFAULT 1,
  B REAL NULL DEFAULT NULL,
  C REAL NULL DEFAULT 3
);
INSERT INTO REAL_TABLE
VALUES (1, DEFAULT ,DEFAULT, NULL);

CREATE TABLE NUMERIC_DECIMAL_TABLE (
  id int PRIMARY KEY,
  A NUMERIC(3, 2) NOT NULL DEFAULT 1.23,
  B DECIMAL(4, 3) NOT NULL DEFAULT 2.321,
  C NUMERIC(7, 5) NULL DEFAULT '12.678',
  D NUMERIC(7, 5) NULL DEFAULT '15.28'
);
INSERT INTO NUMERIC_DECIMAL_TABLE
VALUES (1, 1.33 ,2.111 , 3.444, NULL);

CREATE TABLE DATE_TIME_TABLE (
  id int PRIMARY KEY,
  A DATE NOT NULL DEFAULT '1976-08-23',
  B TIMESTAMP DEFAULT '1970-01-01 00:00:01',
  C DATETIME DEFAULT '2018-01-03 00:00:10',
  D DATETIME(1) DEFAULT '2018-01-03 00:00:10.7',
  E DATETIME(6) DEFAULT '2018-01-03 00:00:10.123456',
  F YEAR NOT NULL DEFAULT 1,
  G TIME DEFAULT '00:00:00',
  H TIME(1) DEFAULT '23:00:00.7',
  I TIME(6) DEFAULT '23:00:00.123456',
  J TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  K TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  L TIME(1) DEFAULT '-23:45:56.7',
  M TIME(6) DEFAULT '123:00:00.123456'
);
INSERT INTO DATE_TIME_TABLE
VALUES (1, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, NULL, DEFAULT, DEFAULT);

CREATE TABLE DBZ_771_CUSTOMERS (
  id INTEGER NOT NULL PRIMARY KEY,
  CUSTOMER_TYPE ENUM ('b2c','b2b') NOT NULL default 'b2c'
);

INSERT INTO DBZ_771_CUSTOMERS
VALUES (1, 'b2b');