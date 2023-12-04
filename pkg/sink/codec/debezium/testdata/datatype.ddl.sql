CREATE TABLE foo(
  pk              INT PRIMARY KEY,

  col_d_2023      DATE,
  col_d_1000      DATE,
  col_d_9999      DATE,
  col_d_0000      DATE,

  col_dt          DATETIME,
  col_dt_fsp_0    DATETIME(0),
  col_dt_fsp_1    DATETIME(1),
  col_dt_fsp_4    DATETIME(4),
  col_dt_fsp_6    DATETIME(6),
  col_dt_0000     DATETIME(6),

  col_t           TIME,
  col_t_fsp_0     TIME(0),
  col_t_fsp_1     TIME(1),
  col_t_fsp_4     TIME(4),
  col_t_fsp_6     TIME(6),
  col_t_neg       TIME(6),

  col_ts          TIMESTAMP,
  col_ts_fsp_0    TIMESTAMP(0),
  col_ts_fsp_1    TIMESTAMP(1),
  col_ts_fsp_5    TIMESTAMP(5),
  col_ts_fsp_6    TIMESTAMP(6),

  col_y           YEAR,

  col_bit_1       BIT(1),
  col_bit_5       BIT(5),
  col_bit_6       BIT(6),
  col_bit_60      BIT(60),

  col_varchar     VARCHAR(100),
  col_char        CHAR(100),
  col_varbinary   VARBINARY(100),
  col_binary      BINARY(100),
  col_blob        BLOB,

  col_decimal     DECIMAL(10, 5),
  col_numeric     NUMERIC(10, 5),
  col_float       FLOAT,
  col_double      DOUBLE,

  col_int                 INT,
  col_int_unsigned        INT UNSIGNED,
  col_tinyint             TINYINT,
  col_tinyint_unsigned    TINYINT UNSIGNED,
  col_smallint            SMALLINT,
  col_smallint_unsigned   SMALLINT UNSIGNED,
  col_mediumint           MEDIUMINT,
  col_mediumint_unsigned  MEDIUMINT UNSIGNED,
  col_bigint              BIGINT,
  col_bigint_unsigned     BIGINT UNSIGNED,

  col_enum            ENUM('a', 'b', 'c'),
  col_enum_invalid    ENUM('a', 'b', 'c'),
  col_set             SET('a', 'b', 'c'),
  col_set_invalid     SET('a', 'b', 'c'),

  col_json    JSON
);
