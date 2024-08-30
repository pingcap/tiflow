DROP DATABASE IF EXISTS multi_down_addresses;
CREATE DATABASE multi_down_addresses;
USE multi_down_addresses;

CREATE TABLE changefeed_create (round INT PRIMARY KEY, val INT);
CREATE TABLE changefeed_update (round INT PRIMARY KEY, val INT);
