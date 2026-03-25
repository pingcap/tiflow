SET @@foreign_key_checks=1;
DROP DATABASE IF EXISTS fk_route_dst;
CREATE DATABASE fk_route_dst;
USE fk_route_dst;

CREATE TABLE parent_r (
  parent_id INT PRIMARY KEY,
  payload   VARCHAR(100) NOT NULL,
  note      VARCHAR(100) NOT NULL,
  UNIQUE KEY uk_payload (payload)
) ENGINE=InnoDB;

CREATE TABLE child_r (
  child_id   INT PRIMARY KEY,
  parent_id  INT NOT NULL,
  child_data VARCHAR(100) NOT NULL,
  CONSTRAINT fk_child_parent
    FOREIGN KEY (parent_id) REFERENCES parent_r(parent_id)
    ON DELETE CASCADE
) ENGINE=InnoDB;
