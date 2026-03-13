SET @@foreign_key_checks=1;
DROP DATABASE IF EXISTS fk_demo;
CREATE DATABASE fk_demo;
USE fk_demo;

CREATE TABLE parent (
  parent_id INT PRIMARY KEY,
  payload   VARCHAR(100) NOT NULL,
  UNIQUE KEY uk_payload (payload)
) ENGINE=InnoDB;

CREATE TABLE child (
  child_id  INT PRIMARY KEY,
  parent_id INT NOT NULL,
  child_data VARCHAR(100) NOT NULL,
  CONSTRAINT fk_child_parent
    FOREIGN KEY (parent_id) REFERENCES parent(parent_id)
    ON DELETE CASCADE
) ENGINE=InnoDB;

INSERT INTO parent(parent_id, payload) VALUES
 (1,'p1'), (2,'p2'), (3,'p3');

INSERT INTO child(child_id, parent_id, child_data) VALUES
 (10,1,'c10'), (11,1,'c11'),
 (20,2,'c20'),
 (30,3,'c30');