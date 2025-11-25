SET @@foreign_key_checks=1;
DROP DATABASE IF EXISTS fk_chain;
CREATE DATABASE fk_chain;
USE fk_chain;

CREATE TABLE grandparent (
  gp_id INT PRIMARY KEY,
  note VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE parent (
  parent_id INT PRIMARY KEY,
  gp_id INT NOT NULL,
  info VARCHAR(64) NOT NULL,
  CONSTRAINT fk_parent_gp FOREIGN KEY (gp_id) REFERENCES grandparent(gp_id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE child (
  child_id INT PRIMARY KEY,
  parent_id INT NOT NULL,
  data VARCHAR(64) NOT NULL,
  CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES parent(parent_id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

INSERT INTO grandparent(gp_id, note) VALUES
 (1,'gp1'), (2,'gp2'), (3,'gp3');

INSERT INTO parent(parent_id, gp_id, info) VALUES
 (10,1,'p10'),
 (20,2,'p20'),
 (30,3,'p30');

INSERT INTO child(child_id, parent_id, data) VALUES
 (100,10,'c100'),
 (101,10,'c101'),
 (200,20,'c200'),
 (201,20,'c201'),
 (300,30,'c300');
