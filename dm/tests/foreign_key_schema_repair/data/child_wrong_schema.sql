CREATE TABLE child (
  child_id INT PRIMARY KEY,
  parent_id_shadow INT NOT NULL,
  data VARCHAR(64) NOT NULL,
  CONSTRAINT fk_child_parent FOREIGN KEY (parent_id_shadow) REFERENCES parent(parent_id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;
