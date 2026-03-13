USE fk_chain;

-- update a parent and add a child under it
UPDATE parent SET info='p20_updated' WHERE parent_id=20;
INSERT INTO child(child_id, parent_id, data) VALUES (202,20,'c202');

-- move a child to a different parent to exercise FK-based causality
UPDATE child SET parent_id=10, data='c201_moved' WHERE child_id=201;

-- cascade delete from the top of the chain
DELETE FROM grandparent WHERE gp_id=3;

-- delete an intermediate parent to cascade its children
DELETE FROM parent WHERE parent_id=20;

-- add a new branch in the hierarchy
INSERT INTO grandparent(gp_id, note) VALUES (4,'gp4');
INSERT INTO parent(parent_id, gp_id, info) VALUES (40,4,'p40');
INSERT INTO child(child_id, parent_id, data) VALUES (400,40,'c400');

-- update an existing child
UPDATE child SET data='c100_updated' WHERE child_id=100;
