USE fk_route_src;

UPDATE parent SET note='n1_v2' WHERE parent_id=1;
UPDATE parent SET note='n1_v2' WHERE parent_id=1;

UPDATE child SET child_data='c20_v2' WHERE child_id=20;

DELETE FROM parent WHERE parent_id=3;

INSERT INTO child(child_id, parent_id, child_data) VALUES (21,2,'c21');
INSERT INTO child(child_id, parent_id, child_data) VALUES (21,2,'c21') ON DUPLICATE KEY UPDATE child_data=VALUES(child_data);

-- These DMLs interleave parent and child operations for the same FK key.
-- If the parent delete reaches downstream before the child move, cascade
-- removes child 50 and the final child assertion fails.
INSERT INTO parent(parent_id, payload, note) VALUES (5,'p5','n5'), (6,'p6','n6');
INSERT INTO child(child_id, parent_id, child_data) VALUES (50,5,'c50');
UPDATE child SET parent_id=6, child_data='c50_moved' WHERE child_id=50;
DELETE FROM parent WHERE parent_id=5;
