USE fk_route_src;

UPDATE parent SET note='n1_v2' WHERE parent_id=1;
UPDATE parent SET note='n1_v2' WHERE parent_id=1;

UPDATE child SET child_data='c20_v2' WHERE child_id=20;

DELETE FROM parent WHERE parent_id=3;

INSERT INTO child(child_id, parent_id, child_data) VALUES (21,2,'c21');
INSERT INTO child(child_id, parent_id, child_data) VALUES (21,2,'c21') ON DUPLICATE KEY UPDATE child_data=VALUES(child_data);
