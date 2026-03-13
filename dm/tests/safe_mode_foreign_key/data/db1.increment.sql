USE fk_demo;

-- parent non-key update should not delete children
UPDATE parent SET payload='p1_v2' WHERE parent_id=1;
-- replaying the same update keeps data intact
UPDATE parent SET payload='p1_v2' WHERE parent_id=1;

-- child update should behave normally
UPDATE child SET child_data='c20_v2' WHERE child_id=20;

-- deleting parent cascades to children
DELETE FROM parent WHERE parent_id=3;

-- insert new child and replay to assert idempotence
INSERT INTO child(child_id, parent_id, child_data) VALUES (21,2,'c21');
INSERT INTO child(child_id, parent_id, child_data) VALUES (21,2,'c21') ON DUPLICATE KEY UPDATE child_data=VALUES(child_data);