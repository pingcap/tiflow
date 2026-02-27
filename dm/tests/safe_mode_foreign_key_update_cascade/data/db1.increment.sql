USE fk_update_demo;

-- Update parent's PK and let downstream cascade-update children's FK.
UPDATE parent SET parent_id=101, payload='p1_v2' WHERE parent_id=1;
