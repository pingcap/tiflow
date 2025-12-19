-- Test mixed DDL and DML operations for sink routing
USE source_db;

-- ============================================
-- DML: INSERT more data
-- ============================================
INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');
INSERT INTO orders VALUES (3, 3, 300.00);

-- ============================================
-- DML: UPDATE data
-- ============================================
UPDATE users SET email = 'alice_updated@example.com' WHERE id = 1;
UPDATE orders SET amount = 150.00 WHERE id = 1;

-- ============================================
-- DML: DELETE data
-- ============================================
DELETE FROM orders WHERE id = 2;

-- ============================================
-- DDL: ALTER TABLE ADD COLUMN
-- ============================================
ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- ============================================
-- DDL: CREATE TABLE (new table should be routed)
-- ============================================
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10, 2)
);

INSERT INTO products VALUES (1, 'Widget', 9.99);
INSERT INTO products VALUES (2, 'Gadget', 19.99);

-- ============================================
-- DDL: CREATE TABLE LIKE
-- ============================================
CREATE TABLE products_backup LIKE products;

INSERT INTO products_backup VALUES (1, 'Widget', 9.99);

-- ============================================
-- DDL: ALTER TABLE DROP COLUMN
-- ============================================
ALTER TABLE users DROP COLUMN created_at;

-- ============================================
-- DDL: ALTER TABLE ADD INDEX
-- ============================================
ALTER TABLE orders ADD INDEX idx_user_id (user_id);

-- ============================================
-- DDL: RENAME TABLE
-- ============================================
CREATE TABLE temp_table (
    id INT PRIMARY KEY,
    value VARCHAR(50)
);
INSERT INTO temp_table VALUES (1, 'test');

RENAME TABLE temp_table TO renamed_table;

-- Verify renamed table works with DML
INSERT INTO renamed_table VALUES (2, 'test2');
UPDATE renamed_table SET value = 'updated' WHERE id = 1;

-- ============================================
-- DDL: TRUNCATE TABLE
-- ============================================
CREATE TABLE truncate_test (
    id INT PRIMARY KEY,
    data VARCHAR(100)
);
INSERT INTO truncate_test VALUES (1, 'will be truncated');
INSERT INTO truncate_test VALUES (2, 'also truncated');

TRUNCATE TABLE truncate_test;

-- Insert new data after truncate
INSERT INTO truncate_test VALUES (10, 'after truncate');

-- ============================================
-- DDL: DROP TABLE
-- ============================================
CREATE TABLE to_be_dropped (
    id INT PRIMARY KEY
);
INSERT INTO to_be_dropped VALUES (1);

DROP TABLE to_be_dropped;

-- ============================================
-- Mixed operations on existing tables
-- ============================================
-- More inserts
INSERT INTO users VALUES (4, 'Diana', 'diana@example.com');
INSERT INTO users VALUES (5, 'Eve', 'eve@example.com');

-- Batch update
UPDATE users SET name = CONCAT(name, '_v2') WHERE id IN (3, 4);

-- More deletes
DELETE FROM users WHERE id = 5;

-- Update with multiple columns
UPDATE products SET name = 'Super Widget', price = 12.99 WHERE id = 1;

-- Delete with condition
DELETE FROM products WHERE price < 15.00;

-- ============================================
-- Create finish marker table
-- ============================================
CREATE TABLE finish_mark (id INT PRIMARY KEY);
INSERT INTO finish_mark VALUES (1);
