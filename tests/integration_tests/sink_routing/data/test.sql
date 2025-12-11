-- Test DML operations
USE source_db;

-- Insert more data
INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');
INSERT INTO orders VALUES (3, 3, 300.00);

-- Update data
UPDATE users SET email = 'alice_updated@example.com' WHERE id = 1;
UPDATE orders SET amount = 150.00 WHERE id = 1;

-- Additional DDL: Add a column
ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Create a new table (should also be routed)
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10, 2)
);

INSERT INTO products VALUES (1, 'Widget', 9.99);
INSERT INTO products VALUES (2, 'Gadget', 19.99);

-- Create finish marker table
CREATE TABLE finish_mark (id INT PRIMARY KEY);
INSERT INTO finish_mark VALUES (1);
