-- Prepare the source database
DROP DATABASE IF EXISTS source_db;
CREATE DATABASE source_db;
USE source_db;

-- Create test tables
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    amount DECIMAL(10, 2)
);

-- Insert initial data
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');

INSERT INTO orders VALUES (1, 1, 100.00);
INSERT INTO orders VALUES (2, 2, 200.00);
