-- KS SQL V1.0.0 Reference Script
-- Optimized for Data Integrity, Bug Prevention, and Efficiency

-- 1. Schema Definition with SERIAL (Auto-Increment)
CREATE TABLE products (
    id SERIAL,
    name TEXT,
    category TEXT,
    price FLOAT,
    stock INT
);

-- 2. Explicit Indexing for O(1) B+Tree Lookups
CREATE INDEX idx_product_name ON products (name);
CREATE INDEX idx_category ON products (category);

-- 3. Data Ingestion using Explicit Column Lists
-- Ensures data integrity even if table schema evolves
INSERT INTO products (name, category, price, stock) VALUES ('Quantum CPU', 'Hardware', 899.99, 50);
INSERT INTO products (name, category, price, stock) VALUES ('Neural Link', 'BioTech', 2500.00, 12);
INSERT INTO products (name, category, price, stock) VALUES ('Titan SSD', 'Storage', 199.50, 100);
INSERT INTO products (name, category, price, stock) VALUES ('Legacy HDD', 'Storage', 45.00, 5);

-- 4. Advanced Querying with AND/OR Logic and Numeric Comparison
-- The engine handles numeric strings ('899.99' > '199.50') correctly.
SELECT name, price
FROM products
WHERE (category = 'Storage' AND price < 100) OR price > 1000;

-- 5. Multi-Column ORDER BY with ASC/DESC
-- Handles mixed numeric/text sorting gracefully.
SELECT category, name, stock
FROM products
ORDER BY category ASC, stock DESC;

-- 6. Aggregate Functions for Real-time Metrics
SELECT COUNT(*), SUM(price), AVG(price)
FROM products
WHERE stock > 0;

-- 7. Universal Search (Titan-Prime Feature)
-- Scans all records across the entire sharded B+Tree.
SEARCH 'Titan';

-- 8. Transaction Management (Snapshot Isolation)
BEGIN;
UPDATE products SET stock = stock - 1 WHERE name = 'Quantum CPU';
-- If another session updates this record, COMMIT will fail with a conflict.
COMMIT;

-- 9. Maintenance: Manual Flush to disk
FLUSH;
