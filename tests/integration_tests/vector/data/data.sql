DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
use test;
DROP TABLE IF EXISTS test.simple1;
DROP TABLE IF EXISTS test.simple2;

CREATE TABLE test.simple1(id int primary key, data VECTOR(5));
-- CREATE VECTOR INDEX idx_name1 USING HNSW ON test.simple1(VEC_COSINE_DISTANCE(data)) ;
INSERT INTO test.simple1(id, data) VALUES (1, "[1,2,3,4,5]");
INSERT INTO test.simple1(id, data) VALUES (2, '[2,3,4,5,6]');
INSERT INTO test.simple1(id, data) VALUES (3, '[0.1,0.2,0.3,0.4,0.5]');
INSERT INTO test.simple1(id, data) VALUES (4, '[0,-0.1,-2,2,0.1]');


CREATE TABLE test.simple2(id int primary key, data VECTOR(5), embedding VECTOR(5) COMMENT "hnsw(distance=cosine)");
INSERT INTO test.simple2(id, data, embedding) VALUES (1, '[1,2,3,4,5]','[1,2,3,4,5]');
INSERT INTO test.simple2(id, data, embedding) VALUES (2, '[2,3,4,5,6]','[1,2,3,4,5]');
INSERT INTO test.simple2(id, data, embedding) VALUES (3, '[0.1,0.2,0.3,0.4,0.5]','[1,2,3,4,5]');
INSERT INTO test.simple2(id, data, embedding) VALUES (4, '[0,-0.1,-2,2,0.1]','[1,2,3,4,5]');

DELETE FROM test.simple1 where id=1;
DELETE FROM test.simple2 where id=1;
DELETE FROM test.simple1 where id=2;
DELETE FROM test.simple2 where id=2;

UPDATE test.simple1 SET data = '[0,-0.1,-2,2.0,0.1]' WHERE id = 3;
UPDATE test.simple2 SET data = '[0,-0.1,-2,2.0,0.1]' WHERE id = 3;

ALTER TABLE test.simple1 ADD column embedding VECTOR(3) COMMENT "hnsw(distance=cosine)";
INSERT INTO test.simple1 (id, data, embedding) VALUES (5, '[1,2,3,4,5]', '[1,2,3]');

ALTER TABLE test.simple2 DROP column embedding;

CREATE TABLE test.finish_mark(id int primary key);