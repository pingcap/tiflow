-- create user and grant all privileges
-- 'test123456' encoded by base64 -> "dGVzdDEyMzQ1Ng=="
CREATE USER 'syncpoint'@'%' IDENTIFIED BY 'test123456';

-- 授予所有数据库的所有权限
GRANT ALL PRIVILEGES ON *.* TO 'syncpoint'@'%' WITH GRANT OPTION;

-- 应用权限更改
FLUSH PRIVILEGES;
