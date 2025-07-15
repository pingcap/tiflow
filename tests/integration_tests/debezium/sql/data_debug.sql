SET GLOBAL time_zone = 'Asia/Shanghai';
CREATE TABLE user_info (
id bigint NOT NULL AUTO_INCREMENT,
user_code varchar(200) NOT NULL,
user_name varchar(200) NOT NULL,
phone varchar(200) DEFAULT NULL,
email varchar(200) DEFAULT NULL,
created_time timestamp NOT NULL,
updated_time datetime DEFAULT NULL,
PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=42857281846452003;

INSERT INTO test.user_info (user_code,user_name,phone,email,created_time,updated_time) VALUES
('123123123112','1111111','2222222','3333333','2025-07-13 15:54:31','2025-07-13 15:38:50');
