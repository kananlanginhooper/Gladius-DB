/* Gladius Database Engine
 * @author legolas558
 * @version 0.8.0
 * Licensed under GNU General Public License (GPL)
 *
 * Example SQL file
*/

SHOW DATABASES;

DROP DATABASE gtestdb;

CREATE DATABASE gtestdb;

USE gtestdb;

CREATE TABLE gladius_one(
	id integer auto_increment,
	name varchar(50),
	comment text,
	years int not null default 45,
	primary key(id)	
);

CREATE TABLE gladius_two(
	name varchar(100),
	phone varchar(20)
);

CREATE TABLE textfield_test(
	id integer auto_increment,
	content text
);


RENAME TABLE gladius_one TO gladius_tt, gladius_two TO phonebook;

INSERT INTO gladius_tt VALUES(5, 'Test value 1', 'Sample data');

INSERT INTO gladius_tt VALUES(90, 'Test value 2', 'Quoted '' data', 543);

INSERT INTO gladius_tt VALUES(2, 'Test value 3', 'Sample data');

INSERT INTO gladius_tt VALUES(7, 'Working set', 'very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string very log string');

SELECT COUNT(*) FROM gladius_tt;

SELECT * FROM gladius_tt WHERE ((comment LIKE '%string%') and (years > 30)) OR (name LIKE '%data%');

SELECT * FROM gladius_tt WHERE id < 7 and id>=2 and id<>5;

SELECT * FROM gladius_tt WHERE (comment LIKE '%string%');

SELECT COUNT(*) FROM gladius_tt;

INSERT INTO textfield_test (content) VALUES ('Generosity and perfection are your everlasting goals.');

INSERT INTO textfield_test (content) VALUES ('Your mode of life will be changed for the better because of good news soon.');

SELECT * FROM textfield_test WHERE (content LIKE '%mode%');
