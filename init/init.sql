\c leetcodedb;
CREATE TABLE IF NOT EXISTS Activity (
    user_id int,
    session_id int,
    activity_date date,
    activity_type VARCHAR
);
TRUNCATE TABLE Activity;
INSERT INTO Activity (user_id, session_id, activity_date, activity_type)
VALUES
    ('1', '1', '2019-07-20', 'open_session'),
    ('1', '1', '2019-07-20', 'scroll_down'),
    ('1', '1', '2019-07-20', 'end_session'),
    ('2', '4', '2019-07-20', 'open_session'),
    ('2', '4', '2019-07-21', 'send_message'),
    ('2', '4', '2019-07-21', 'end_session'),
    ('3', '2', '2019-07-21', 'open_session'),
    ('3', '2', '2019-07-21', 'send_message'),
    ('3', '2', '2019-07-21', 'end_session'),
    ('4', '3', '2019-06-25', 'open_session'),
    ('4', '3', '2019-06-25', 'end_session');

CREATE TABLE IF NOT EXISTS Products (
    product_id int,
    low_fats VARCHAR,
    recyclable VARCHAR
);
TRUNCATE TABLE Products;
INSERT INTO Products (product_id, low_fats, recyclable)
VALUES
    ('0', 'Y', 'N'),
    ('1', 'Y', 'Y'),
    ('2', 'N', 'Y'),
    ('3', 'Y', 'Y'),
    ('4', 'N', 'N');
Create table If Not Exists Customer (
    id int,
    name VARCHAR,referee_id int
);
TRUNCATE TABLE Customer;
INSERT INTO Customer (id, name, referee_id) 
VALUES
    ('1', 'Will', NULL),
    ('2', 'Jane', NULL),
    ('3', 'Alex', '2'),
    ('4', 'Bill', NULL),
    ('5', 'Zack', '1'),
    ('6', 'Mark', '2');
