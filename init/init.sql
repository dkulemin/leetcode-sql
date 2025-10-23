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
CREATE TABLE IF NOT EXISTS Customer (
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

CREATE TABLE IF NOT EXISTS Views (
    article_id int,
    author_id int,
    viewer_id int,
    view_date date
);
TRUNCATE TABLE Views;
INSERT INTO Views (article_id, author_id, viewer_id, view_date)
VALUES
    ('1', '3', '5', '2019-08-01'),
    ('1', '3', '6', '2019-08-02'),
    ('2', '7', '7', '2019-08-01'),
    ('2', '7', '6', '2019-08-02'),
    ('4', '7', '1', '2019-07-22'),
    ('3', '4', '4', '2019-07-21'),
    ('3', '4', '4', '2019-07-21');

CREATE TABLE IF NOT EXISTS Tweets (
    tweet_id int,
    content VARCHAR
);
TRUNCATE TABLE Tweets;
INSERT INTO Tweets (tweet_id, content)
VALUES
    ('1', 'Let us Code'),
    ('2', 'More than fifteen chars are here!');

CREATE TABLE IF NOT EXISTS Employees (
    id int,
    name VARCHAR
);
TRUNCATE TABLE Employees;
INSERT INTO Employees (id, name)
VALUES
    ('1', 'Alice'),
    ('7', 'Bob'),
    ('11', 'Meir'),
    ('90', 'Winston'),
    ('3', 'Jonathan');

CREATE TABLE IF NOT EXISTS EmployeeUNI (
    id int,
    unique_id int
);
TRUNCATE TABLE EmployeeUNI;
INSERT INTO EmployeeUNI (id, unique_id)
VALUES
    ('3', '1'),
    ('11', '2'),
    ('90', '3');

CREATE TABLE IF NOT EXISTS Sales (
    sale_id int,
    product_id int,
    year int,
    quantity int,
    price int
);
TRUNCATE TABLE Sales;
INSERT INTO Sales (sale_id, product_id, year, quantity, price)
VALUES
    ('1', '100', '2008', '10', '5000'),
    ('2', '100', '2009', '12', '5000'),
    ('7', '200', '2011', '15', '9000');

CREATE TABLE IF NOT EXISTS Product (
    product_id int,
    product_name VARCHAR
);
TRUNCATE TABLE Product;
INSERT INTO Product (product_id, product_name)
VALUES
    ('100', 'Nokia'),
    ('200', 'Apple'),
    ('300', 'Samsung');