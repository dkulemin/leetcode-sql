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

CREATE TABLE IF NOT EXISTS Employee (
    id int,
    name VARCHAR,
    department VARCHAR,
    managerId int
);
TRUNCATE TABLE Employee;
INSERT INTO Employee (id, name, department, managerId)
VALUES
    ('101', 'John', 'A', NULL),
    ('102', 'Dan', 'A', '101'),
    ('103', 'James', 'A', '101'),
    ('104', 'Amy', 'A', '101'),
    ('105', 'Anne', 'A', '101'),
    ('106', 'Ron', 'B', '101'),
    ('111', 'John', 'A', NULL),
    ('102', 'Dan', 'A', '111'),
    ('103', 'James', 'A', '111'),
    ('104', 'Amy', 'A', '111'),
    ('105', 'Anne', 'A', '111'),
    ('106', 'Ron', 'B', '111');

CREATE TABLE IF NOT EXISTS Signups (
    user_id int,
    time_stamp timestamp
);
TRUNCATE TABLE Signups;
INSERT INTO Signups (user_id, time_stamp)
VALUES
    ('3', '2020-03-21 10:16:13'),
    ('7', '2020-01-04 13:57:59'),
    ('2', '2020-07-29 23:09:44'),
    ('6', '2020-12-09 10:39:37');

CREATE TABLE IF NOT EXISTS Confirmations (
    user_id int,
    time_stamp timestamp,
    action VARCHAR
);
TRUNCATE TABLE Confirmations;
INSERT INTO Confirmations (user_id, time_stamp, action)
VALUES
    ('3', '2021-01-06 03:30:46', 'timeout'),
    ('3', '2021-07-14 14:00:00', 'timeout'),
    ('7', '2021-06-12 11:57:29', 'confirmed'),
    ('7', '2021-06-13 12:58:28', 'confirmed'),
    ('7', '2021-06-14 13:59:27', 'confirmed'),
    ('2', '2021-01-22 00:00:00', 'confirmed'),
    ('2', '2021-02-28 23:59:59', 'timeout');

CREATE TABLE IF NOT EXISTS Transactions (
    id int,
    country VARCHAR,
    state VARCHAR,
    amount int,
    trans_date date
);
TRUNCATE TABLE Transactions;
INSERT INTO Transactions (id, country, state, amount, trans_date)
VALUES
    ('121', 'US', 'approved', '1000', '2018-12-18'),
    ('122', 'US', 'declined', '2000', '2018-12-19'),
    ('123', 'US', 'approved', '2000', '2019-01-01'),
    ('124', 'DE', 'approved', '2000', '2019-01-07');

CREATE TABLE IF NOT EXISTS Delivery (
    delivery_id int,
    customer_id int,
    order_date date,
    customer_pref_delivery_date date
);
TRUNCATE TABLE Delivery;
INSERT INTO Delivery (delivery_id, customer_id, order_date, customer_pref_delivery_date)
VALUES
    ('1', '1', '2019-08-01', '2019-08-02'),
    ('2', '2', '2019-08-02', '2019-08-02'),
    ('3', '1', '2019-08-11', '2019-08-12'),
    ('4', '3', '2019-08-24', '2019-08-24'),
    ('5', '3', '2019-08-21', '2019-08-22'),
    ('6', '2', '2019-08-11', '2019-08-13'),
    ('7', '4', '2019-08-09', '2019-08-09');