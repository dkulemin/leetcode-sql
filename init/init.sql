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