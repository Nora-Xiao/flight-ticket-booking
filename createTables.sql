CREATE TABLE Users (
	username varchar(20) PRIMARY KEY,
	password_hash varbinary(20),
	password_salt varbinary(20),
	balance int);

CREATE TABLE Reservations (
	id int PRIMARY KEY,
	itinerary varchar(max),
	date int,
	fid1 int, 
	fid2 int,
	total_price int,
	paid int, -- 1 for paid, 0 for unpaid
	username varchar(20) NOT NULL REFERENCES Users);

CREATE TABLE ID (
	next_id int);

CREATE TABLE Booked (
	fid int PRIMARY KEY,
	booked int);

INSERT INTO Users
VALUES (?, ?, ?, ?);

INSERT INTO Reservations
VALUES (?, ?, ?, ?, ?, ?, ?, ?);

INSERT INTO ID
VALUES (?);

INSERT INTO Booked
VALUES (?, ?);
