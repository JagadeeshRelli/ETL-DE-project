CREATE TABLE Users (
    Userid int PRIMARY KEY,
    Name VARCHAR(100),
    Contact VARCHAR(50),
    Balance DECIMAL(10, 2)
) 


CREATE TABLE Transactions (
    Transactionid INT PRIMARY KEY,
    Userid INT,
    Amount DECIMAL(10, 2),
    Type VARCHAR(50),
    Date TIMESTAMP,
    Status VARCHAR(50),
    Method VARCHAR(50),
    FOREIGN KEY (UserID) REFERENCES Users(UserID)
)



create index idx_transactions_date on transactions(date);

CREATE TABLE updatemetadata 
( id int PRIMARY KEY, last_update_date TIMESTAMP NOT NULL );

INSERT INTO updatemetadata (id, last_update_date )
VALUES (1, '2024-08-20 00:00:00');


