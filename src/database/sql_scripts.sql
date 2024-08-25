CREATE TABLE Users (
    Userid int PRIMARY KEY,
    Name VARCHAR(100),
    Contact VARCHAR(50),
    Balance DECIMAL(10, 2)
) 
PARTITION BY HASH (userid);


-- Create 4 partitions (you can adjust the number of partitions as needed)
CREATE TABLE users_partition_1 PARTITION OF users FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE users_partition_2 PARTITION OF users FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE users_partition_3 PARTITION OF users FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE users_partition_4 PARTITION OF users FOR VALUES WITH (MODULUS 4, REMAINDER 3);





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
PARTITION BY hash (transactionid);


CREATE TABLE transactions_p0 PARTITION OF transactions
FOR VALUES WITH (MODULUS 4, REMAINDER 0);

CREATE TABLE transactions_p1 PARTITION OF transactions
FOR VALUES WITH (MODULUS 4, REMAINDER 1);

CREATE TABLE transactions_p2 PARTITION OF transactions
FOR VALUES WITH (MODULUS 4, REMAINDER 2);

CREATE TABLE transactions_p3 PARTITION OF transactions
FOR VALUES WITH (MODULUS 4, REMAINDER 3);




create index idx_transactions_date on transactions(date);
CREATE INDEX idx_user_id ON transactions(userId); 


CREATE TABLE updatemetadata 
( id int PRIMARY KEY, last_update_date TIMESTAMP NOT NULL );

INSERT INTO updatemetadata (id, last_update_date )
VALUES (1, '2024-08-20 00:00:00');


