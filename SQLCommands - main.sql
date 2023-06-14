show databases;

use classicmodels;

CREATE TABLE retail_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(255),
    price DECIMAL(10, 2),
    quantity INT,
    customer_name VARCHAR(255),
    ordered_at DATETIME
);

CREATE TABLE retail_data_checkpoint_tbl (
    last_processed_ordered_at DATETIME PRIMARY KEY
);


--SELECT * FROM retail_data WHERE ordered_at > '2023-06-11 00:10:22';
select * from retail_data;

--INSERT INTO retail_data_checkpoint_tbl (last_processed_ordered_at) VALUES ("2023-06-10 21:51:28");
select * from retail_data_checkpoint_tbl;

-----------------------------------------------------------------------------------------------------------------

use retail_data;

--Create Table in Cassandra (CQL)
CREATE TABLE IF NOT EXISTS data_retail (
        id INT PRIMARY KEY,
        product_name TEXT,
        price DECIMAL,
        quantity INT,
        customer_name TEXT,
        ordered_at TIMESTAMP
    )