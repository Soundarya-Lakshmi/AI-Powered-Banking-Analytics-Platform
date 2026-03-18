-- Preparation: Creating database, stage, file format

CREATE DATABASE banking_db;

CREATE OR REPLACE STAGE bank_file;

CREATE OR REPLACE FILE FORMAT bank_csv
    TYPE='CSV'
    FIELD_DELIMITER=','
    DATE_FORMAT='MM/DD/YYYY'
    SKIP_HEADER=1
    FIELD_OPTIONALLY_ENCLOSED_BY='"';

-- Creating three level of schemas

CREATE SCHEMA banking_db.bronze;
CREATE SCHEMA banking_db.silver;
CREATE SCHEMA banking_db.gold;

-- Creating bronze table and loading the raw data

CREATE OR REPLACE TABLE bronze.raw_banking_data (
    customer_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    age INT,
    gender VARCHAR,
    address VARCHAR,
    city VARCHAR,
    contact_number VARCHAR,
    email VARCHAR,
    account_type VARCHAR,
    account_balance NUMBER(18,2),
    date_of_account_opening DATE,
    last_transaction_date DATE,
    transaction_id VARCHAR,
    transaction_date DATE,
    transaction_type VARCHAR,
    transaction_amount NUMBER(18,2),
    account_balance_after_transaction NUMBER(18,2),
    branch_id VARCHAR,
    loan_id VARCHAR,
    loan_amount NUMBER(18,2),
    loan_type VARCHAR,
    interest_rate NUMBER(18,2),
    loan_term INT,
    approval_rejection_date DATE,
    loan_status VARCHAR,
    card_id VARCHAR,
    card_type VARCHAR,
    credit_limit NUMBER(18,2),
    credit_card_balance NUMBER(18,2),
    minimum_payment_due NUMBER(18,2),
    payment_due_date DATE,
    last_credit_card_payment_date DATE,
    reward_points INT,
    feedback_id VARCHAR,
    feedback_date DATE,
    feedback_type VARCHAR,
    resolution_status VARCHAR,
    resolution_date DATE,
    anomaly INT
    
);

COPY INTO bronze.raw_banking_data
FROM @public.bank_file
FILE_FORMAT = (FORMAT_NAME='bank_csv');

SELECT * FROM raw_banking_data LIMIT 50;

-- DATA CLEANING & VALIDATION

-- Checking if there are invalid emails or ages
SELECT * FROM raw_banking_data WHERE email NOT LIKE '%@%.com';
SELECT * FROM raw_banking_data WHERE age<18 OR age>100;
-- No invalid emails and ages

-- Checking for incorrect spellings
SELECT DISTINCT gender FROM raw_banking_data;
SELECT DISTINCT account_type FROM raw_banking_data;
SELECT DISTINCT transaction_type FROM raw_banking_data;
SELECT DISTINCT loan_type FROM raw_banking_data;
SELECT DISTINCT card_type FROM raw_banking_data;
SELECT DISTINCT feedback_type FROM raw_banking_data;
SELECT DISTINCT resolution_status FROM raw_banking_data;
SELECT DISTINCT anomaly FROM raw_banking_data;
-- No incorrect spellings

-- Checking for duplicates
SELECT * FROM (SELECT ROW_NUMBER() OVER(PARTITION BY CUSTOMER_ID ORDER BY CUSTOMER_ID) AS rn, CUSTOMER_ID FROM raw_banking_data)
WHERE rn>1;
-- No duplicates

-- Checking for invalid account opening dates
SELECT * FROM raw_banking_data WHERE date_of_account_opening > transaction_date;
-- No invalid account opening dates

-- Checking for invalid loan approval dates
SELECT * FROM raw_banking_data WHERE loan_status='approved' AND approval_rejection_date IS NULL;
-- No invalid approval dates

-- Checking credit limit
SELECT * FROM raw_banking_data WHERE credit_card_balance > credit_limit;
-- 855 rows exceeded credit limit

-- Checking invalid resolution dates
SELECT * FROM raw_banking_data WHERE feedback_date > resolution_date;
-- 2534 rows

-- Checking for negative values
SELECT * FROM raw_banking_data WHERE account_balance_after_transaction < 0;
-- 604 rows

-- Checking missed payment deadline
SELECT * FROM raw_banking_data WHERE last_credit_card_payment_date > payment_due_date;
-- 2474 rows with late payments

-- Checking if the account_balance_after_transaction is calculated correctly
WITH CTE AS (SELECT account_balance, transaction_type, transaction_amount, account_balance_after_transaction,
    CASE WHEN transaction_type='Withdrawal' 
    AND account_balance_after_transaction=account_balance-transaction_amount THEN 1
    WHEN transaction_type='Deposit'
    AND account_balance_after_transaction=account_balance+transaction_amount THEN 1
    WHEN transaction_type='Transfer'
    AND (account_balance_after_transaction=account_balance-transaction_amount
    OR account_balance_after_transaction=account_balance+transaction_amount) THEN 1
    ELSE 0 
    END AS score
FROM raw_banking_data)
SELECT * FROM CTE WHERE score=0; 
-- 1703 rows with unexpected values, no error in transfer, 834 errors in withdrawal, 869 errors in Deposit

-- Creating silver table (validated and reporting summary)

CREATE OR REPLACE TABLE silver.banking_validated AS (
    WITH check_balance AS (SELECT *,
    CASE WHEN transaction_type='Withdrawal' 
    AND account_balance_after_transaction=account_balance-transaction_amount THEN 1
    WHEN transaction_type='Deposit'
    AND account_balance_after_transaction=account_balance+transaction_amount THEN 1
    WHEN transaction_type='Transfer'
    AND (account_balance_after_transaction=account_balance-transaction_amount
    OR account_balance_after_transaction=account_balance+transaction_amount) THEN 1
    ELSE 0 
    END AS balance_check
    FROM bronze.raw_banking_data)

    SELECT *,

    CASE WHEN credit_card_balance <= credit_limit
    THEN 'WITHIN_LIMIT'
    ELSE 'LIMIT_EXCEEDED'
    END AS credit_limit_status,

    CASE WHEN feedback_date <= resolution_date
    THEN 'VALID'
    ELSE 'INVALID_DATE'
    END AS feedback_date_status,

    CASE WHEN account_balance_after_transaction < 0
    THEN 'NEGATIVE_BALANCE'
    ELSE 'VALID'
    END AS balance_after_transaction_amount_status,

    CASE WHEN last_credit_card_payment_date <= payment_due_date
    THEN 'ON_TIME'
    ELSE 'LATE_PAYMENT'
    END AS payment_status,

    CASE WHEN balance_check=1
    THEN 'VALID'
    ELSE 'BALANCE_MISMATCH'
    END AS balance_after_transaction_calculation_status
    
    FROM check_balance
);

SELECT * FROM silver.banking_validated LIMIT 10;

-- Creating report summary
CREATE OR REPLACE TABLE silver.banking_data_quality_sumary AS (
    SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN credit_limit_status='LIMIT_EXCEEDED' THEN 1 ELSE 0 END) AS limit_exceeded,
    SUM(CASE WHEN feedback_date_status='INVALID_DATE' THEN 1 ELSE 0 END) AS incorrect_feedback_date,
    SUM(CASE WHEN balance_after_transaction_amount_status='NEGATIVE_BALANCE' THEN 1 ELSE 0 END) AS
    negative_balance_after_transaction,
    SUM(CASE WHEN payment_status='LATE_PAYMENT' THEN 1 ELSE 0 END) AS late_payments,
    SUM(CASE WHEN balance_after_transaction_calculation_status='BALANCE_MISMATCH' THEN 1 ELSE 0 END) AS balance_mismatch
    FROM silver.banking_validated
);

SELECT * FROM silver.banking_data_quality_sumary;


-- Creating gold tables

CREATE OR REPLACE TABLE gold.customers AS
SELECT DISTINCT customer_id,
first_name,
last_name,
age,
gender,
address,
city,
contact_number,
email FROM silver.banking_validated;

CREATE OR REPLACE TABLE gold.accounts AS
SELECT
customer_id,
account_type,
account_balance,
date_of_account_opening,
branch_id
FROM silver.banking_validated;

CREATE OR REPLACE TABLE gold.transactions AS
SELECT
transaction_id,
customer_id,
transaction_date,
transaction_type,
transaction_amount,
account_balance_after_transaction
FROM silver.banking_validated
WHERE balance_after_transaction_calculation_status = 'VALID';

CREATE OR REPLACE TABLE gold.loans AS
SELECT
loan_id,
customer_id,
loan_amount,
loan_type,
interest_rate,
loan_term,
approval_rejection_date,
loan_status
FROM silver.banking_validated;

CREATE OR REPLACE TABLE gold.credit_cards AS
SELECT
card_id,
customer_id,
card_type,
credit_limit,
credit_card_balance,
minimum_payment_due,
payment_due_date,
last_credit_card_payment_date,
reward_points,
credit_limit_status,
payment_status
FROM silver.banking_validated;

CREATE OR REPLACE TABLE gold.feedback AS
SELECT
feedback_id,
customer_id,
feedback_date,
feedback_type,
resolution_status,
resolution_date
FROM silver.banking_validated
WHERE feedback_date_status = 'VALID';

-- Late Payment %
SELECT
    ROUND(
        SUM(CASE WHEN payment_status='LATE_PAYMENT' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS late_payment_percentage
FROM gold.credit_cards;


-- Top customers by transaction amount
SELECT
    customer_id,
    SUM(transaction_amount) AS total_transaction_value
FROM gold.transactions
GROUP BY customer_id
ORDER BY total_transaction_value DESC
LIMIT 10;

-- Creating Streams and Tasks
CREATE OR REPLACE STREAM bronze.bronze_stream
ON TABLE bronze.raw_banking_data;

CREATE OR REPLACE TASK silver.silver_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 HOUR' 
AS INSERT INTO silver.banking_validated
SELECT * FROM raw_banking_data;

ALTER WAREHOUSE compute_wh SET AUTO_SUSPEND = 60;

-- Creating a separate schema analytics for views
CREATE SCHEMA analytics;

-- Creating views

-- Risk Score
CREATE OR REPLACE VIEW analytics.customer_risk_report AS
SELECT
    customer_id,
    CASE WHEN payment_status = 'LATE_PAYMENT' THEN 1 ELSE 0 END +
    CASE WHEN credit_limit_status = 'OVER_LIMIT' THEN 1 ELSE 0 END +
    CASE WHEN (credit_card_balance/credit_limit) > 0.7 THEN 1 ELSE 0 END
    AS risk_score
FROM gold.credit_cards;

SELECT * FROM customer_risk_report WHERE risk_score>=2;

-- Limit Exceeded 
CREATE OR REPLACE VIEW analytics.limit_exceeded_customers AS
SELECT
    customer_id,
    card_id,
    credit_limit,
    credit_card_balance
FROM gold.credit_cards
WHERE credit_limit_status = 'LIMIT_EXCEEDED';

SELECT * FROM limit_exceeded_customers;

-- Credit Utilization Percentage
CREATE OR REPLACE VIEW analytics.credit_utilization AS
SELECT 
    customer_id,
    card_id,
    credit_limit,
    credit_card_balance,
    ROUND((credit_card_balance / credit_limit) * 100,2) AS credit_utilization_percentage,
    CASE WHEN credit_utilization_percentage<=30 THEN '0 to 30%'
    WHEN credit_utilization_percentage>30 AND credit_utilization_percentage<=70 THEN  '31 to 70%'
    WHEN credit_utilization_percentage>70 THEN 'More than 70%'
    END AS credit_utilization_category    
FROM gold.credit_cards;

SELECT credit_utilization_category, COUNT(*) AS customer_count FROM credit_utilization GROUP BY credit_utilization_category;

-- Payment Status
CREATE OR REPLACE VIEW analytics.payment_status AS
SELECT
    payment_status,
    COUNT(*) AS total_accounts
FROM gold.credit_cards
GROUP BY payment_status;

SELECT * FROM payment_status;

-- Transaction type distribution
CREATE OR REPLACE VIEW analytics.transaction_type_summary AS
SELECT
    transaction_type,
    COUNT(*) AS total_transactions,
    SUM(transaction_amount) AS total_amount
FROM gold.transactions
GROUP BY transaction_type;

SELECT * FROM transaction_type_summary;

-- Customer complaint resolution time
CREATE OR REPLACE VIEW analytics.feedback_resolution_time AS
SELECT 
    feedback_type,
    AVG(DATEDIFF(day, feedback_date, resolution_date)) AS avg_resolution_days
    FROM gold.feedback WHERE feedback_type='Complaint'
    GROUP BY feedback_type;

SELECT * FROM feedback_resolution_time;

-- Loan exposure by loan type
CREATE OR REPLACE VIEW analytics.loan_type_distribution AS
SELECT 
    loan_type,
    COUNT(*) AS number_of_loans,
    SUM(loan_amount) AS total_loan_amount
FROM gold.loans GROUP BY loan_type ORDER BY total_loan_amount DESC;

SELECT * FROM loan_type_distribution;

CREATE OR REPLACE VIEW analytics.credit_card_summary AS
SELECT
    customer_id,
    credit_card_balance,
    credit_limit,
    credit_card_balance / credit_limit AS credit_utilization,
    payment_status,
    credit_limit_status,
    CASE WHEN payment_status = 'LATE_PAYMENT' THEN 1 ELSE 0 END +
    CASE WHEN credit_limit_status = 'OVER_LIMIT' THEN 1 ELSE 0 END +
    CASE WHEN (credit_card_balance/credit_limit) > 0.7 THEN 1 ELSE 0 END
    AS risk_score
FROM gold.credit_cards;

SELECT * FROM analytics.credit_card_summary;


-- Creating semantic view for AI
CREATE OR REPLACE VIEW analytics.customer_financial_profile AS
SELECT
    c.customer_id,
    first_name,
    last_name,
    account_balance,
    credit_card_balance,
    credit_limit,
    payment_status,
    credit_limit_status
FROM gold.customers c
JOIN gold.accounts a
ON c.customer_id = a.customer_id
JOIN gold.credit_cards cc
ON c.customer_id = cc.customer_id;

-- Enabling cross region inference
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';




















