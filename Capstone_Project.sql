#Functional Requirements 2.1
#Transaction Details Module
#1. Display the transactions made by customers living in a given zip code for a given month and year.alter
SELECT C.FIRST_NAME, C.MIDDLE_NAME, C.LAST_NAME, CC.TIMEID, CC.TRANSACTION_TYPE, CC.TRANSACTION_VALUE, c.CUST_ZIP
        FROM CDW_SAPP_CUSTOMER C 
        INNER JOIN CDW_SAPP_CREDIT_CARD CC ON C.SSN = CC.CUST_SSN
        WHERE c.CUST_ZIP = '60091' 
        AND TIMEID LIKE '201801%'
        ORDER BY TIMEID DESC;
        
#2. Display the number and total values of transactions for a given type.
SELECT TRANSACTION_TYPE, COUNT(TRANSACTION_ID) AS NUMBER_OF_TRANSACTION, SUM(TRANSACTION_VALUE) AS TOTAL_VALUE_OF_TRANSACTION
    FROM CDW_SAPP_CREDIT_CARD
    WHERE TRANSACTION_TYPE = 'BILLS';
    
    
#3. Display the total number and total values of transactions for branches in a given state.
SELECT B.BRANCH_STATE, COUNT(CC.TRANSACTION_ID) AS TOTAL_TRANSACTIONS, SUM(CC.TRANSACTION_VALUE) AS TOTAL_VALUE
    FROM CDW_SAPP_BRANCH B
    INNER JOIN CDW_SAPP_CREDIT_CARD CC ON B.BRANCH_CODE = CC.BRANCH_CODE
    WHERE B.BRANCH_STATE = 'NY'
    GROUP BY B.BRANCH_STATE;
    
#Functional Requirements 2.2 
#Customer Details Module 
#1. check the existing account details of a customer.
SELECT *
        FROM CDW_SAPP_CUSTOMER
        WHERE SSN = '123456100';
        
#2. Used to modify the existing account details of a customer.
UPDATE CDW_SAPP_CUSTOMER
    SET CUST_CITY = '{new_city}'
    WHERE SSN = '{customer_ssn}';
    
#3. Generate a monthly bill for a credit card number for a given month and year.
SELECT CUST_SSN, CUST_CC_NO, SUM(TRANSACTION_VALUE) AS TOTAL_BILL, COUNT(TRANSACTION_ID) AS NUMBER_OF_TRANSACTIONS
        FROM CDW_SAPP_CREDIT_CARD
        WHERE CUST_CC_NO = '4210653349028689' 
        AND TIMEID LIKE '201803%'
        GROUP BY CUST_SSN;
        
#4. Display the transactions made by a customer between two dates. Order by year, month, and day in descending order
	SELECT *
        FROM CDW_SAPP_CREDIT_CARD
        WHERE CUST_SSN = '123456100' 
        AND TIMEID BETWEEN '20180101' AND '20180130'
        ORDER BY TIMEID DESC;
        
#Functional Requirements 3 
#1. which transaction type has the highest transaction count
	SELECT TRANSACTION_TYPE, COUNT(*) AS TRANSACTION_COUNT
        FROM CDW_SAPP_CREDIT_CARD
        GROUP BY TRANSACTION_TYPE
        ORDER BY TRANSACTION_COUNT DESC;
        
#2. Which state has a high number of customers.
SELECT CUST_STATE, COUNT(DISTINCT SSN) AS CUSTOMER_COUNT
        FROM CDW_SAPP_CUSTOMER
        GROUP BY CUST_STATE
        ORDER BY CUSTOMER_COUNT DESC;
        
#3. Sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
SELECT CUST_SSN, SUM(TRANSACTION_VALUE) AS TOTAL_TRANSACTION_VALUE
        FROM CDW_SAPP_CREDIT_CARD
        GROUP BY CUST_SSN
        ORDER BY TOTAL_TRANSACTION_VALUE DESC
        LIMIT 10;
        