# Data-Engineering-Capstone Project

## Project Overview
The project's primary goal is to manage an ETL process for both a Loan Application dataset and a Credit Card Dataset using tools such as Python (with Pandas, Matplotlib, and other visualization libraries), SQL, and Apache Spark (comprising Spark Core, SparkSQL, and PySpark).

### Requirements:
- Project is uploaded to a Github Repository (with a minimum of 1 branch) with minimum of 3 commits to the repository.
- `Readme.md` file documents the project details, including technical challenges.
- Python codes, PySpark codes, database scripts, and databases are uploaded to the GitHub repository.
- Include screenshots of all generated graphs.

## Objectives

### 1. Load Credit Card Database
   - **Python and PySpark SQL Program**: Create a program named "Credit Card System" to read and extract data from the following JSON files as per the mapping document:
     - CDW_SAPP_BRANCH.JSON
     - CDW_SAPP_CREDITCARD.JSON
     - CDW_SAPP_CUSTOMER.JSON
   - **Data Load to RDBMS(SQL)**:
     - Initiate a MySQL database named "creditcard_capstone".
     - Develop a Python and PySpark program to write data into RDBMS.

### 2. Python Console-based Program
This program targets Transaction Details and Customer Details.

   - **Transaction Details**:
     - Display transactions by customers based on given zip code, month, and year. Arrange by day in descending order.
     - Showcase the number and total value of transactions for a specific type.
     - Illustrate the total number and value of transactions for branches in a particular state.
   - **Customer Details**:
     - View a customer's current account details.
     - Update a customer's account details.
     - Generate a monthly bill based on a credit card number, month, and year.
     - Display transactions made by a customer between two specified dates, ordered by year, month, and day in descending order.

### 3. Data Visualization
   - Graph the transaction type with the most significant transaction count.
   - Graph the state with the highest customer count.
   - Chart the sum of all transactions for the top 10 customers and identify the customer with the largest transaction amount.

### 4. Loan API Endpoint
   - Develop a python program to GET data from the loan application dataset API endpoint.
   - Determine the status code of the said API endpoint.
   - Utilize PySpark to upload data into RDBMS(SQL) under the table "CDW_SAPP_loan_application".

### 5. Visualization and Analysis of Loan Application Data:
   - Visualize the approval percentage for self-employed applicants.
   - Calculate the rejection percentage for married male applicants.
   - Graph the top three months based on transaction volume.
   - Chart the branch with the highest total dollar value in healthcare transactions.

## Data Sources
- Credit Card Dataset
- Loan Application Dataset

## Technical Challenges
1. Data Format Inconsistencies: JSON files and the API endpoint returned data in different formats. This required significant data transformation tasks like date standardizations and null value handling.

2. Mapping & ETL Complexities: The mapping document posed challenges as it required precise field-to-field mapping from the source to the database. Careful data extraction and transformation logic, like constructing the FULL_STREET_ADDRESS and formatted CUST_PHONE, were needed.

3. Data Quality Assurance: Addressing data anomalies, such as setting default values for missing fields or standardizing textual content, was essential to maintain data integrity.

4. Data Type Handling: Various fields needed data type casting, for instance, merging the YEAR, MONTH, and DAY fields or transforming phone numbers.

5. API Data Integration: Fetching data from the API endpoint required additional validation to ensure data quality and format consistency with existing datasets.

6. Performance & Efficiency: Given the potential size of the datasets, optimizing the data processing functions and managing PySpark's in-memory computations were critical.

7. Testing & Validation: After all transformations, it was pivotal to ensure the data loaded into the databases matched the expected schema and values.


