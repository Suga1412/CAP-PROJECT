# Data-Engineering-Capstone Project

## Project Overview
The project's primary goal is to manage an ETL process for both Credit Card Dataset and Loan Application dataset using tools such as Python (Pandas, advanced modules, e.g., Matplotlib), SQL, Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries. 

## Data Sources
- Credit Card Dataset
- Loan Application Dataset
  
## Workflow Diagram of the Requirements
![image](https://github.com/Suga1412/CAP-PROJECT/assets/104521056/196eded4-eeeb-405b-9310-01f8c6234cbd)

### 1. Credit Card Dataset 
   - **Python and PySpark SQL Program**: Create a program named "Credit Card System" to read and extract data from the following JSON files as per the mapping document:
     - CDW_SAPP_BRANCH.JSON
     - CDW_SAPP_CREDITCARD.JSON
     - CDW_SAPP_CUSTOMER.JSON
   - **Data Load to RDBMS(SQL)**:
     - Initiate a MySQL database named "creditcard_capstone".
     - Develop a Python and PySpark program to write data into RDBMS.

### 2. Loan Application Data API
   - Develop a python program to GET data from the loan application dataset API endpoint.
   - Determine the status code of the said API endpoint.
   - Utilize PySpark to upload data into RDBMS(SQL) under the table "CDW_SAPP_loan_application".

## Technical Challenges
1. Mapping & ETL Complexities: The mapping document posed challenges as it required precise field-to-field mapping from the source to the database. Careful data extraction and transformation logic, like constructing the FULL_STREET_ADDRESS and formatted CUST_PHONE, were needed.

2. Data Quality Assurance: Addressing data anomalies, such as setting default values for missing fields or standardizing textual content, was essential to maintain data integrity.

3. Data Type Handling: Various fields needed data type casting, for instance, merging the YEAR, MONTH, and DAY fields or transforming phone numbers.

4. Performance & Efficiency: Given the potential size of the datasets, optimizing the data processing functions and managing PySpark's in-memory computations were critical.

5. Testing & Validation: After all transformations, it was pivotal to ensure the data loaded into the databases matched the expected schema and values.
   
6. Graph Plotting & Visualization: Representing the data graphically posed challenges in terms of choosing the right visualization types, ensuring data accuracy in the plots, and optimizing the rendering performance, especially for large datasets.


## Visualization
Requirement No. 1 - Transaction type with highest trasaction count
![Req_3 1_Transaction_Type_With_Highest_Transaction_Count](https://github.com/Suga1412/CAP-PROJECT/assets/104521056/d855cd51-d92f-43df-9ec9-c2f8d298aefd)

Requirement No. 2 - Number of customers by state
![Req_3 2_Number_of_Customer_by_State](https://github.com/Suga1412/CAP-PROJECT/assets/104521056/b6cfd01f-4432-41e0-9e8b-408e0fcea113)

Requirement No. 3 - Top 10 customers by total transaction amount
![Req_3 3_Top_10_Customers_by_Total_Transaction_Amount](https://github.com/Suga1412/CAP-PROJECT/assets/104521056/493e4dd0-8e47-4a73-8878-21d7e00799e9)

Requirement No. 4 - Loan application of self employed applicants
![Req_5 1_Application_Approved_For_Self_Employed](https://github.com/Suga1412/CAP-PROJECT/assets/104521056/a0c8751e-3a8d-4f11-bdda-26252656b5cb)

Requirement No. 5 - Loan application of married applicants
![Req_5 2_Loan_Rejection_For_Married_Male_Applicants](https://github.com/Suga1412/CAP-PROJECT/assets/104521056/7165b947-7c32-4c1d-bd03-f8508883b673)

Requirement No. 6.a - Monthly number of trasnactions of year 2018
![Req_5 3_Months_VS_Transaction_Count](https://github.com/Suga1412/CAP-PROJECT/assets/104521056/117c5c73-7102-45d0-8c7a-75ec10ba7dfe)

Requirement No. 6.b - Monthly trasnactions value of year 2018
![Req_5 3_Months_VS_Transaction_Value](https://github.com/Suga1412/CAP-PROJECT/assets/104521056/fdb70c13-939c-47e7-8e55-a1fd2da0b321)

Requirement No. 7 - Top 10 branches processed Healthcare transaction
![Req_5 4_Top_10_Branches_Processed_Healthcare_Transaction](https://github.com/Suga1412/CAP-PROJECT/assets/104521056/01b69fe3-89cc-478e-93f8-8a64c23686bf)


