from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, lower, when, lpad, initcap, substring
from pyspark.sql.types import IntegerType, StringType, TimestampType, DoubleType
import mysql.connector
from mysql.connector import Error
import matplotlib.pyplot as plt
import numpy as np
import requests
from pyspark.sql import DataFrame
import pandas as pd
import secretss


spark = SparkSession.builder.appName("CreditCardSystem").getOrCreate()


branch_json_path = "C:/Users/Learner_9ZH3Z179/Desktop/CAP PROJECT/cdw_sapp_branch.json"
creditcard_json_path = "C:/Users/Learner_9ZH3Z179/Desktop/CAP PROJECT/cdw_sapp_credit.json"
customer_json_path = "C:/Users/Learner_9ZH3Z179/Desktop/CAP PROJECT/cdw_sapp_custmer.json"



creditcard_df = spark.read.json(creditcard_json_path)
customer_df = spark.read.json(customer_json_path)
branch_df = spark.read.json(branch_json_path)

#print(creditcard_df.printSchema())
total_rows = creditcard_df.count()
#print(f"Total rows in customer_df: {total_rows}")

total_rows = customer_df.count()
#print(f"Total rows in customer_df: {total_rows}")

total_rows = branch_df.count()
#print(f"Total rows in customer_df: {total_rows}")

#print(customer_df.show(2))
#creditcard_df.show(2)
#branch_df.show(50)

creditcard1_df = creditcard_df.withColumn("YEAR", col("YEAR").cast("string"))
creditcard2_df = creditcard1_df.withColumn("MONTH", lpad(col("MONTH").cast("string"), 2, "0"))
creditcard3_df = creditcard2_df.withColumn("DAY", lpad(col("DAY").cast("string"), 2, "0"))
#print(creditcard3_df.show(12))


updated_credit_card_df = creditcard3_df.withColumn("TIMEID", concat(col("YEAR"), col("MONTH"), col("DAY")))
#print(updated_credit_card_df.show(12))

renamed_credit_card_df = updated_credit_card_df.withColumnRenamed("CREDIT_CARD_NO", "CUST_CC_NO")
#print(renamed_credit_card_df.printSchema())

changeDataType_credit_card_df = (renamed_credit_card_df
                                 .withColumn("CUST_CC_NO", col("CUST_CC_NO").cast(StringType()))
                                .withColumn("TIMEID", col("TIMEID").cast(StringType()))
                                .withColumn("CUST_SSN", col("CUST_SSN").cast(IntegerType()))
                                .withColumn("BRANCH_CODE", col("BRANCH_CODE").cast(IntegerType()))
                                .withColumn("TRANSACTION_TYPE", col("TRANSACTION_TYPE").cast(StringType()))
                                .withColumn("TRANSACTION_VALUE", col("TRANSACTION_VALUE").cast(DoubleType()))
                                .withColumn("TRANSACTION_ID", col("TRANSACTION_ID").cast(IntegerType()))
                                )



new_creditcard_df = changeDataType_credit_card_df.select("CUST_CC_NO", "TIMEID", "CUST_SSN", "BRANCH_CODE", "TRANSACTION_TYPE","TRANSACTION_VALUE", "TRANSACTION_ID")
#print(new_creditcard_df.show(2))
#print(new_creditcard_df.printSchema())
#print(new_creditcard_df.printSchema())

#print(customer_df.printSchema())
#1. Convert the Name to Title Case
#2. Convert the middle name in lower case
#3. Convert the Last Name in Title Case
updated_customer1_df = customer_df.withColumn("FIRST_NAME", initcap(col("FIRST_NAME")))
updated_customer2_df = updated_customer1_df.withColumn("MIDDLE_NAME", lower(col("FIRST_NAME")))
updated_customer3_df = updated_customer2_df.withColumn("LAST_NAME", initcap(col("LAST_NAME")))

#4. Concatenate Apartment no and Street name of customer's Residence with comma as a seperator (Street, Apartment)
updated_customer4_df = updated_customer3_df.withColumn("FULL_STREET_ADDRESS", concat(col("STREET_NAME"), lit(","), col("APT_NO")))
#print(updated_customer4_df.show(2))

#5. Change the format of phone number to (XXX)XXX-XXXX
#updated_customer5_df = updated_customer4_df.withColumn("CUST_PHONE", concat(lit("("), lit("000"), lit(")"),substring(col("CUST_PHONE"), 1, 3), lit("-"), substring(col("CUST_PHONE"), 4, 4)))
updated_customer5_df = updated_customer4_df.withColumn("CUST_PHONE", concat(lit("("),substring(col("CUST_PHONE"), 1, 3), lit(")"), lit("000"), lit("-"), substring(col("CUST_PHONE"), 4, 4)))

updated_customer6_df = (updated_customer5_df
                        .withColumn("SSN", col("SSN").cast(IntegerType()))
                        .withColumn("FIRST_NAME", col("FIRST_NAME").cast(StringType()))
                        .withColumn("MIDDLE_NAME", col("MIDDLE_NAME").cast(StringType()))
                        .withColumn("LAST_NAME", col("LAST_NAME").cast(StringType()))
                        .withColumn("CREDIT_CARD_NO", col("CREDIT_CARD_NO").cast(StringType()))
                        .withColumn("FULL_STREET_ADDRESS", col("FULL_STREET_ADDRESS").cast(StringType()))
                        .withColumn("CUST_CITY", col("CUST_CITY").cast(StringType()))
                        .withColumn("CUST_STATE", col("CUST_STATE").cast(StringType()))
                        .withColumn("CUST_COUNTRY", col("CUST_COUNTRY").cast(StringType()))
                        .withColumn("CUST_ZIP", col("CUST_ZIP").cast(IntegerType()))
                        .withColumn("CUST_PHONE", col("CUST_PHONE").cast(StringType()))
                        .withColumn("CUST_EMAIL", col("CUST_EMAIL").cast(StringType()))
                        .withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType()))
                        )


#print(updated_customer6_df.printSchema())

new_customer_df = updated_customer6_df.select("SSN", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "CREDIT_CARD_NO","FULL_STREET_ADDRESS", "CUST_CITY", "CUST_STATE", "CUST_COUNTRY", "CUST_ZIP","CUST_PHONE", "CUST_EMAIL", "LAST_UPDATED")
#print(new_customer_df.show(2))
#print(new_customer_df.printSchema())

#print(new_customer_df.printSchema())

#print(branch_df.show(50))
#changes required for BRANCH FILE
#1. If the source value is null load default (99999) value else Direct move
#2. Change the format of phone number to (XXX)XXX-XXXX

updated_branch1_df = branch_df.withColumn("BRANCH_ZIP", when(col("BRANCH_ZIP").isNull(), lit(99999)).otherwise(col("BRANCH_ZIP")))
updated_branch2_df = updated_branch1_df.withColumn("BRANCH_PHONE", concat(lit("("), lit("000"), lit(")"),substring(col("BRANCH_PHONE"), 1, 3), lit("-"), substring(col("BRANCH_PHONE"), 4, 4)))


#print(updated_branch2_df.printSchema())


changeDataType_updated_branch2_df = (updated_branch2_df
                                    .withColumn("BRANCH_CODE", col("BRANCH_CODE").cast(IntegerType()))
                                    .withColumn("BRANCH_NAME", col("BRANCH_NAME").cast(StringType()))
                                    .withColumn("BRANCH_STREET", col("BRANCH_STREET").cast(StringType()))
                                    .withColumn("BRANCH_CITY", col("BRANCH_CITY").cast(StringType()))
                                    .withColumn("BRANCH_STATE", col("BRANCH_STATE").cast(StringType()))
                                    .withColumn("BRANCH_ZIP", col("BRANCH_ZIP").cast(IntegerType()))
                                    .withColumn("BRANCH_PHONE", col("BRANCH_PHONE").cast(StringType()))
                                    .withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType())) 
                                    )



new_branch_df = changeDataType_updated_branch2_df.select("BRANCH_CODE", "BRANCH_NAME", "BRANCH_STREET", "BRANCH_CITY", "BRANCH_STATE","BRANCH_ZIP", "BRANCH_PHONE", "LAST_UPDATED")
#new_branch_df.show(2)

print(new_branch_df.printSchema())


#Function Requirement 1.2 Data loading into Database
#CREATE DATABSE creditcard_capstone

'''new_branch_df.write.format("jdbc") \
  .mode("overwrite") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
  .option("user", secretss.mysql_username) \
  .option("password", secretss.mysql_password) \
  .save()'''

'''new_customer_df.write.format("jdbc") \
  .mode("overwrite") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_CUSTOMER") \
  .option("user", secretss.mysql_username) \
  .option("password", secretss.mysql_password) \
  .save()'''

'''new_creditcard_df.write.format("jdbc") \
  .mode("overwrite") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_CREDIT_CARD") \
  .option("user", secretss.mysql_username) \
  .option("password", secretss.mysql_password) \
  .save()'''


#2. Functional Requirements - Application Front-End

#Functional Requirements 2.1.1 Used to display the transactions made by customers living in a given zip code for a given month and year. 
# Order by day in descending order.

# Function to connect to the MySQL database


def get_connection():
    try:
        connection =  mysql.connector.connect(
        host="localhost",
        user=secretss.mysql_username,
        password=secretss.mysql_password,
        database="creditcard_capstone" 
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None

def fetch_transactions_by_zip_code_month_and_year():
    connection = get_connection()
    cursor = connection.cursor()
    
    while True:
        cust_zip = input("Please enter the ZIP Code: ")
        # Ensure the zip code is valid
        if len(cust_zip) == 5 and cust_zip.isnumeric():
            break # Exit loop when user entered valid zip code
        elif cust_zip.lower() == 'exit':
            print("Returning to the main menu.")
            return # Exits the function and returns to the main function
        else:
            print("Invalid input. Please enter a valid 5 digit zip code or type 'exit' to go back to the main menu.")
            
    while True:    
        month = input("Please enter the month (MM format): ")
        if len(month) == 1:
            month = '0' + month
        if month.isnumeric() and (int(month) >= 1 and int(month) <= 12):
            break
        elif month.lower() == 'exit':
            print("Returning to the main menu.")
            return 
        else:
            print("Invalid input.Please enter a valid month between 01 and 12 or type 'exit' to go back to the main menu.")

    while True:   
        year = input("Please enter the year (YYYY format): ")
        if len(year) == 4 and year.isnumeric():
            break
        elif year.lower() == 'exit':
            print("Returning to the main menu.")
            return 
        else:
            print("Invalid input.Please enter a valid 4-digit year or type 'exit' to go back to the main menu.")

    query = f"""
        SELECT C.FIRST_NAME, C.MIDDLE_NAME, C.LAST_NAME, CC.TIMEID, CC.TRANSACTION_TYPE, CC.TRANSACTION_VALUE, c.CUST_ZIP
        FROM CDW_SAPP_CUSTOMER C 
        INNER JOIN CDW_SAPP_CREDIT_CARD CC ON C.SSN = CC.CUST_SSN
        WHERE c.CUST_ZIP = '{cust_zip}' 
        AND SUBSTRING(CC.TIMEID, 1, 4) = '{year}' 
        AND SUBSTRING(CC.TIMEID, 5, 2) = '{month}'
        ORDER BY TIMEID DESC;
    """

    cursor.execute(query)
    transactions = cursor.fetchall()

    df = pd.DataFrame(transactions, columns=["FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "TIMEID", "TRANSACTION_TYPE","TRANSACTION_VALUE", "CUST_ZIP"])
    print(df)

    cursor.close()
    connection.close()

#Functional Requirements 2.1.2 Used to display the number and total values of transactions for a given type.


def fetch_transactions_by_type():
    connection = get_connection()
    cursor = connection.cursor()
    while True:
        transaction_type = input("Please enter the Transaction_type from below list: ['Bills', 'Healthcare', 'Test', 'Education', 'Entertainment' 'Gas', 'Grosory' :   ")
        user_input = transaction_type.upper()
        if user_input in ['EDUCATION', 'BILLS', 'HEALTHCARE', 'ENTERTAINMENT', 'GROCERY', 'GAS', 'TEST']:
            break
        elif transaction_type.lower() == 'exit':
            print("Returning to the main menu.")
            return
        else:
            print("Invalid input.Enter valid transaction type or type 'exit' to go back to the main menu.")

    query = f"""
    SELECT TRANSACTION_TYPE, COUNT(TRANSACTION_ID) AS NUMBER_OF_TRANSACTION, SUM(TRANSACTION_VALUE) AS TOTAL_VALUE_OF_TRANSACTION
    FROM CDW_SAPP_CREDIT_CARD
    WHERE TRANSACTION_TYPE = '{user_input}';
    """

    cursor.execute(query)
    data = cursor.fetchall()

    df = pd.DataFrame(data, columns=["TRANSACTION_TYPE", "NUMBER_OF_TRANSACTION", "TOTAL_VALUE_OF_TRANSACTION"])
    print(df)

    cursor.close()
    connection.close()


#Functional Requirements 2.1.3 Used to display the total number and total values of transactions for branches in a given state.

def fetch_transactions_by_state():
    connection = get_connection()
    cursor = connection.cursor()

    while True:
        state = input("Please enter the state abbreviation (e.g.:\n'IL' for Illinois \n'NY' for New York): ").upper()
        if state in ['MN', 'IL', 'NY', 'FL', 'PA', 'NJ', 'CT', 'OH', 'MI', 'KY', 'MD', 'WA', 'CA', 'TX', 'NC', 'VA', 'GA', 'MT', 'AR', 'MS', 'WI', 'IN', 'SC', 'MA', 'IA', 'AL']:
            break
        elif state.lower() == 'exit':
            print("Returning to the main menu.")
            return
        else:
            print("Invalid input.Enter valid state name or type 'exit' to go back to the main menu.")
    query = f"""  
    SELECT B.BRANCH_STATE, COUNT(CC.TRANSACTION_ID) AS TOTAL_TRANSACTIONS, SUM(CC.TRANSACTION_VALUE) AS TOTAL_VALUE
    FROM CDW_SAPP_BRANCH B
    INNER JOIN CDW_SAPP_CREDIT_CARD CC ON B.BRANCH_CODE = CC.BRANCH_CODE
    WHERE B.BRANCH_STATE = '{state}'
    GROUP BY B.BRANCH_STATE;
    """
    
    cursor.execute(query)
    data = cursor.fetchall()

    df = pd.DataFrame(data, columns=["BRANCH_STATE", "TOTAL_TRANSACTIONS", "TOTAL_VALUE"])
    print(df)

    cursor.close()
    connection.close()

def main1():
    while True:
        print("\n \n ----------------Welcome to Functional Requirements 2.1 - Transaction Details Module----------------\n ")
        print("""
        Please select a module to dive deeper into its functional requirements:
              
        1. Functional Requirements 2.1.1 - Display transactions made by customer through ZIP Code, Year and Month.
        2. Functional Requirements 2.1.2 - Display transactions number and value through transaction type.
        3. Functional Requirements 2.1.3 - Display transactions number and value of branches through the state.
        4. Exit
        """)
        
        choice = input("Enter your choice: ")
        
        if choice == '1':
            fetch_transactions_by_zip_code_month_and_year()
        elif choice == '2':
            fetch_transactions_by_type()
        elif choice == '3':
            fetch_transactions_by_state()
        elif choice == '4':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

#if __name__ == '__main__':
#   main1()


#2.2 Customer Details Module

# Connect to the database
def get_connection():
    try:
        connection =  mysql.connector.connect(
        host="localhost",
        user=secretss.mysql_username,
        password=secretss.mysql_password,
        database="creditcard_capstone" 
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None

#Functional Requirements 2.2.1 Used to check the existing account details of a customer.
def check_account_details():
    connection = get_connection()
    cursor = connection.cursor()

    # Get the SSN from the user
    while True:
        customer_ssn = input("Please enter the customer's SSN: ")
        # Ensure the SSN is valid
        if len(customer_ssn) == 9 and customer_ssn.isnumeric():
            break # Exit loop when user entered valid SSN
        elif customer_ssn.lower() == 'exit':
            print("Returning to the main menu.")
            return
        else:
            print("Invalid input. Please enter a valid 9 digit SSN or type 'exit' to go back to the main menu.")

    query = f"""
        SELECT *
        FROM CDW_SAPP_CUSTOMER
        WHERE SSN = '{customer_ssn}'; """

    cursor.execute(query)
    account_details = cursor.fetchall()

    #print(account_details)
    column_names = [column[0] for column in cursor.description]
    df = pd.DataFrame(account_details, columns=column_names)
    

    #print(account_details)
    #df = pd.DataFrame(account_details, columns=["SSN", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "CREDIT_CARD_NO", "FULL_STREET_ADDRESS", "CUST_CITY", "CUST_STATE", "CUST_COUNTRY", "CUST_ZIP", "CUST_PHONE", "CUST_EMAIL", "LAST_UPDATED"])
    print(df)

    cursor.close()
    connection.close()


#Functional Requirements 2.2.2 Used to modify the existing account details of a customer.
# Connect to the database
def modify_account_details():
    connection = get_connection()
    cursor = connection.cursor()

    # Get the SSN from the user
    #suppose, customer wants to update his CUST_CITY then

    customer_ssn = input("ENter a valid SSN: ")
    
    query = f"""
    SELECT CUST_CITY 
    FROM CDW_SAPP_CUSTOMER 
    WHERE SSN = '{customer_ssn}';"""

    cursor.execute(query)
    current_city =cursor.fetchone()
    if current_city:
        #print(current_city[0])
        print(f"Your city in our records is: {current_city[0]}")

    new_city = input("Enter your new city name: ")

    update_query = f"""
    UPDATE CDW_SAPP_CUSTOMER
    SET CUST_CITY = '{new_city}'
    WHERE SSN = '{customer_ssn}';"""

    cursor.execute(update_query)
    connection.commit()
    print("City updated successfully!")

    updated_query = f"""
    SELECT SSN, FIRST_NAME, LAST_NAME, CUST_CITY 
    FROM CDW_SAPP_CUSTOMER 
    WHERE SSN = '{customer_ssn}';"""
    cursor.execute(updated_query)
    updated_details =cursor.fetchall()

    column_names = [column[0] for column in cursor.description]
    df = pd.DataFrame(updated_details, columns=column_names)
    print(df)

    cursor.close()
    connection.close()

#Functional Requirements 2.2.3 Used to generate a monthly bill for a credit card number for a given month and year.
# Connect to the database
def monthly_bill():
    connection = get_connection()
    cursor = connection.cursor()
    while True:
        card_number = input("Please enter the credit card number: ")
        # Ensure the credit card number is valid
        if len(card_number) == 16 and card_number.isnumeric():
            break # Exit loop when user entered valid card number
        elif card_number.lower() == 'exit':
            print("Returning to the main menu.")
            return
        else:
            print("Invalid input. Please enter a valid 16-digit credit card number or type 'exit' to go back to the main menu.")
    
    while True:    
        month = input("Please enter the month (MM format): ")
        # Ensure the month is in the correct format
        if len(month) == 1:
            month = '0' + month
        
        if month.isnumeric() and (int(month) >= 1 and int(month) <= 12):
            break
        elif month.lower() == 'exit':
            print("Returning to the main menu.")
            return
        else:
            print("Invalid input. Please enter a valid month between 01 and 12 or type 'exit' to go back to the main menu.")

    while True:   
        year = input("Please enter the year (YYYY format): ")
        if len(year) == 4 and year.isnumeric():
            break # Exit loop when user entered valid year
        elif year.lower() == 'exit':
            print("Returning to the main menu.")
            return
        else:
            print("Invalid input. Please enter a valid 4-digit year or type 'exit' to go back to the main menu.") 

# Formulate the query
    query = f"""
        SELECT CUST_CC_NO, SUM(TRANSACTION_VALUE) AS TOTAL_BILL
        FROM CDW_SAPP_CREDIT_CARD
        WHERE CUST_CC_NO = '{card_number}' 
        AND TIMEID LIKE '{year}{month}%'
        GROUP BY CUST_CC_NO; """

    cursor.execute(query)
    records = cursor.fetchall()
    df = pd.DataFrame(records, columns=["CUST_CC_NO", "TOTAL_BILL"])
    print(df)

    cursor.close()
    connection.close()

#Functional Requirements 2.2.4 Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.
# Get the SSN, start date, and end date from the user

def Transaction_range():
    connection = get_connection()
    cursor = connection.cursor()
    #customer_ssn = input("Please enter the customer's SSN: ")
    while True:
        customer_ssn = input("Please enter the customer's SSN: ")
        # Ensure the SSN is valid
        if len(customer_ssn) == 9 and customer_ssn.isnumeric():
            break # Exit loop when user entered valid SSN
        elif customer_ssn.lower() == 'exit':
            print("Returning to the main menu.")
            return
        else:
            print("Invalid input. Please enter a valid 9 digit SSN or type 'exit' to go back to the main menu.")

    while True:
        start_date = input("Please enter the Starting date(in YYYYMMDD format): ")
        # Ensure the date is valid
        if len(start_date) == 8 and customer_ssn.isnumeric():
            break # Exit loop when user entered valid starting date
        elif start_date.lower() == 'exit':
            print("Returning to the main menu.")
            return
        else:
            print("Invalid input. Please enter a valid 8 digit date format YYYYMMDD  or type 'exit' to go back to the main menu.")

    while True:
        end_date = input("Please enter the ending date(in YYYYMMDD format): ")
        # Ensure the date is valid
        if len(end_date) == 8 and customer_ssn.isnumeric():
            break # Exit loop when user entered valid ending date
        elif end_date.lower() == 'exit':
            print("Returning to the main menu.")
            return
        else:
            print("Invalid input. Please enter a valid 8 digit date format YYYYMMDD  or type 'exit' to go back to the main menu.")

# Formulate the query
    query = f"""
        SELECT *
        FROM CDW_SAPP_CREDIT_CARD
        WHERE CUST_SSN = '{customer_ssn}' 
        AND TIMEID BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY TIMEID DESC; """

    cursor.execute(query)
    tras = cursor.fetchall()


    df = pd.DataFrame(tras, columns=["CUST_CC_NO", "TIMEID", "CUST_SSN", "BRANCH_CODE", "TRANSACTION_TYPE", "TRANSACTION_VALUE", "TRANSACTION_ID"])
    print(df)

    cursor.close()
    connection.close()

def main2():
    while True:
        print("\n \n----------------Welcome to Functional Requirements 2.2 - Customer Details----------------\n")
        print("""
        Please select a module to dive deeper into its functional requirements:
              
        1. Functional Requirements 2.2.1 - Check the existing account details of a the customer.
        2. Functional Requirements 2.2.2 - Modify the existing account details of a customer.
        3. Functional Requirements 2.2.3 - Generate monthly credit card bill through the card number for a given month and year.
        4. Functional Requirements 2.2.4 - Generate transactions made by a customer between two dates range.
        5. Exit
        """)
        
        choice = input("Enter your choice: ")
        
        if choice == '1':
            check_account_details()
        elif choice == '2':
            modify_account_details()
        elif choice == '3':
            monthly_bill()
        elif choice == '4':
            Transaction_range()
        elif choice == '5':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

#if __name__ == '__main__':
#   main2()

#3. Functional Requirements - Data Analysis and Visualization


# Function to connect to the MySQL database
def get_connection():
    try:
        connection =  mysql.connector.connect(
        host="localhost",
        user=secretss.mysql_username,
        password=secretss.mysql_password,
        database="creditcard_capstone" 
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None


#Functional Requirements 3.1 Find and plot which transaction type has the highest transaction count.
def plot_transaction_type_with_highest_count():
    connection = get_connection()
    cursor = connection.cursor()

    query = """
        SELECT TRANSACTION_TYPE, COUNT(*) AS TRANSACTION_COUNT
        FROM CDW_SAPP_CREDIT_CARD
        GROUP BY TRANSACTION_TYPE
        ORDER BY TRANSACTION_COUNT DESC;
        """
    cursor.execute(query)
    data = cursor.fetchall() 
    df = pd.DataFrame(data, columns=["Transaction Type", "Transaction Count"])
    plt.figure(figsize=(8, 5))
    #bars = plt.bar(df["Transaction Type"], df["Transaction Count"])
    plt.bar(df["Transaction Type"], df["Transaction Count"])
    #for bar in bars:
     #   yval = bar.get_height()
     #   plt.text(bar.get_x() + bar.get_width()/2, yval + 1, round(yval, 2), ha='center', va='bottom')
    plt.xlabel("Transaction Type")
    plt.ylabel("Number of Transactions")
    plt.title("Transaction Type with Highest Transaction Count")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.yticks([6000, 6100, 6200, 6300, 6400, 6500, 6600, 6700, 6800, 6900])
    plt.ylim(6000,6900)
    plt.grid(axis='y', linestyle ='--', alpha=0.7)
    plt.show()

    cursor.close()
    connection.close()




#Functional Requirements 3.2 Find and plot which state has a high number of customers.
def plot_state_with_high_customer_count():
    connection = get_connection()
    cursor = connection.cursor()

    query = """
        SELECT CUST_STATE, COUNT(DISTINCT SSN) AS CUSTOMER_COUNT
        FROM CDW_SAPP_CUSTOMER
        GROUP BY CUST_STATE
        ORDER BY CUSTOMER_COUNT DESC;
        """
    cursor.execute(query)
    data = cursor.fetchall()
        
    df = pd.DataFrame(data, columns=["State", "Customer Count"])
    plt.figure(figsize=(12, 7))
    bars = plt.bar(df["State"], df["Customer Count"])
        # Annotate each bar with its height (the customer count)
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 1, round(yval, 2), ha='center', va='bottom')

    plt.xlabel("State")
    plt.ylabel("Number of customers")
    plt.title("Number of customers by State")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.grid(linestyle='--')
    plt.show()
    
    cursor.close()
    connection.close()

#Functional Requirements 3.3 Find and plot the sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
# Hint (use CUST_SSN).

# Function to connect to the MySQL database
def plot_top_10_customers_with_high_tran_count():
    connection = get_connection()
    cursor = connection.cursor()

    query = """
        SELECT CUST_SSN, SUM(TRANSACTION_VALUE) AS TOTAL_TRANSACTION_VALUE
        FROM CDW_SAPP_CREDIT_CARD
        GROUP BY CUST_SSN
        ORDER BY TOTAL_TRANSACTION_VALUE DESC
        LIMIT 10;
        """
    cursor.execute(query)
    data = cursor.fetchall()
        
    df = pd.DataFrame(data, columns=["Customer SSN", "Total Transaction Value"]) #giving descriptive names
    plt.figure(figsize=(8, 5))
    #plt.bar(df["Customer SSN"].astype(str), df["Total Transaction Value"])
    bars = plt.bar(df["Customer SSN"].astype(str), df["Total Transaction Value"])
    #for bar in bars:
    #    yval = bar.get_height()
    #    plt.text(bar.get_x() + bar.get_width()/2, yval + (0.01 * max(df["Total Transaction Value"])), 
     #        round(yval, 2), ha='center', va='bottom')
    plt.xlabel("Customer SSN")
    plt.ylabel("Total Transaction Value")
    plt.title("Top 10 Customers by Total Transaction Amount")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.grid(linestyle='--')
    plt.yticks([4000, 4200, 4400, 4600, 4800, 5000, 5200, 5400, 5600, 5800, 6000])
    plt.ylim(4000,6000)
    plt.show()
    
    cursor.close()
    connection.close()

def main3():
    while True:
        print("\n \n----------------Welcome to Functional Requirements 3.1, 3.2 and 3.3 - Data Analysis and Visualization----------------\n")
        print("""
        Please select a module to dive deeper into its functional requirements:
              
        1. Functional Requirements 3.1 - Check which transaction type has the highest transaction counts.
        2. Functional Requirements 3.2 - Check which state has a high number of customers.
        3. Functional Requirements 3.3 - Check top 10 customers who has the highest transaction amount.
        4. Exit
        """)
        
        choice = input("Enter your choice: ")
        
        if choice == '1':
            plot_transaction_type_with_highest_count()
        elif choice == '2':
            plot_state_with_high_customer_count()
        elif choice == '3':
            plot_top_10_customers_with_high_tran_count()
        elif choice == '4':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")
#if __name__ == '__main__':
#    main3()


#4. Functional Requirements - LOAN Application Dataset
#Functional Requirements 4.1	Create a Python program to GET (consume) data from the above API endpoint for the loan application dataset.
#Functional Requirements 4.2	Find the status code of the above API endpoint.
#Hint: status code could be 200, 400, 404, 401.
#Functional Requirements 4.3	Once Python reads data from the API, utilize PySpark to load data into RDBMS (SQL). 
#The table name should be CDW-SAPP_loan_application in the database.
#Note: Use the “creditcard_capstone” database.

url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
response = requests.get(url)

status_code = response.status_code
print(f"Status Code: {status_code}")

if status_code == 200:
    data = response.json()
else:
    print(f"Failed to fetch data from API. Status Code: {status_code}")
    data = None


Spark = SparkSession.builder.appName("LoanData").getOrCreate()

loan_app_df = spark.createDataFrame(data)
#print(loan_app_df.printSchema())
#print(loan_app_df.show(5))


'''loan_app_df.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application ") \
  .option("user", secretss.mysql_username) \
  .option("password", secretss.mysql_password) \
  .save()'''

#5. Functional Requirements - Data Analysis and Visualization for LOAN Application
#Functional Requirements 5.1 - Find and plot the percentage of applications approved for self-employed applicants.
def get_connection():
    try:
        connection =  mysql.connector.connect(
        host="localhost",
        user=secretss.mysql_username,
        password=secretss.mysql_password,
        database="creditcard_capstone" 
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None
#Functional Requirements 5.1 - Find and plot the percentage of applications approved for self-employed applicants.

#def plot_app_approved_self_emp():
#    connection = get_connection()
#    cursor = connection.cursor()

# Query for total number of self-employed applicants
''' query_total_self_employed = """
        SELECT COUNT(Application_ID)
        FROM CDW_SAPP_loan_application
        WHERE SELF_EMPLOYED = 'YES';
    """
    cursor.execute(query_total_self_employed)
    total_self_employed = cursor.fetchone()[0]

    # Query for number of approved self-employed applicants
    query_approved_self_employed = """
        SELECT COUNT(Application_ID)
        FROM CDW_SAPP_loan_application
        WHERE SELF_EMPLOYED = 'YES' AND Application_Status = 'Y';
    """
    cursor.execute(query_approved_self_employed)
    approved_self_employed = cursor.fetchone()[0]

    not_approved_self_employed = total_self_employed - approved_self_employed
    percentage_approved = (approved_self_employed / total_self_employed) * 100

    # Plot Pie chart data
     
    labels = ['Approved', 'Not Approved']
    sizes = [approved_self_employed, not_approved_self_employed]
    colors = ['green', 'red']
    explode = (0.1, 0)  # explode 1st slice
    plt.figure(figsize=(10, 6))
    plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.2f%%', shadow=True, startangle=140)
    plt.title(f"Percentage of Applications Approved for Self-Employed: {percentage_approved:.2f}%")
    plt.show()

    cursor.close()
    connection.close()'''

def plot_app_approved_self_emp():
    connection = get_connection()
    cursor = connection.cursor()
    query = """
    SELECT * FROM CDW_SAPP_loan_application;
    """
    cursor.execute(query)
    data = cursor.fetchall()
    #column_names = [i[0] for i in cursor.description]  # Extract column headers from cursor description
    
    df = pd.DataFrame(data, columns=["Application_ID", "Application_Status", "Credit_History", "Dependents", "Education", "Gender", "Income", "Married", "Property_Area", "Self_Employed"  ])
    #print(df.head())

    self_employed = df[df['Self_Employed'] == 'Yes']
    approved_self_employed = self_employed[self_employed['Application_Status'] == 'Y']
    not_approved_self_employed = len(self_employed) - len(approved_self_employed)
    percentage_approved = (len(approved_self_employed) / len(self_employed)) * 100

    # Plot Pie chart data
    labels = ['Approved', 'Not Approved']
    sizes = [len(approved_self_employed), not_approved_self_employed]
    colors = ['green', 'red']
    explode = (0.1, 0)  # explode 1st slice
    plt.figure(figsize=(10, 6))
    plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%', shadow= True, startangle=140)
    plt.title(f"Percentage of Applications Approved for Self-Employed: {percentage_approved:.2f}%")
    plt.show()

    cursor.close()
    connection.close()



#Functional Requirements 5.2 Find the percentage of rejection for married male applicants.

def plot_app_rejected_married_male():
    connection = get_connection()
    cursor = connection.cursor()
    query = """
    SELECT * FROM CDW_SAPP_loan_application;
    """
    cursor.execute(query)
    data = cursor.fetchall()
    #column_names = [i[0] for i in cursor.description]  # Extract column headers from cursor description
    
    df = pd.DataFrame(data, columns=["Application_ID", "Application_Status", "Credit_History", "Dependents", "Education", "Gender", "Income", "Married", "Property_Area", "Self_Employed"  ])
    #print(df.head())
    married_males = df[(df['Gender'] == 'Male') & (df['Married'] == 'Yes')]
    rejected_married_males = married_males[married_males['Application_Status'] == 'N']
    percentage_rejected = (len(rejected_married_males) / len(married_males)) * 100

    labels = ['Rejected', 'Accepted']
    sizes = [len(rejected_married_males), len(married_males) - len(rejected_married_males)]
    colors = ['red', 'blue']
    explode = (0.1, 0)  # explode 1st slice for emphasis

    plt.figure(figsize=(10, 6))
    plt.pie(sizes, explode=explode, labels=labels, colors=colors,
            autopct='%1.2f%%', shadow=True, startangle=140)
    plt.title(f"Percentage of Rejection for Married Male Applicants: {percentage_rejected:.2f}%")
    plt.show()

    cursor.close()
    connection.close()

#Functional Requirements 5.3 
#Find and plot the top three months with the largest volume of transaction data.
def plot_top_three_months_largest_vol():
    connection = get_connection()
    cursor = connection.cursor()

    # SQL Query
    query = """
SELECT 
    YEAR(STR_TO_DATE(TIMEID, '%Y%m%d')) AS Transaction_Year,
    MONTH(STR_TO_DATE(TIMEID, '%Y%m%d')) AS Transaction_Month,
    SUM(TRANSACTION_VALUE) AS Total_Transaction_Value
FROM 
    cdw_sapp_credit_card
GROUP BY 
    Transaction_Year,
    Transaction_Month
ORDER BY 
    Total_Transaction_Value DESC
LIMIT 3;
"""
    cursor.execute(query)
    data = cursor.fetchall()
    #column_names = [i[0] for i in cursor.description]  # Extract column headers from cursor description
    
    df = pd.DataFrame(data, columns=["Transaction_Year", "Transaction_Month", "Total_Transaction_Value"])
    #print(df.head())

# Fetch data into a pandas DataFrame
#df = pd.read_sql(query, conn)

# Close the connection
#conn.close()

# Create a new column for the 'Year-Month' format for better visualization
    df['Year_Month'] = df['Transaction_Year'].astype(str) + "-" + df['Transaction_Month'].astype(str).str.zfill(2)

# Plotting
    plt.figure(figsize=(8, 5))
    bars = plt.bar(df['Year_Month'], df['Total_Transaction_Value'], color='skyblue')
    plt.title('Top 3 Months with the Largest Volume of Transaction Data')
    plt.xlabel('Month')
    plt.ylabel('Total Transaction Value')
    plt.xticks(rotation=45)
    plt.yticks([200500, 201000, 201500, 202000, 202500, 203000])
    plt.ylim(200500,203000)

# Displaying the value on top of the bars
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 5, round(yval, 2), ha='center', va='bottom')

    plt.tight_layout()
    plt.show()

    cursor.close()
    connection.close()

#------------------OR--------------------

'''def plot_top_three_months_largest_vol():
    connection = get_connection()
    cursor = connection.cursor()
    query = """
SELECT 
    YEAR(STR_TO_DATE(TIMEID, '%Y%m%d')) AS Transaction_Year,
    MONTH(STR_TO_DATE(TIMEID, '%Y%m%d')) AS Transaction_Month,
    COUNT(TRANSACTION_ID) AS Number_of_Transactions
FROM cdw_sapp_credit_card
GROUP BY Transaction_Year, Transaction_Month
ORDER BY Number_of_Transactions DESC
LIMIT 3;

"""
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["Transaction_Year", "Transaction_Month", "Number_of_Transactions"])

# Fetch data into a pandas DataFrame
#df = pd.read_sql(query, conn)

# Close the connection
#cursor.close()
#conn.close()

# Create a new column for the 'Year-Month' format for better visualization
    df['Year_Month'] = df['Transaction_Year'].astype(str) + "-" + df['Transaction_Month'].astype(str).str.zfill(2)

# Plotting
    plt.figure(figsize=(8, 4))
    bars = plt.bar(df['Year_Month'], df['Number_of_Transactions'], color='lightgreen')
    plt.title('Top 3 Months with the Highest Number of Transactions')
    plt.xlabel('Month')
    plt.ylabel('Number of Transactions')
    plt.xticks(rotation=45)

# Displaying the value on top of the bars
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 5, int(yval), ha='center', va='bottom')

    plt.tight_layout()
    plt.show()


    cursor.close()
    connection.close()'''


#Functional Requirements 5.4
#Find and plot which branch processed the highest total dollar value of healthcare transactions.

def plot_heighest_value_in_healthcare():
    connection = get_connection()
    cursor = connection.cursor()

    query = """
SELECT b.BRANCH_CODE, SUM(c.TRANSACTION_VALUE) AS Total_Healthcare_Transaction_Value
FROM  CDW_SAPP_BRANCH b
JOIN CDW_SAPP_CREDIT_CARD c ON b.BRANCH_CODE = c.BRANCH_CODE
WHERE c.TRANSACTION_TYPE = 'Healthcare'
GROUP BY b.BRANCH_CODE
ORDER BY Total_Healthcare_Transaction_Value DESC
LIMIT 10;
"""
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["BRANCH_CODE", "Total_Healthcare_Transaction_Value"])

# Fetch data into a pandas DataFrame
#df = pd.read_sql(query, conn)

# Close the connection
#conn.close()
# Plotting
    branches = df['BRANCH_CODE'].astype(str).tolist()
    values = df['Total_Healthcare_Transaction_Value'].tolist()


    plt.figure(figsize=(8, 4))
    plt.bar(df['BRANCH_CODE'].astype(str), df['Total_Healthcare_Transaction_Value'])
    plt.title('Branch with the Highest Dollar Value of Healthcare Transactions')
    plt.xlabel('Branch Code')
    plt.ylabel('Total Transaction Value')
    plt.xticks(df['BRANCH_CODE'].astype(str), rotation=45)

# Displaying the value on top of the bar
    for index, value in enumerate(df['Total_Healthcare_Transaction_Value']):
        plt.text(index, value + 5, f"${round(value, 2):,}", ha='center', va='bottom')

    plt.tight_layout()
    plt.show()

def main4():
    while True:
        print("\n \n----------------Welcome to Functional Requirements 5.1, 5.2 and 5.3 - Data Analysis and Visualization----------------\n")
        print("""
        Please select a module to dive deeper into its functional requirements:
              
        1. Functional Requirements 5.1 - Check applications approved for self-employed applicants.
        2. Functional Requirements 5.2 - Check application rejection for married male applicants.
        3. Functional Requirements 5.3 - Top three months with the largest volume of transactions.
        4. Functional Requirements 5.4 - Branch processed the highest total dollar value of healthcare transactions.
        5. Exit
        """)
        
        choice = input("Enter your choice: ")
        
        if choice == '1':
            plot_app_approved_self_emp()
        elif choice == '2':
            plot_app_rejected_married_male()
        elif choice == '3':
            plot_top_three_months_largest_vol()
        elif choice == '4':
            plot_heighest_value_in_healthcare()
        elif choice == '5':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

#if __name__ == '__main__':
#   main4()


def main_main():
    while True:
        print("\n \n ******************************Welcome to Capstone Project Interface*****************************")
        print("""
        Please select a module to dive deeper into its functional requirements:
              
        1. Functional Requirements 2.1 - Transaction Details Module
        2. Functional Requirements 2.2 - Customer Details
        3. Functional Requirements 3.1, 3.2 and 3.3 - Data Analysis and Visualization
        4. Functional Requirements 5.1, 5.2 and 5.3 - Data Analysis and Visualization
        5. Exit
        """)
        choice = input("Enter your choice: ")
        if choice == '1':
            main1()
        elif choice == '2':
            main2()
        elif choice == '3':
            main3()
        elif choice == '4':
            main4()
        elif choice == '5':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

#if __name__ == '__main__':
#    main_main()

