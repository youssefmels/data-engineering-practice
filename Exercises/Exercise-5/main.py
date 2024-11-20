import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cursor = conn.cursor()
    COMMANDS = ("""DROP TABLE IF EXISTS Accounts CASCADE;""",
                """DROP TABLE IF EXISTS Products CASCADE;""",
                """DROP TABLE IF EXISTS Transactions CASCADE;""","""CREATE TABLE Accounts(customer_id int PRIMARY KEY, firstname char(20), lastname char(20), 
                address_1 varchar, address_2 varchar, city char(20), state char(20), 
                zipcode int, join_date date)""",
                """CREATE TABLE Products(product_id int PRIMARY KEY, product_code int, product_description varchar)""",
                """CREATE TABLE Transactions(transaction_id varchar PRIMARY KEY, transaction_date date,
                product_id int references Products (product_id), product_code int, product_description varchar, 
                quantity int, account_id int references Accounts (customer_id))""",

                """CREATE INDEX idx_accounts_join_date ON Accounts (customer_id);"""
                ,"""CREATE INDEX idx_products_product_id ON products (product_id);"""
                ,"""CREATE INDEX idx_transactions_account_id ON transactions (transaction_id);"""
                ,"""CREATE INDEX idx_transactions_product_id ON transactions (product_id);""")
    
    accountsDF = pd.read_csv('/app/data/accounts.csv')
    productsDF = pd.read_csv('/app/data/products.csv')
    transactionsDF = pd.read_csv('/app/data/transactions.csv')
    for command in COMMANDS:
        cursor.execute(command)
    conn.commit()

    data_files = {"Accounts": accountsDF,"Products":productsDF,"Transactions":transactionsDF}

    insert = { "Accounts": "INSERT INTO Accounts (customer_id, firstname, lastname, address_1, address_2, city, state, zipcode, join_date) VALUES %s",
    "Products": "INSERT INTO Products (product_id, product_code, product_description) VALUES %s",
    "Transactions": "INSERT INTO Transactions (transaction_id, transaction_date, product_id, product_code, product_description, quantity, account_id) VALUES %s"}

    for table, df in data_files.items():
        rows = [tuple(x) for x in df.to_numpy()]
        print(f"Inserting data into {table}: {rows[:5]}...") 
        execute_values(cursor, insert[table], rows)

    conn.commit()
    
    cursor.close()
    conn.close()
if __name__ == "__main__":
    main()
