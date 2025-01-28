import os
import snowflake.connector
import pandas as pd

def fetch_sensitive_info():
    user = os.getenv('SNOWSQL_USER', 'kmallya7')
    password = os.getenv('SNOWSQL_PWD', 'Udumalpet@7')
    account = os.getenv('ACCOUNT', 'hfgwbta-cf08090')
    warehouse = os.getenv('WAREHOUSE', 'COMPUTE_WH')
    database = os.getenv('DATABASE', 'ABCINC')
    schema = os.getenv('SCHEMA', 'ABCINC.PUBLIC')
    role = os.getenv('ROLE', 'ACCOUNTADMIN')

    con = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role
    )

    query = """
    SELECT Name, Email, Phone, Address, Aadhar, PAN, Passport, License
    FROM customerinfo
    """

    df = pd.read_sql(query, con)
    con.close()

    return df

def main():
    df = fetch_sensitive_info()
    print(df)
    df.to_csv('pii_data.csv', index=False)

if __name__ == "__main__":
    main()
