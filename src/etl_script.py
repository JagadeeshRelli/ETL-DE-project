# pip install pandas
# pip install SQLAlchemy
# pip install psycopg2-binary

import pandas as pd
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

def extract_data(users_csv_path, transactions_csv_path):
    """
    Extract data from CSV files into pandas DataFrames.
    
    :param users_csv_path: Path to the users CSV file.
    :param transactions_csv_path: Path to the transactions CSV file.
    :return: Tuple of DataFrames (users_df, transactions_df).
    """
    users_dtype_dict = {
        'userid': int,
        'name': str,
        'contact': str,
        'balance': float
    }

    transactions_dtype_dict = {
        'transactionid': int,
        'userid': int,
        'amount': float,
        'type': str,
        'status': str,
        'method': str,
        # 'date': 'datetime64'
    }

    users_df = pd.read_csv(users_csv_path, dtype=users_dtype_dict)
    transactions_df = pd.read_csv(transactions_csv_path, dtype=transactions_dtype_dict, parse_dates=['date'])

    return users_df, transactions_df

def transform_users(users_df):
    """
    Clean and transform the users DataFrame.
    
    :param users_df: DataFrame containing users data.
    :return: Transformed users DataFrame.
    """
    # Remove duplicates based on 'userid'
    users_df = users_df.drop_duplicates(subset=['userid'])
    
    # Handle missing values
    users_df = users_df.fillna({
        'name': 'unknown',         # Fill missing names
        'contact': 'not provided', # Fill missing contacts
        'balance': 0.0             # Fill missing balances
    })
    
    # Validate contact format
    users_df['contact'] = users_df['contact'].apply(lambda x: x if pd.Series(x).str.match(r'^\d{10}$').any() else 'not provided')
    
    # Normalize column names to lowercase
    users_df.columns = [col.lower() for col in users_df.columns]
    
    return users_df

def transform_transactions(transactions_df):
    """
    Clean and transform the transactions DataFrame.
    
    :param transactions_df: DataFrame containing transactions data.
    :return: Transformed transactions DataFrame.
    """
    # Remove duplicates based on 'transactionid'
    transactions_df = transactions_df.drop_duplicates(subset=['transactionid'])
    
    # Validate business rules: filter out rows with negative 'amount' values
    transactions_df = transactions_df[transactions_df['amount'] >= 0]
    
    # Handle missing values
    transactions_df = transactions_df.fillna({
        'amount': 0.0,              # Fill missing amounts
        'type': 'unknown',          # Fill missing types
        'status': 'unknown',        # Fill missing statuses
        'method': 'not specified'   # Fill missing methods
    })
    
    # Convert columns to appropriate data types
    transactions_df['transactionid'] = transactions_df['transactionid'].astype(int)
    transactions_df['amount'] = pd.to_numeric(transactions_df['amount'], errors='coerce')
    transactions_df['date'] = pd.to_datetime(transactions_df['date'], errors='coerce')
    
    # Normalize column names to lowercase
    transactions_df.columns = [col.lower() for col in transactions_df.columns]
    

    
    return transactions_df

def load_data(cleaned_users_df, cleaned_transactions_df, engine):
    """
    Load transformed data into the database and update the user's balance.
    
    :param cleaned_users_df: Transformed users DataFrame.
    :param cleaned_transactions_df: Transformed transactions DataFrame.
    :param engine: SQLAlchemy engine for database connection.
    """
    # SQL queries for upsert and update operations
    upsert_users_query = """
    INSERT INTO users (userid, name, contact, balance)
    VALUES (:userid, :name, :contact, :balance)
    ON CONFLICT (userid) DO UPDATE SET
        name = EXCLUDED.name,
        contact = EXCLUDED.contact,
        balance = EXCLUDED.balance;
    """

    transactions_upsert_stmt = text("""
    INSERT INTO transactions (transactionid, amount, type, date, status, method, userid)
    VALUES (:transactionid, :amount, :type, :date, :status, :method, :userid)
    ON CONFLICT (transactionid) DO UPDATE SET
        amount = EXCLUDED.amount,
        type = EXCLUDED.type,
        date = EXCLUDED.date,
        status = EXCLUDED.status,
        method = EXCLUDED.method,
        userid = EXCLUDED.userid;
    """)

    update_balance_query = """
    WITH transaction_summaries AS (
        SELECT
            userid,
            SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS total_credits,
            SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) AS total_debits
        FROM transactions
        WHERE status = 'completed'
          AND date > :last_update_date
        GROUP BY userid
    )
    UPDATE users u
    SET balance = u.balance +
                  COALESCE(ts.total_credits, 0) -
                  COALESCE(ts.total_debits, 0)
    FROM transaction_summaries ts
    WHERE u.userid = ts.userid;
    """

    fetch_last_update_date_query = "SELECT last_update_date FROM updatemetadata WHERE id = 1"
    update_last_update_date_query = "UPDATE updatemetadata SET last_update_date = :new_update_date WHERE id = :id"

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        with engine.connect() as connection:
            # Load users data
            for user_data in cleaned_users_df.to_dict(orient='records'):
                connection.execute(text(upsert_users_query), user_data)
            print("\nUsers data loaded into database.")

            # Load transactions data
            for transaction_data in cleaned_transactions_df.to_dict(orient='records'):
                connection.execute(transactions_upsert_stmt, transaction_data)
            print("\nTransactions data loaded into database.")

            # Fetch last update date and update balances
            result = connection.execute(text(fetch_last_update_date_query))
            last_update_date = result.scalar()
            connection.execute(text(update_balance_query), {'last_update_date': last_update_date})
            print("\nUsers' balances updated.")

            # Update the last update date in metadata
            new_update_date = datetime.now()
            connection.execute(text(update_last_update_date_query), {'new_update_date': new_update_date, 'id': 1})
            print("\nLast update date updated to:", new_update_date)

            connection.commit()

    except Exception as e:
        print(f"Error during load operation: {e}")
    finally:
        session.close()

def main():
    # Extract data from CSV files
    users_df, transactions_df = extract_data(
        "data/users.csv",
        "data/transactions.csv"
    )


    print("\nData extracted successfully.")
    
    # Transform the data
    cleaned_users_df = transform_users(users_df)
    cleaned_transactions_df = transform_transactions(transactions_df)
    
    print("\nData transformed successfully.")
    
    # Retrieve database credentials from environment variables
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    
    # Create an SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_name}')
    print("\nSQLAlchemy engine created successfully.")
    
    # Load data into the database
    load_data(cleaned_users_df, cleaned_transactions_df, engine)

    #print("\nData loaded into database successfully.")


if __name__ == "__main__":
    main()
