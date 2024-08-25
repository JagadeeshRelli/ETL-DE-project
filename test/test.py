import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from etl_script import extract_data, transform_users, transform_transactions, load_data

class ETLTests(unittest.TestCase):
    
    @patch('pandas.read_csv')
    def test_extract_data(self, mock_read_csv):
        mock_users_df = pd.DataFrame({
            'userid': [1, 2],
            'name': ['Alice', 'Bob'],
            'contact': ['1234567890', '0987654321'],
            'balance': [100.0, 200.0]
        })
        mock_transactions_df = pd.DataFrame({
            'transactionid': [1, 2],
            'userid': [1, 2],
            'amount': [50.0, 75.0],
            'type': ['credit', 'debit'],
            'status': ['completed', 'completed'],
            'method': ['card', 'cash'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-02'])
        })

        mock_read_csv.side_effect = [mock_users_df, mock_transactions_df]

        users_df, transactions_df = extract_data('dummy_users.csv', 'dummy_transactions.csv')
        
        self.assertEqual(users_df.shape, (2, 4))
        self.assertEqual(transactions_df.shape, (2, 7))
        self.assertEqual(users_df['name'].tolist(), ['Alice', 'Bob'])
        self.assertEqual(transactions_df['type'].tolist(), ['credit', 'debit'])
    
    def test_transform_users(self):
        users_df = pd.DataFrame({
            'userid': [1, 2],
            'name': ['Alice', None],
            'contact': ['1234567890', 'invalid'],
            'balance': [100.0, None]
        })
        
        transformed_df = transform_users(users_df)
        
        self.assertEqual(transformed_df['name'].tolist(), ['Alice', 'unknown'])
        self.assertEqual(transformed_df['contact'].tolist(), ['1234567890', 'not provided'])
        self.assertEqual(transformed_df['balance'].tolist(), [100.0, 0.0])
        self.assertEqual(transformed_df['userid'].tolist(), [1, 2])
    
    def test_transform_transactions(self):
        transactions_df = pd.DataFrame({
            'transactionid': [1, 2, 2],
            'userid': [1, 2, 3],
            'amount': [50.0, -25.0, None],
            'type': ['credit', 'debit', None],
            'status': ['completed', 'failed', 'completed'],
            'method': ['card', None, 'cash'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-02', None])
        })
        
        transformed_df = transform_transactions(transactions_df)
        
        self.assertEqual(transformed_df['transactionid'].tolist(), [1])
        self.assertEqual(transformed_df['amount'].tolist(), [50.0])
        self.assertEqual(transformed_df['type'].tolist(), ['credit'])
        self.assertEqual(transformed_df['status'].tolist(), ['completed'])
        self.assertEqual(transformed_df['method'].tolist(), ['card'])
    
    @patch('sqlalchemy.create_engine')
    @patch('sqlalchemy.engine.base.Connection.execute')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_load_data(self, mock_sessionmaker, mock_execute, mock_create_engine):
        # Mock setup
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_session = MagicMock()
        mock_sessionmaker.return_value = mock_session
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        # Prepare mock return values
        mock_connection.execute.return_value.scalar.return_value = datetime(2024, 1, 1)

        cleaned_users_df = pd.DataFrame({
            'userid': [1],
            'name': ['Alice'],
            'contact': ['1234567890'],
            'balance': [100.0]
        })
        cleaned_transactions_df = pd.DataFrame({
            'transactionid': [1],
            'amount': [50.0],
            'type': ['credit'],
            'date': pd.to_datetime(['2024-01-01']),
            'status': ['completed'],
            'method': ['card'],
            'userid': [1]
        })
        
        load_data(cleaned_users_df, cleaned_transactions_df, mock_engine)
        
        # Check if SQL statements are executed
        self.assertTrue(mock_connection.execute.called)
        self.assertEqual(mock_connection.execute.call_count, 5)  # Adjust this based on the number of SQL executions

        # Check if commit was called
        self.assertTrue(mock_connection.commit.called)

if __name__ == '__main__':
    unittest.main()
