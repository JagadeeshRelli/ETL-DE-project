import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from your_script import extract_data, transform_users, transform_transactions, load_data

class ETLTests(unittest.TestCase):
    
    @patch('pandas.read_csv')
    def test_extract_data(self, mock_read_csv):
        # Mock CSV data
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
        
        # Check DataFrame shapes and contents
        self.assertEqual(users_df.shape, (2, 4))
        self.assertEqual(transactions_df.shape, (2, 7))
        self.assertEqual(users_df['name'].tolist(), ['Alice', 'Bob'])
        self.assertEqual(transactions_df['type'].tolist(), ['credit', 'debit'])
    
    def test_transform_users(self):
        users_df = pd.DataFrame({
            'userid': [1, 2, 2],
            'name': ['Alice', None, 'Bob'],
            'contact': ['1234567890', 'invalid', None],
            'balance': [100.0, None, 200.0]
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
        self.assertEqual(transformed_df['type'].tolist(), ['unknown'])
        self.assertEqual(transformed_df['status'].tolist(), ['completed'])
        self.assertEqual(transformed_df['method'].tolist(), ['not specified'])
    
    @patch('sqlalchemy.create_engine')
    @patch('sqlalchemy.engine.base.Connection.execute')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_load_data(self, mock_sessionmaker, mock_execute, mock_create_engine):
        mock_engine = MagicMock()
        mock_sessionmaker.return_value = MagicMock()
        mock_sessionmaker.return_value.__enter__.return_value = MagicMock()
        mock_create_engine.return_value = mock_engine
        
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
        self.assertTrue(mock_execute.called)
        self.assertEqual(mock_execute.call_count, 4)  # Adjust this based on the number of SQL executions
        
        # Check if commit was called
        self.assertTrue(mock_engine.connect.return_value.__enter__.return_value.commit.called)

if __name__ == '__main__':
    unittest.main()
