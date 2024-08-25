# Setup Guide

## Prerequisites

- **Operating System:** Windows
- **Programming Language:** Python 3.10
- **Database:** PostgreSQL 16
- **Tools:** Docker Desktop

## Installation

### Clone the Repository


git clone <repo_url>

cd <project_name>

### Install Dependencies

1. Make sure you are in the project directory where the `requirements.txt` file is located.

2. Install the required Python packages using pip:


   `pip install -r requirements.txt`


## Database Setup

1. Install PostgreSQL.
2. open terminal and run to create database
createdb -h localhost -U postgres walletdb
3.

`database_scripts.sql`.

## Docker Desktop Setup

1. Install Docker Desktop.
2. Open Command Prompt in the `docker files` folder and run:

   docker-compose build


This will build the containers.
Start the containers with:

docker-compose up


Stop the containers with:

docker-compose down

You can visually monitor everything in Docker Desktop.
Configuration
Environment Variables: Set up the .env file.
Running the Project

python etl_script.py

Testing

python test.py

Troubleshooting
Contributing


