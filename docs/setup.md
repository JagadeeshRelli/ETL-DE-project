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
2. set the password
3. open terminal and run the below command to create database
`createdb -h localhost -U postgres walletdb`
4. To create tables and related things , pls run :
   `psql -h localhost -U postgres -d walletdb -f /<repo name>/src/database/sql_scripts.sql`

## Docker Setup

1. Install Docker Desktop.
2. Edit .env file in the docker folder, type your postgresql password in place of <your_password> and save
3. Now Open Command Prompt in the same folder to run:

   `docker-compose build`

This will build the containers.

4. Start the containers with:

`docker-compose up`

5. You can view ,trigger the dags by visiting to `localhost:8080` in any browser
6. Stop the containers with:

`docker-compose down`

You can visually monitor everything in Docker Desktop.


### Running the Project

`python etl_script.py`

### Testing

`python test.py`


