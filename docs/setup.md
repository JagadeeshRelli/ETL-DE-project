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


### Database Setup

1. Install PostgreSQL.
2. set the password and update .env file accordingly
3. open pgadmin and connect to server using password
4. open terminal and run the below command to create database
`createdb -h localhost -U postgres walletdb`
Enter the password when prompted
6. To create tables and related things , pls run :
   `psql -h localhost -U postgres -d walletdb -f src/database/sql_scripts.sql`

## Running the Project

`python src/etl_script.py`


## Testing

`python test/test.py`


## Docker Setup

1. Install Docker Desktop.
2. Edit .env file in the docker folder, type your postgresql password in place of <your_password> and save
3. Now Open Command Prompt in the same folder to run:

   `docker-compose build`

This will build the containers.

4. Start the containers with:

`docker-compose up`
(make sure you have good enough RAM)

5. You can view ,trigger the dags by visiting to `localhost:8080` in any browser
6. Stop the containers with:

`docker-compose down`

You can visually monitor everything in Docker Desktop.





