# snowflake-api

## Project
In this project, the same API is made twice to with flask and FastAPI to serve data from snowflake. The data is about business transaction. You can retreive the top 10 customers or the yearly sales by clerk.

## Built with

- [![Snowflake](https://img.shields.io/badge/Snowflake-646CFF?style=for-the-badge&logo=snowflake)](https://www.snowflake.com/)

- [![Flask](https://img.shields.io/badge/Flask-2.3.2-646CFF?style=for-the-badge&logo=Flask)](https://flask.palletsprojects.com/)

- [![FastAPI](https://img.shields.io/badge/FastAPI-0.99.0-646CFF?style=for-the-badge&logo=fastapi&logoColor=Green)](https://fastapi.tiangolo.com/)

Feel free to use these logos and adjust the image URLs as needed for your README file.

## Setup
Execute all the query in snowflake_setup.sql to setup the infrastructure.

Either install the flask env or FastApi env with:

```bash
conda env create -f conda_environment.yml
```

Launch the FastAPI with:
```bash
uvicorn app:app --reload
```

Launch the Flask API with :
```bash
python3 app.py
```
Execute all the query in snowflake_cleanup.sql to clean up the infrastructure.