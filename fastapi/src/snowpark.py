import datetime
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from snowflake.snowpark import Session
import snowflake.snowpark.functions as f
from config import creds

def connect() -> Session:
    return Session.builder.configs(creds).create()

session = connect()

snowpark = APIRouter()

dateformat = '%Y-%m-%d'

@snowpark.get('/customers/top10')
async def customers_top10(start_range: str = Query(default='1995-01-01'), end_range: str = Query(default='1995-03-31')):
    # Validate arguments
    sdt_str = start_range
    edt_str = end_range
    try:
        sdt = datetime.datetime.strptime(sdt_str, dateformat)
        edt = datetime.datetime.strptime(edt_str, dateformat)
    except:
        raise HTTPException(status_code=400, detail="Invalid start and/or end dates.")
    try:
        df = session.table('snowflake_sample_data.tpch_sf10.orders') \
                .filter((f.col('O_ORDERDATE') >= sdt) & (f.col('O_ORDERDATE') <= edt)) \
                .group_by(f.col('O_CUSTKEY')) \
                .agg(f.sum(f.col('O_TOTALPRICE')).alias('SUM_TOTALPRICE')) \
                .sort(f.col('SUM_TOTALPRICE').desc()) \
                .limit(10)
        data = [x.as_dict() for x in df.to_local_iterator()]
        serialized_data = [
        {key: str(value) if isinstance(value, Decimal) else value for key, value in item.items()}
        for item in data
        ]
        return JSONResponse(content=serialized_data)
    except:
        raise HTTPException(status_code=500, detail="Error reading from Snowflake. Check the logs for details.")
    
@snowpark.get('/clerk/<clerkid>/yearly_sales/<year>')
def clerk_montly_sales(clerkid, year):
    # Validate arguments
    try: 
        year_int = int(year)
    except:
        raise HTTPException(status_code=400, detail="Invalid year")
    if not clerkid.isdigit():
        raise HTTPException(status_code=400, detail="Clerk ID can only contain numbers.")
    clerkid_str = f"Clerk#{clerkid}"
    try:
        df = session.table('snowflake_sample_data.tpch_sf10.orders') \
                .filter(f.year(f.col('O_ORDERDATE')) == year_int) \
                .filter(f.col('O_CLERK') == clerkid_str) \
                .with_column('MONTH', f.month(f.col('O_ORDERDATE'))) \
                .groupBy(f.col('O_CLERK'), f.col('MONTH')) \
                .agg(f.sum(f.col('O_TOTALPRICE')).alias('SUM_TOTALPRICE')) \
                .sort(f.col('O_CLERK'), f.col('MONTH'))
        data = [x.as_dict() for x in df.to_local_iterator()]
        serialized_data = [
        {key: str(value) if isinstance(value, Decimal) else value for key, value in item.items()}
        for item in data
        ]
        return JSONResponse(content=serialized_data)
    except:
        raise HTTPException(status_code=500, detail="Error reading from Snowflake. Check the logs for details.")