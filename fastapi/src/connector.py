import datetime
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
import snowflake.connector
from snowflake.connector import DictCursor
from config import creds

def connect() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(**creds)

conn = connect()

connector = APIRouter()

dateformat = '%Y-%m-%d'

class CustomJSONResponse(JSONResponse):
    def render(self, content):
        # Serialize Decimal values
        serialized_content = [
            {key: str(value) if isinstance(value, Decimal) else value for key, value in item.items()}
            for item in content
        ]
        return super().render(serialized_content)

@connector.get('/customers/top10')
async def customers_top10(start_range: str = Query(default='1995-01-01'), end_range: str = Query(default='1995-03-31')):
    # Validate arguments
    sdt_str = start_range
    edt_str = end_range
    try:
        sdt = datetime.datetime.strptime(sdt_str, dateformat)
        edt = datetime.datetime.strptime(edt_str, dateformat)
    except:
        raise HTTPException(status_code=400, detail="Invalid start and/or end dates.")
    sql_string = '''
        SELECT
            o_custkey
          , SUM(o_totalprice) AS sum_totalprice
        FROM snowflake_sample_data.tpch_sf10.orders
        WHERE o_orderdate >= '{sdt}'
          AND o_orderdate <= '{edt}'
        GROUP BY o_custke
        ORDER BY sum_totalprice DESC
        LIMIT 10
    '''
    sql = sql_string.format(sdt=sdt, edt=edt)
    try:
        res = conn.cursor(DictCursor).execute(sql)
        result = res.fetchall()
        res.close()
        return CustomJSONResponse(content=result)
    except:
        raise HTTPException(status_code=500, detail="Error reading from Snowflake. Check the logs for details.")
    
@connector.get('/clerk/<clerkid>/yearly_sales/<year>')
def clerk_montly_sales(clerkid, year):
    # Validate arguments
    try: 
        year_int = int(year)
    except:
        raise HTTPException(status_code=400, detail="Invalid year")
    if not clerkid.isdigit():
        raise HTTPException(status_code=400, detail="Clerk ID can only contain numbers.")
    clerkid_str = f"Clerk#{clerkid}"
    sql_string = '''
        SELECT
            o_clerk
          ,  Month(o_orderdate) AS month
          , SUM(o_totalprice) AS sum_totalprice
        FROM snowflake_sample_data.tpch_sf10.orders
        WHERE Year(o_orderdate) = {year}
          AND o_clerk = '{clerkid}'
        GROUP BY o_clerk, month
        ORDER BY o_clerk, month
    '''
    sql = sql_string.format(year=year_int, clerkid=clerkid_str)
    try:
        res = conn.cursor(DictCursor).execute(sql)
        result = res.fetchall()
        res.close()
        return CustomJSONResponse(content=result)
    except:
        raise HTTPException(status_code=500, detail="Error reading from Snowflake. Check the logs for details.")