from typing import Union

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi import Request
from pydantic import BaseModel
from snowflake.snowpark import Session
from snowpark import snowpark
from connector import connector
app = FastAPI()

app.include_router(connector, prefix="/connector")
app.include_router(snowpark, prefix="/snowpark")

@app.get("/")
async def default():
    return {"result" : "Nothing to see here"}

@app.get("/test")
async def default():
    return {"result" : "Test page."}

# Custom HTTP request exception handler
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})