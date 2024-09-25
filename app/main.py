# app/main.py
from fastapi import FastAPI
from api.orders import router as orders_router

app = FastAPI()

app.include_router(orders_router)
