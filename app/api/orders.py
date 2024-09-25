# app/api/orders.py
from fastapi import APIRouter, HTTPException
from services import order_factory, order_parser




router = APIRouter()

@router.post("/api/orders")
async def receive_order(order_data: dict):
    try:
        # Create an order object
        orderFactory = order_factory.OrderFactory(order_data)
        order = orderFactory.create_order()
        # Parse the order
        orderParser = order_parser.OrderParser(order)
        parsed_order = orderParser.parse_order()
        return parsed_order
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

