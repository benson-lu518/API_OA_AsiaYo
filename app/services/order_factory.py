from models.order import Order
from fastapi import  HTTPException
from pydantic import ValidationError

# Factory class to create an Order object
class OrderFactory:
    def __init__(self, order_data):
        self.order_data = order_data
    def create_order(self):
        try:
            order = Order(**self.order_data)
        except ValidationError as e:
            raise HTTPException(status_code=412, detail=e.errors())
        return order
