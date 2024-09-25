from pydantic import BaseModel
# Entity check when creating an order
class Order(BaseModel):
    id: str 
    name: str
    address: dict
    price: float
    currency: str 
