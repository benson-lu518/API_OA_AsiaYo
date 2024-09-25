import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import when, col
from fastapi import HTTPException
from services.order_parser import OrderParser  
from services.order_factory import OrderFactory  

class TestOrderParser(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a Spark session before running tests
        cls.spark = SparkSession.builder \
            .appName("OrderAPI") \
            .master("local") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session after tests are done
        cls.spark.stop()

    def setUp(self):
        # Sample order data for testing
        self.valid_order = {
            "id": "A0000001",
            "name": "Melody Holiday Inn", 
            "address": {
                "city": "taipei-city",
                "district": "da-an-district", 
                "street": "fuxing-south-road"
            },
            "price": "2000", 
            "currency": "USD"
        }

        self.invalid_order_entity = {
            "id": "A1234567",
            "name": "john doe",  
            "address": {
                "city": "Taipei", 
                "district": "Xinyi", 
                "street": "123 Main St"
            },
            # no price, currency
        }

        self.invalid_order_value = {
            "id": "A1234567",
            "name": "john doe",  # Invalid capitalization
            "address": {
                "city": "Taipei", 
                "district": "Xinyi", 
                "street": "123 Main St"
            },
            "price": 2500.0,  # Invalid price
            "currency": "EUR"  # Invalid currency
        }

    
    # Valide 
    # Check that one row is returned and order object to df
    def test_order_to_df(self):
        order = OrderFactory(self.valid_order).create_order()
        order_parser = OrderParser(order)
        df = order_parser.order_to_df()
        self.assertEqual(df.count(), 1)  

    # Check that all feilds are valid
    def test_parse_order(self):
        order = OrderFactory(self.valid_order).create_order()
        order_parser = OrderParser(order)
        records = order_parser.parse_order()
        self.assertEqual(len(records), 1)  # Check that one record is returned
        self.assertEqual(records[0]['id'], "A0000001")
        self.assertEqual(records[0]['name'], "Melody Holiday Inn") 
        self.assertEqual(records[0]['address'].asDict(), {
            "city": "taipei-city",
            "district": "da-an-district", 
            "street": "fuxing-south-road"
        })
        self.assertEqual(records[0]['price'], 62000)  
        self.assertEqual(records[0]['currency'], "TWD")

    # Invalid
    # Validate the entity
    def test_validate_entity_invalid(self):
        factory = OrderFactory(self.invalid_order_entity)
        with self.assertRaises(HTTPException) as context:
            factory.create_order()
        self.assertEqual(context.exception.status_code, 412)

    # Validate the value and Exception
    def test_validate_name_curr_invalid(self):
        order = OrderFactory(self.invalid_order_value).create_order()
        order_parser = OrderParser(order)
        df = order_parser.order_to_df()
        with self.assertRaises(HTTPException) as context:
            order_parser.validate_order(df)
        self.assertIn("Invalid name capitalization (should start with a capital letter).", context.exception.detail["errors"])
        self.assertIn("Price exceeds the allowed limit of 2000.", context.exception.detail["errors"])
        self.assertIn("Invalid currency format (should be TWD or USD).", context.exception.detail["errors"])
if __name__ == "__main__":
    unittest.main()
