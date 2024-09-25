from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import when, col
from fastapi import HTTPException

# Parse and validate order data
class OrderParser:

    def __init__(self, order):
        self.order = order.dict() 

    def order_to_df(self):
        spark = SparkSession.builder.appName("OrderAPI").getOrCreate()

        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StructType([
                StructField("city", StringType(), True),
                StructField("district", StringType(), True),
                StructField("street", StringType(), True)
            ])),
            StructField("price", FloatType(), True),
            StructField("currency", StringType(), True)
        ])
        rdd = spark.sparkContext.parallelize([self.order])
        df = spark.read.json(rdd, schema)
        return df

    def validate_name(self, df, errors):
        # Name should contain only English characters
        df = df.withColumn("valid_name", when(col("name").rlike("^[A-Za-z ]+$"), "Valid").otherwise("Invalid"))
        
        # Name should be capitalized
        df = df.withColumn("valid_capitalization", when(col("name").rlike(r"^([A-Z][a-z]*)( [A-Z][a-z]*)*$"), "Valid").otherwise("Invalid"))

        
        # Collect errors
        invalid_name = df.filter(col("valid_name") == "Invalid").count() > 0
        invalid_capitalization = df.filter(col("valid_capitalization") == "Invalid").count() > 0
        
        if invalid_name:
            errors.append("Invalid name format (non-English characters).")
        if invalid_capitalization:
            errors.append("Invalid name capitalization (should start with a capital letter).")
        
        return df

    def validate_price(self, df, errors):
        # Price should not exceed 2000
        df = df.withColumn("valid_price", when(col("price") <= 2000, "Valid").otherwise("Invalid"))
        
        # Collect errors
        invalid_price = df.filter(col("valid_price") == "Invalid").count() > 0
        if invalid_price:
            errors.append("Price exceeds the allowed limit of 2000.")
        
        return df

    def validate_currency(self, df, errors):
        # Check currency format
        df = df.withColumn("valid_currency", when(col("currency").isin(["TWD", "USD"]), "Valid").otherwise("Invalid"))
        
        # Convert price to TWD if currency is USD
        df = df.withColumn("price", when(col("currency") == "USD", col("price") * 31).otherwise(col("price")))
        df = df.withColumn("currency", when(col("currency") == "USD", "TWD").otherwise(col("currency")))
        
        # Collect errors
        invalid_currency = df.filter(col("valid_currency") == "Invalid").count() > 0
        if invalid_currency:
            errors.append("Invalid currency format (should be TWD or USD).")
        
        return df

    def validate_order(self, df):
        errors = []
        
        # Run all validation functions
        df = self.validate_name(df, errors)
        df = self.validate_price(df, errors)
        df = self.validate_currency(df, errors)
        
        # If errors were collected, raise an HTTP exception with all error details
        if errors:
            raise HTTPException(status_code=400, detail={"errors": errors})
        
        return df
    
    def parse_order(self):
        df = self.order_to_df()
        validated_df = self.validate_order(df)
        parsed_df = validated_df.select("id", "name", "address", "price", "currency")

        records = [row.asDict() for row in parsed_df.collect()]

        return records
