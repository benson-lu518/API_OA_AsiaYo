# AsiaYo API OA

## Description
This project implements a RESTful API using FastAPI to handle orders and Pyspark to process data. 


## Features

- **FastAPI**: Built with FastAPI to ensure high performance and easy development.
- **Data Processing**: Integrates PySpark for efficient data processing and analytics.
- **Docker Support**: Dockerized application for easy deployment and scaling.

## Workflow
![Flow Diagram](workflow.png)

1. A request is sent to the `POST /api/orders` endpoint.
2. The request is handled by the `order_factory`, which checks if the form is valid:
   - If valid, it proceeds to create the order.
   - If invalid, it returns an `Error Code=412`.
3. The `order parser` validates the order:
   - Validates the name, price, and currency.
   - Transforms the currency if needed.
   - If any validation fails, an `Error Code=400` is returned.

## Installation

To set up the project locally, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/benson-lu518/API_OA_AsiaYo.git
   cd API_OA_AsiaYo/app 

2. Build Docker Image
   ```bash
   docker build . -t asiayo-fastapi:latest

3. Run Docker container
   ```bash
   docker run -p 8000:8000 asiayo-fastapi  
