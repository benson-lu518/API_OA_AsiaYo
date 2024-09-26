# AsiaYo API OA

## Description
This project implements a RESTful API using FastAPI to handle orders. 


## Features

- **FastAPI**: Built with FastAPI to ensure high performance and easy development.
- **Data Processing**: Integrates PySpark for efficient data processing and analytics.
- **Docker Support**: Dockerized application for easy deployment and scaling.

## Workflow
![Flow Diagram](workflow.png)


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
