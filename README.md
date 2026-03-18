# 📊 AI-Powered Banking Analytics Platform (Snowflake)

![AI_result1](https://github.com/user-attachments/assets/5530e937-2716-4c18-8086-16306dc4d995)

![AI_result2](https://github.com/user-attachments/assets/e05a8a65-7b8e-4c08-81e8-a1fdc15e4371)

![AI_result4](https://github.com/user-attachments/assets/ebc7613b-7c91-4849-8bcc-f6168f7727f4)

# Overview

This project is an end-to-end data analytics platform built using Snowflake, implementing a modern Medallion Architecture (Bronze, Silver, Gold) and enhanced with AI-powered natural language querying.

It enables users to interact with banking data using plain English and receive both query results and automated business insights.

# Architecture

![architecture](https://github.com/user-attachments/assets/4fc3fc4e-2064-4180-8ebb-f126054b6d53)

# Tools

1. Data Warehouse: Snowflake
2. SQL & Data Modeling
3. Python (Snowpark)
4. UI: Streamlit (inside Snowflake)
5. AI/LLM: Snowflake Cortex

# Key Features

✅ Data Validation (Silver Layer)

1. Credit limit violations detection
2. Late payment identification
3. Invalid date handling
4. Balance mismatch validation
5. Negative balance checks

📊 Analytics Layer (Gold)

1. Customer credit utilization analysis
2. Risk identification (high utilization, late payments)
3. Business-ready datasets for reporting

🤖 AI-Powered Querying

1 Natural Language → SQL using Snowflake Cortex
2. Schema-aware query generation
3. Supports questions like:
4. "Show customers who exceeded their credit limit"
5. "Which customers have late payments?"

💡 AI Insights Generation

Automatically generates business insights from query results
Example:
Customers exceeding credit limits identified
Risk segmentation insights

📈 Dashboard

Built using Snowflake Snowsight
1. KPI tracking:
2. Late payments
3. Credit utilization
4. High-risk customers

![dashboard](https://github.com/user-attachments/assets/fd09d662-dc9c-4111-9e1c-d884d4063f04)


🧠 Streamlit AI Assistant

Interactive UI inside Snowflake
Users can:
1. Ask questions in natural language
2. View results instantly
3. Get AI-generated insights

# Key Learnings

1. Implemented Medallion Architecture in Snowflake
2. Built data validation pipelines using SQL
3. Designed business-focused data models
4. Integrated LLMs directly within Snowflake
5. Developed an AI-powered analytics interface
