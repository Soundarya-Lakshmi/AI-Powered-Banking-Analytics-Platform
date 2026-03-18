# 📊 AI-Powered Banking Analytics Platform (Snowflake)

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

Credit limit violations detection
Late payment identification
Invalid date handling
Balance mismatch validation
Negative balance checks

📊 Analytics Layer (Gold)

Customer credit utilization analysis
Risk identification (high utilization, late payments)
Business-ready datasets for reporting

🤖 AI-Powered Querying

Natural Language → SQL using Snowflake Cortex
Schema-aware query generation
Supports questions like:
"Show customers who exceeded their credit limit"
"Which customers have late payments?"

💡 AI Insights Generation

Automatically generates business insights from query results
Example:
Customers exceeding credit limits identified
Risk segmentation insights

📈 Dashboard

Built using Snowflake Snowsight
KPI tracking:
Late payments
Credit utilization
High-risk customers

🧠 Streamlit AI Assistant

Interactive UI inside Snowflake
Users can:
Ask questions in natural language
View results instantly
Get AI-generated insights

# Key Learnings

Implemented Medallion Architecture in Snowflake
Built data validation pipelines using SQL
Designed business-focused data models
Integrated LLMs directly within Snowflake
Developed an AI-powered analytics interface
