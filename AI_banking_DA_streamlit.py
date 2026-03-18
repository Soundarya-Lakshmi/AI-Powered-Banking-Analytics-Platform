import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("AI Banking Data Assistant")

question = st.text_input("Ask a question about the banking data")

if question:

    prompt = f"""
    You are an expert Snowflake SQL generator.

    Convert the question into a Snowflake SQL query.

    
    Rules:
    - Use only the table analytics.credit_card_summary
    - Return ONLY SQL
    - No explanation
    - No markdown
    - No text before or after SQL

    The column credit_limit_status has these values:
    LIMIT_EXCEEDED, WITHIN_LIMIT

    payment_status has these values:
    ON_TIME, LATE_PAYMENT

    risk_score has these values: 0, 1, 2, 3

    So when there are questions about which customers exceeded credit limit
    or which customers paid late or which customers are risky
    then make sure to use the correct values
    of LIMIT_EXCEEDED and LATE_PAYMENT and a risk_score >=2 
    in where clause


    
    Question: {question}
    """

    ai_response = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama3-8b',
            '{prompt}'
        )
    """).collect()[0][0]

    # clean the AI output
    sql_query = ai_response.strip()

    try:
        result = session.sql(sql_query).to_pandas()
        st.dataframe(result)
        st.metric("Rows Returned", len(result))

        insight_prompt = f"""
You are a banking data analyst.

User question:
{question}

Dataset summary:
{result}

Provide 2 short business insights.
Directly provide the insights. Don't give any headings

"""

        insights = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'llama3-8b',
                $$ {insight_prompt} $$
            )
            """).collect()[0][0]

        st.subheader("AI Insights")
        st.write(insights)


    except:
        st.error("Sorry, I couldn't generate a valid query. Try rephrasing your question.")
