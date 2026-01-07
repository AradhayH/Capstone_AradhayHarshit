import streamlit as st
import pandas as pd
import os
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser

st.set_page_config(page_title="Urban Mobility AI Assistant", layout="wide")
os.environ["GOOGLE_API_KEY"] = "api-key"

def get_combined_context():
    file_paths = {
        "Global Monthly KPIs": "outputs/kpi_summary.csv",
        "Peak demand hours": "outputs/sqlop1.csv",
        "Revenue by Pickup Zones": "outputs/sqlop2.csv",
        "Top 10 higest revenue days":"ouputs/sqlop3.csv",
        "Average fare by weekday":"outputs/sqlop4.csv",
        "Monthly growth":"outputs/sqlop5.csv",
        "Revenue by Pickup Zone": "outputs/sqlop6.csv",
        
    }
    
    combined_text = ""
    dataframes = {}

    for name, path in file_paths.items():
        if os.path.exists(path):
            df = pd.read_csv(path)
            dataframes[name] = df
            combined_text += f"\n--- {name} ---\n{df.to_string(index=False)}\n"
            
    return dataframes, combined_text

dataframes, context_string = get_combined_context()

st.title("Intelligent Urban Mobility Platform")
st.markdown(" GenAI Insights & Strategic Dashboard")


with st.sidebar:
    st.header("Intelligence Knowledge Base")
    if dataframes:
        for name, df in dataframes.items():
            st.subheader(name)
            st.dataframe(df.astype(str)) 
    else:
        st.error("No data context found. Run your analytics notebook first.")

st.subheader("Ask the Senior Mobility Consultant")
user_query = st.text_input("Example: 'Analyze the revenue drop in February and suggest driver re-distribution.'")

if user_query:
    with st.spinner("Analyzing cross-dataset patterns..."):
        template = """
        <Role>
        You are a Senior Urban Mobility Strategist. You provide data-driven strategic insights.
        </Role>

        <Context>
        The following metrics are derived from NYC Yellow Taxi trip data:
        {kpi_data}
        </Context>

        <Instructions>
        1. Compare performance between January and February metrics.
        2. Identify specific hours of peak demand from the 'Hourly Demand' data.
        3. Use the 'Busiest Zones' coordinates to suggest staging locations.
        4. Provide an 'Executive Summary' with 3 actionable strategic recommendations.
        </Instructions>

        <Question>
        {user_question}
        </Question>

        <Response>"""

        prompt = PromptTemplate.from_template(template)
        
        try:
            llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash-lite", temperature=0.2)
            chain = prompt | llm | StrOutputParser()
            
            response = chain.invoke({
                "kpi_data": context_string, 
                "user_question": user_query
            })
            
            st.markdown("---")
            st.markdown("AI Strategic Insights")
            st.write(response)
            
        except Exception as e:
            st.error(f"Error connecting to GenAI: {e}")

st.divider()
st.caption("Data Source: NYC Yellow Taxi Big Data Pipeline (PySpark & SQL Layer)")