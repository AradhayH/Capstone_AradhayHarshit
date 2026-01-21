import streamlit as st
import pandas as pd
import os
import plotly.express as px
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Urban Mobility AI Dashboard", layout="wide")


def load_csv_folder(folder_path):
    dataframes = {}
    combined_text = ""
    if os.path.exists(folder_path):
        for file in os.listdir(folder_path):
            if file.endswith(".csv"):
                name = file.replace(".csv", "")
                df = pd.read_csv(os.path.join(folder_path, file))
                dataframes[name] = df
                combined_text += f"\n--- {name} ---\n{df.to_string(index=False)}\n"
    return dataframes, combined_text

kpi_dataframes, kpi_context = load_csv_folder("outputs/kpis_output")
sql_dataframes, sql_context = load_csv_folder("outputs/sqloutputs")

context_string = kpi_context + sql_context

with st.sidebar:
    st.title("Urban Mobility Insights")
    st.info(f"Loaded {len(kpi_dataframes)} KPI files and {len(sql_dataframes)} SQL files.")
    

    st.divider()
    st.subheader("Filter Timeline")
    start_date = st.date_input("Start Date", value=pd.to_datetime("2015-11-01"))
    end_date = st.date_input("End Date", value=pd.to_datetime("2016-04-30"))
    
    
    if kpi_dataframes:
        st.subheader("Raw KPI Data")
        for name in kpi_dataframes.keys():
            with st.expander(name):
                st.dataframe(kpi_dataframes[name])


st.title("Intelligent Urban Mobility Dashboard")

st.subheader("Key Performance Indicators")
if "summary_kpis" in kpi_dataframes:
    kpis = kpi_dataframes["summary_kpis"].iloc[0]
    cols = st.columns(5)
    cols[0].metric("Total Revenue", f"${kpis.get('total_revenue', 0):,.0f}")
    cols[1].metric("Avg Fare", f"${kpis.get('avg_fare', 0):.2f}")
    cols[2].metric("Total Trips", f"{kpis.get('total_trips', 0):,.0f}")
    cols[3].metric("Avg Distance", f"{kpis.get('avg_trip_distance', 0):.2f} mi")
    cols[4].metric("Avg Tip %", f"{kpis.get('avg_tip_percentage', 0):.2f}%")
else:
    st.warning("summary_kpis.csv not found or empty.")



st.subheader("Data Visualizations")
tabs = st.tabs(["Monthly Revenue", "Hourly Demand", "Weekday Performance", "Distance Buckets"])

with tabs[0]:
    if "monthly_revenue" in kpi_dataframes:
        df_raw = kpi_dataframes["monthly_revenue"].copy()
        
        date_column = df_raw.columns[0] 

        df_raw[date_column] = pd.to_datetime(df_raw[date_column])

        mask = (df_raw[date_column] >= pd.to_datetime(start_date)) & \
               (df_raw[date_column] <= pd.to_datetime(end_date))
        df_filtered = df_raw.loc[mask].sort_values(by=date_column)

        fig = px.line(
            df_filtered,
            x=date_column,
            y="revenue",
            markers=True,
            title="Revenue Trend (Filtered)",
            labels={date_column: "Month", "revenue": "Total Revenue ($)"}
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("monthly_revenue.csv not found.")

with tabs[1]:
    if "hourly_demand" in kpi_dataframes:
        df = kpi_dataframes["hourly_demand"]
        fig = px.bar(df, x=df.columns[0], y=df.columns[1], title="Trips by Hour of Day", color="trips")
        st.plotly_chart(fig, width="stretch")

with tabs[2]:
    if "weekday_revenue" in kpi_dataframes:
        df = kpi_dataframes["weekday_revenue"]
        fig = px.pie(df, names="pickup_day", values="revenue", title="Revenue Distribution by Weekday")
        st.plotly_chart(fig, width="stretch")

with tabs[3]:
    if "distance_buckets" in kpi_dataframes:
        df = kpi_dataframes["distance_buckets"]
        fig = px.bar(df, x="distance_bucket", y="trips", title="Trip Count by Distance Bucket")
        st.plotly_chart(fig, width="stretch")

st.divider()
st.subheader("AI Mobility Consultant")
user_query = st.text_input("Ask a question about trends, revenue, or demand:")

if user_query:
    with st.spinner("Analyzing mobility data..."):
        try:
            llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash-lite", temperature=0.2)
            
            template = """
            <Role>Senior Urban Mobility Strategist</Role>
            <Context>{kpi_data}</Context>
            <Question>{user_question}</Question>
            <Instructions>
            Analyze the provided CSV data to answer the user. 
            If asking about 'why' revenue dropped, look at monthly_revenue or weekday_revenue.
            Provide an Executive Summary followed by Actionable Insights.
            </Instructions>
            """
            
            prompt = PromptTemplate.from_template(template)
            chain = prompt | llm | StrOutputParser()
            response = chain.invoke({"kpi_data": context_string, "user_question": user_query})
            
            st.markdown(response)
        except Exception as e:
            st.error(f"GenAI Error: {e}")

st.caption("NYC Yellow Taxi Analytics | Capstone Project")