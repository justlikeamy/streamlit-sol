import streamlit as st
import snowflake.connector 
import pandas as pd
import numpy as np


st.title('State Of The Lake')

@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

conn = init_connection()

@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
       
        return cur.fetchall()

rows = run_query("select venue_id, event_date, EVENT_DESCRIPTION,count(1) as transaction_count, sum(total_price_incldisc) as total_revenue, max(total_tickets_scanned) as attendance, sum(total_price_incldisc)/max(total_tickets_scanned) as per_cap from SCOREBOARD2.VW_LEVY_CONCESSIONS where event_date >= date'2021-01-01' group by venue_id, EVENT_DESCRIPTION, event_date order by venue_id, EVENT_DESCRIPTION, event_date DESC limit 100;")
data = pd.DataFrame(rows)
st.write(data)
data[1] = pd.to_datetime(data[1])
hist_values = np.histogram(data[1].dt.hour, bins=24, range=(0,24))


st.bar_chart(hist_values)
with st.sidebar:
    st.line_chart(hist_values)
    # # Print results.
    for row in rows:
        st.write(f"{row[0]} had an event on :{row[1]} - {row[2]}: with a total of {row[4]} in revenue")


