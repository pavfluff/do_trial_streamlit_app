import streamlit as st 
from snowflake.snowpark import Session
import altair as alt

st.title("Data Observability Dashboard")

# Initialize connection.
@st.cache_resource
def create_session():
    return Session.builder.configs(st.secrets.snowflake).create()

session = create_session() 

sql = """
        select 
            DATE_TRUNC('HOUR', LAST_LOAD_TIME) AS  LAST_LOAD_TIME , 
            SUM(ROW_PARSED) AS COUNT,
            'SOURCE_FILE_COUNT' AS COUNT_TYPE,
            TABLE_NAME,
            COALESCE(SUM(ERROR_COUNT), 0) AS ERROR_COUNT
        from DATA_OBSERVABILITY.DO.COPY_HISTORY
        GROUP BY LAST_LOAD_TIME, TABLE_NAME
        UNION ALL
        select 
            DATE_TRUNC('HOUR', LAST_LOAD_TIME) AS  LAST_LOAD_TIME , 
            SUM(ROW_COUNT) AS SOURCE_FILE_COUNT,
            'INGESTED_COUNT' AS COUNT_TYPE,
            TABLE_NAME,
            COALESCE(SUM(ERROR_COUNT), 0) AS ERROR_COUNT
        from DATA_OBSERVABILITY.DO.COPY_HISTORY
        GROUP BY LAST_LOAD_TIME, TABLE_NAME;

      """

source = session.sql(sql).to_pandas()
st.subheader('Source Table')
st.write('Came from INFORMATION_SCHEMA.LOAD_HISTORY...')
st.dataframe(source)

st.write('For sample purposes, source data is filtered to Event table only.')
event_df = source[source["TABLE_NAME"]=="TRAN_EVENT_LOAD"]
# Write directly to the app
tab1, tab2, tab3 = st.tabs([  "Source"
                            , "Event - Ingested vs. Source"
                            , "Event - Ingested Data Volume"
                           ])

with tab1:
    st.write('Filtered Event table from source...')
    st.dataframe(event_df)
    
with tab2:
    st.write('We can use this chart to check if there \
        are any discrepancies with the ingested vs source files. \
        If the ingested_count is not equal to source_file_count, \
        error occurred during loading the source files.')
    chart = alt.Chart(event_df).mark_bar().encode(
                x= alt.X('LAST_LOAD_TIME:T', title='LAST_LOAD_DATE'),
                y= alt.Y('COUNT:Q', title='COUNT', stack="normalize"),
                color = 'COUNT_TYPE:N'
            )
    st.altair_chart(chart, theme=None, use_container_width=True)
    # st.altair_chart(event_df, x="LAST_LOAD_TIME", y="COUNT", color="COUNT_TYPE", stack=False)
    st.write("Wasn't able to exactly copy the DO Level 1 Dashboard since it's \
            hard to recreate the non-stacked charts for now. But there are other \
            chart options that may be useful for us that are not in Snowsight Dashboard.")

with tab3:
    ingested_df = event_df[event_df["COUNT_TYPE"]=='INGESTED_COUNT']
    bar = alt.Chart(event_df).mark_bar().encode(
                x= alt.X('LAST_LOAD_TIME:T', title='LAST_LOAD_DATE'),
                y= alt.Y('COUNT', title='COUNT')
            )
    rule = alt.Chart(event_df).mark_rule(color='red').encode(
        y='mean(COUNT)'
    )
    line = alt.Chart(event_df).mark_line(color='red').transform_window(
        # The field to average
        rolling_mean='mean(COUNT)',
        # The number of values before and after the current value to include.
        frame=[-9, 0]
    ).encode(
        x='LAST_LOAD_TIME:T',
        y='COUNT'
    )
    mean_chart = (bar + rule).properties(width=600)

    st.subheader('Mean Chart')
    st.altair_chart(mean_chart, theme=None, use_container_width=True)

    rolling_mean_chart = (bar + line).properties(width=600)

    st.subheader('Rolling Mean Chart')
    st.altair_chart(rolling_mean_chart, theme=None, use_container_width=True)
