########################################################################################
# Import libraries
########################################################################################
import pandas as pd
import os
import requests
import json
import schedule
import time
import pytz
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pyspark.sql import SparkSession
from databricks import sql


########################################################################################
# Databricks Configuration
########################################################################################
DATABRICKS_SERVER_HOSTNAME = "rivian-prod-us-west-2.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/b050a7573faba9ab"
DATABRICKS_ACCESS_TOKEN = os.getenv("DATABRICKS_ACCESS_TOKEN")


slack_token = os.getenv("SLACK_TOKEN")
url = os.getenv("URL")

########################################################################################
# Slack setup
########################################################################################
client = WebClient(token=slack_token)


########################################################################################
# Function To Send Message To Slack
########################################################################################
def send_message_to_slack(channel, text):
    try:
        response = client.chat_postMessage(channel=channel, text=text)
        print(f"Message sent to {channel} with timestamp {response['ts']}")
    except SlackApiError as e:
        print(f"Error sending message to Slack: {e.response['error']}")


########################################################################################
# Function to Connect to Databricks
########################################################################################
def create_databricks_connection():
    return sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_ACCESS_TOKEN,
    )


########################################################################################
# Function to Execute Query and Get Results
########################################################################################
def execute_query(query, conn):
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0].upper() for desc in cursor.description]
        return pd.DataFrame(result, columns=columns)


########################################################################################
# Function defining all queries to run every hour
########################################################################################
def job():
    t0 = time.time()
    conn = create_databricks_connection()

    local_tz = pytz.timezone("America/Chicago")  # Change this to your expected timezone
    utc_now = datetime.now(pytz.utc)  # Get current UTC time
    local_now = utc_now.astimezone(local_tz)  # Convert to local timezone
    current_hour = local_now.hour
    current_time = local_now.strftime("%Y-%m-%d %H:00")

    one_hour_before = datetime.now() - timedelta(hours=1)
    recorded_at = one_hour_before.strftime("%Y-%m-%d %H:00")
    eight_hours_before = datetime.now() - timedelta(hours=8)
    recorded_at_summary = eight_hours_before.strftime("%Y-%m-%d %H:00")

    # Define the queries
    ########################################################################################
    # Query General Alarms - Every Hour
    ########################################################################################
    query_general = f"""
    select 
        count(alarm_description) as COUNT,
        SUBSTRING_INDEX(alarm_source_scada_short_name, '-', -1) AS STATION_NAME, 
    alarm_description as PARAMETER_NAME
    from manufacturing.drive_unit.fct_du02_scada_alarms
    where alarm_source_scada_short_name ilike '%ROTR01%'
    and activated_at > '{recorded_at}'
    and alarm_priority_desc in ('critical', 'high')
    group by all
    order by COUNT desc
    """
    ########################################################################################
    # Query 40 - Short Stabs - Every Hour
    ########################################################################################
    query_40 = f"""
    SELECT 
        count(file_name) as COUNT, 
        STATION_NAME, 
        'Short-Stab' as PARAMETER_NAME
    FROM manufacturing.drive_unit.fct_nrm1_du02_kistler_press_files
    WHERE shop_name = 'DU02' 
    AND line_name = 'ROTR01'
    AND station_name = '040'
    AND relative_path like '%NOK%'
    AND last_modified_at > '{recorded_at}'
    group by 2,3
    """

#########################################################################################
# If Statement for Summary Queries at EOS
#########################################################################################
    if (15 <= current_hour < 16) or (5 <= current_hour < 6):
        # Define the queries
        ########################################################################################
        # Query General Alarms - Every Hour
        ########################################################################################
        query_general_summary = f"""
        select 
            count(alarm_description) as COUNT,
            SUBSTRING_INDEX(alarm_source_scada_short_name, '-', -1) AS STATION_NAME, 
        alarm_description as PARAMETER_NAME
        from manufacturing.drive_unit.fct_du02_scada_alarms
        where alarm_source_scada_short_name ilike '%ROTR01%'
        and activated_at > '{recorded_at}'
        and alarm_priority_desc in ('critical', 'high')
        group by all
        order by COUNT desc
        """
        ########################################################################################
        # Query 40 - Short Stabs - Every Hour
        ########################################################################################
        query_40_summary = f"""
        SELECT 
            count(file_name), 
            STATION_NAME, 
            'Short-Stab' as PARAMETER_NAME
        FROM manufacturing.drive_unit.fct_nrm1_du02_kistler_press_files
        WHERE shop_name = 'DU02' 
        AND line_name = 'ROTR01'
        AND station_name = '040'
        AND relative_path like '%NOK%'
        AND last_modified_at > '{recorded_at}'
        group by 2,3
        """
        
        ########################################################################################
        # Execute summary queries and fetch data into DataFrames
        ########################################################################################
        df_general_summary = pd.read_sql(query_general_summary, conn)
        df_40_summary = pd.read_sql(query_40_summary, conn)
        # df_50_summary = pd.read_sql(query_50_summary, conn)
        # df_60_summary = pd.read_sql(query_60_summary, conn)
        # df_65_summary = pd.read_sql(query_65_summary, conn)
        # df_80_summary = pd.read_sql(query_80_summary, conn)
        # df_100_summary = pd.read_sql(query_100_summary, conn)
        # df_110_summary = pd.read_sql(query_110_summary, conn)
        # df_210_summary = pd.read_sql(query_210_summary, conn)

        # df_210_unique_sn_summary = pd.read_sql(query_210_unique_sn_summary, conn)
        # df_40_hairpin_origin_summary = pd.read_sql(query_40_hairpin_origin_summary, conn)
        # df_50_hairpin_origin_summary = pd.read_sql(query_50_hairpin_origin_summary, conn)
        # df_65_hairpin_origin_summary = pd.read_sql(query_65_hairpin_origin_summary, conn)
        
        ########################################################################################
        # Combine DataFrames
        ########################################################################################
        df_combined_summary = pd.concat(
            [
                df_general_summary,
                df_40_summary
            #     df_50_summary,
            #     df_60_summary,
            #     df_65_summary,
            #     df_80_summary,
            #     df_100_summary,
            #     df_110_summary,
            #     df_210_summary,
            ],
            ignore_index=True,
        )

        # df_combined_summary["PARAMETER_NAME"] = df_combined_summary[
        #     "ALARM_DESCRIPTION"
        # ].fillna(df_combined_summary["PARAMETER_NAME"])
        # df_combined_summary.drop(
        #     columns=["ALARM_DESCRIPTION"], inplace=True
        # )  # Remove old column
        df_combined_summary = df_combined_summary[df_combined_summary["COUNT"] > 0]

        ########################################################################################
        # Sort combined DataFrame by 'COUNT' column
        ########################################################################################
        if "COUNT" in df_combined_summary.columns:
            df_combined_summary = df_combined_summary.sort_values(
                ["COUNT"], ascending=False, ignore_index=True
            )

        df_combined_summary = df_combined_summary.head(10)
        ########################################################################################
        # Aggregate total failures per station (without duplicates)
        ########################################################################################
        df_sum_summary = (
            df_combined_summary.groupby("STATION_NAME")["COUNT"].sum().reset_index()
        )

        ########################################################################################
        # Merge unique product serial failures for Station 210
        ########################################################################################
        # if (
        #     not df_210_unique_sn_summary.empty
        #     and "STATION_NAME" in df_210_unique_sn_summary.columns
        #     and "COUNT" in df_210_unique_sn_summary.columns
        # ):
        #     df_210_unique_sn_summary = df_210_unique_sn_summary.rename(
        #         columns={"COUNT": "FAIL_COUNT"}
        #     )

        #     # Merge Station 210's unique product serial failures into df_sum
        #     df_sum_summary = df_sum_summary.merge(
        #         df_210_unique_sn_summary, on="STATION_NAME", how="left"
        #     )

        #     # Replace total failure count with unique serial count for Station 210
        #     df_sum_summary["COUNT"] = df_sum_summary["FAIL_COUNT"].fillna(
        #         df_sum["COUNT"]
        #     )

        #     # Drop the temporary column
        #     df_sum_summary.drop(columns=["FAIL_COUNT"], inplace=True)
        # else:
        #     print(
        #         "Warning: STATION_NAME or COUNT column missing from df_210_unique_sn. Falling back to regular sum."
        #     )

        ########################################################################################
        # Convert NaNs to 0 and ensure integer counts
        ########################################################################################
        df_sum_summary["COUNT"] = df_sum_summary["COUNT"].fillna(0).astype(int)

        ########################################################################################
        # Sort results
        ########################################################################################
        df_sum_summary = df_sum_summary[df_sum_summary["COUNT"] > 0]
        df_sum_summary = df_sum_summary.sort_values(
            ["COUNT"], ascending=False, ignore_index=True
        )

        ########################################################################################
        # Convert DataFrames to a JSON-like format (table-like string)
        ########################################################################################
        def df_to_table(df):
            table_str = df.to_string(index=False)
            return table_str

        df_combined_summary_str = df_to_table(df_combined_summary)
        df_sum_summary_str = df_to_table(df_sum_summary)
        # df_hairpin_origin_summary = pd.concat(
        #     [
        #         # df_40_hairpin_origin_summary,
        #         df_50_hairpin_origin_summary,
        #         df_65_hairpin_origin_summary,
        #     ],
        #     ignore_index=True,
        # )
        
        # df_hairpin_origin_summary["COUNT"] = pd.to_numeric(df_hairpin_origin_summary["COUNT"])
        # df_hairpin_origin_summary["STATION_NAME"] = pd.to_numeric(df_hairpin_origin_summary["STATION_NAME"])
        # df_hairpin_origin_summary = df_hairpin_origin_summary.sort_values(by=["COUNT", "STATION_NAME"], ascending=[False, True], ignore_index=True)
        
        # df_hairpin_origin_summary_str = df_to_table(df_hairpin_origin_summary)

    ########################################################################################
    # Execute hourly queries and fetch data into DataFrames
    ########################################################################################
    df_general = pd.read_sql(query_general, conn)
    df_40 = pd.read_sql(query_40, conn)
    # df_50 = pd.read_sql(query_50, conn)
    # df_60 = pd.read_sql(query_60, conn)
    # df_65 = pd.read_sql(query_65, conn)
    # df_80 = pd.read_sql(query_80, conn)
    # df_100 = pd.read_sql(query_100, conn)
    # df_110 = pd.read_sql(query_110, conn)
    # df_210 = pd.read_sql(query_210, conn)

    # df_210_unique_sn = pd.read_sql(query_210_unique_sn, conn)
    # df_40_hairpin_origin = pd.read_sql(query_40_hairpin_origin, conn)
    # df_50_hairpin_origin = pd.read_sql(query_50_hairpin_origin, conn)
    # df_65_hairpin_origin = pd.read_sql(query_65_hairpin_origin, conn)

    ########################################################################################
    # Combine DataFrames
    ########################################################################################
    df_combined = pd.concat(
        [df_general, 
         df_40
        #  df_50, 
        #  df_60, 
        #  df_65, 
        #  df_80, 
        #  df_100, 
        #  df_110, 
        #  df_210
         ],
        ignore_index=True,
    )

    # df_combined["PARAMETER_NAME"] = df_combined["ALARM_DESCRIPTION"].fillna(
    #     df_combined["PARAMETER_NAME"]
    # )
    # df_combined.drop(columns=["ALARM_DESCRIPTION"], inplace=True)  # Remove old column
    # df_combined = df_combined[df_combined["COUNT"] > 0]

    ########################################################################################
    # Sort combined DataFrame by 'COUNT' column
    ########################################################################################
    if "COUNT" in df_combined.columns:
        df_combined = df_combined.sort_values(
            ["COUNT"], ascending=False, ignore_index=True
        )

    df_combined = df_combined.head(20)
    ########################################################################################
    # Aggregate total failures per station (without duplicates)
    ########################################################################################
    df_sum = df_combined.groupby("STATION_NAME")["COUNT"].sum().reset_index()

    ########################################################################################
    # Merge unique product serial failures for Station 210
    ########################################################################################
    # if (
    #     not df_210_unique_sn.empty
    #     and "STATION_NAME" in df_210_unique_sn.columns
    #     and "COUNT" in df_210_unique_sn.columns
    # ):
    #     df_210_unique_sn = df_210_unique_sn.rename(columns={"COUNT": "FAIL_COUNT"})

    #     # Merge Station 210's unique product serial failures into df_sum
    #     df_sum = df_sum.merge(df_210_unique_sn, on="STATION_NAME", how="left")

    #     # Replace total failure count with unique serial count for Station 210
    #     df_sum["COUNT"] = df_sum["FAIL_COUNT"].fillna(df_sum["COUNT"])

    #     # Drop the temporary column
    #     df_sum.drop(columns=["FAIL_COUNT"], inplace=True)
    # else:
    #     print(
    #         "Warning: STATION_NAME or COUNT column missing from df_210_unique_sn. Falling back to regular sum."
    #     )

    ########################################################################################
    # Convert NaNs to 0 and ensure integer counts
    ########################################################################################
    # df_sum["COUNT"] = df_sum["COUNT"].fillna(0).astype(int)

    ########################################################################################
    # Sort results
    ########################################################################################
    # df_sum = df_sum[df_sum["COUNT"] > 0]
    df_sum = df_sum.sort_values(["COUNT"], ascending=False, ignore_index=True)

    ########################################################################################
    # Convert DataFrames to a JSON-like format (table-like string)
    ########################################################################################
    def df_to_table(df):
        table_str = df.to_string(index=False)
        return table_str

    df_combined_str = df_to_table(df_combined)
    df_sum_str = df_to_table(df_sum)
    # df_hairpin_origin = pd.concat(
    #     [df_40_hairpin_origin, df_50_hairpin_origin, df_65_hairpin_origin],
    #     ignore_index=True
    # )
    # df_hairpin_origin = df_hairpin_origin.sort_values(["COUNT"], ascending=False, ignore_index=True)
    # df_hairpin_origin_str = df_to_table(df_hairpin_origin)

    ########################################################################################
    # Payload with both DataFrames formatted as tables
    ########################################################################################
    payload = {
        "blocks": [
            {"type": "divider"},  # Adds a visual divider

            # Title with Timestamp
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🚨 Fail Count by Parameter:* `{recorded_at} - {(one_hour_before + timedelta(hours=1)).strftime('%H:00')}`"
                }
            },

            # Fail count table - Ensures proper spacing using tab alignment
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```{df_combined_str}```"
                }
            },

            # Section Header - Fails by Station Pareto
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*📊 Fails by Station Pareto:*"
                }
            },

            # Pareto Table - Adjusted for better alignment
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```{df_sum_str}```"
                }
            },

            {"type": "divider"}  # Adds another visual divider
        ]
    }


    if (15 <= current_hour < 16) or (5 <= current_hour < 6):
        payload["blocks"].extend(
            [
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*🚨 Shift Summary (Last Shift)*",
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn", "text": "*Fail count by Parameter:* "
                        + recorded_at_summary
                        + " to "
                        + current_time,
                        # + (recorded_at_summary + timedelta(hours=200)).strftime("%Y-%m-%d %H:00"),
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "```" + df_combined_summary_str + "```",
                    },
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*Fails by Station Pareto:*"},
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "```" + df_sum_summary_str + "```",
                    },
                },
                # {
                #     "type": "section",
                #     "text": {"type": "mrkdwn", "text": "*Fails by Hairpin Station:*"},
                #     },
                # {
                #     "type": "section",
                #     "text": {
                #         "type": "mrkdwn",
                #         "text": "```" + df_hairpin_origin_summary_str + "```",
                #     },
                # },
                {"type": "divider"},  # Add a divider to separate sections clearly
            ]
        )

    ########################################################################################
    # Send the payload to Slack using a webhook
    ########################################################################################
    headers = {"Content-type": "application/json"}
    print(f"DEBUG: Sending message to Slack. Token: {slack_token}, Webhook URL: {url}")
    print(f"DATABRICKS_ACCESS_TOKEN Loaded: {DATABRICKS_ACCESS_TOKEN is not None}")
    print(f"SLACK_TOKEN Loaded: {slack_token is not None}")
    print(f"SLACK_WEBHOOK_URL Loaded: {url is not None}")

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    print(f"Slack API Response: {response.status_code}, {response.text}")

    # response = requests.post(url, headers=headers, data=json.dumps(payload))


########################################################################################
# RUN job()
########################################################################################
job()  # Run the function once
