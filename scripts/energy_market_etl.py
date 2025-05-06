import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
import re

def to_snake_case(name):
    return re.sub(r'\s+', '_', name).lower()

def dataframe_processor(df, timezone='Europe/Berlin'):
    df.columns = [to_snake_case(col) for col in df.columns]

    df_melted = df.melt(id_vars=["delivery_day"], var_name="hour", value_name="price")
    df_melted["hour_name"] = df_melted["hour"]
    df_melted["hour"] = df_melted["hour"].str.extract(r'(\d+)').astype(float)
    df_melted["timestamp"] = pd.to_datetime(df_melted["delivery_day"], dayfirst=True) + pd.to_timedelta(
        df_melted["hour"] - 1, unit="h")
    try:
        df_melted["timestamp"] = df_melted["timestamp"].dt.tz_localize(timezone, ambiguous='infer').dt.tz_localize(None)
        df_melted["timestamp_utc"] = df_melted["timestamp"].dt.tz_localize(timezone, ambiguous='infer').dt.tz_convert(
            "UTC").dt.tz_localize(None)
    except:
        localized = df_melted["timestamp"].dt.tz_localize(timezone, ambiguous='NaT', nonexistent='NaT')
        mask = localized.isna()
        df_ambiguous = df_melted[mask]
        df_non_ambiguous = df_melted[~mask]
        hour_3a_3b = df_ambiguous[df_ambiguous["hour_name"].isin(["Hour 3A", "Hour 3B"])]
        prices = hour_3a_3b["price"].values
        if pd.isna(prices).all():
            df_melted = df_melted.dropna(subset=['price'])
            df_melted["timestamp"] = df_melted["timestamp"].dt.tz_localize(timezone, ambiguous='infer').dt.tz_localize(
                None)
            df_melted["timestamp_utc"] = df_melted["timestamp"].dt.tz_localize(timezone,
                                                                               ambiguous='infer').dt.tz_convert(
                "UTC").dt.tz_localize(None)
        else:
            ambiguous_array = df_ambiguous['hour_name'] == 'hour_3a'
            localized = df_ambiguous['timestamp'].dt.tz_localize(timezone, ambiguous=ambiguous_array)
            df_ambiguous['timestamp_utc'] = localized.dt.tz_convert('UTC').dt.tz_localize(None)
            df_non_ambiguous["timestamp"] = df_non_ambiguous["timestamp"].dt.tz_localize(timezone,
                                                                                         ambiguous='infer').dt.tz_localize(
                None)
            df_non_ambiguous["timestamp_utc"] = df_non_ambiguous["timestamp"].dt.tz_localize(timezone,
                                                                                             ambiguous='infer').dt.tz_convert(
                "UTC").dt.tz_localize(None)
            df_melted = pd.concat([df_non_ambiguous, df_ambiguous]).sort_index()



    df_melted = df_melted.dropna(subset=['price'])
    # Calculate UTC offset
    df_melted["utc_offset"] = (
        (df_melted["timestamp"] - df_melted["timestamp_utc"])  # Timedelta difference
        .dt.total_seconds()  # Convert to seconds
        .div(3600)  # Convert to hours
        .astype(int)  # Round to whole hours
    )

    # Format as "UTC+1" or "UTC+2"
    df_melted["timezone"] = f"{timezone} UTC+" + df_melted["utc_offset"].astype(str)

    df_melted['year'] = df_melted['timestamp'].dt.year
    df_melted['month'] = df_melted['timestamp'].dt.month

    df_melted_final = df_melted[["timestamp", "timestamp_utc", "price", "timezone", "year", "month"]]
    return df_melted_final


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'target_bucket', 'database_name', 'table_name', 'modified_file'])
    source_path = args['modified_file']
    print(f"Modified file: {source_path}")
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    timezone='Europe/Berlin'
    source_path = f"s3://{args['source_bucket']}/{args['modified_file']}"

    df = pd.read_csv(source_path, comment='#')
    df_melted_final = dataframe_processor(df, timezone=timezone)
    top_folder = args['modified_file'].split("/")[0]
    year = args['modified_file'].split("/")[1]
    month = args['modified_file'].split("/")[2]
    output_path = f"s3://{args['target_bucket']}/{top_folder}/{year}/{month}/daa_market.parquet"
    df_melted_final.to_parquet(output_path)

    job.commit()