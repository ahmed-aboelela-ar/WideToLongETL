import boto3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import os
import time
import argparse
import shutil
from calendar import monthrange

# Create argument parser
parser = argparse.ArgumentParser(description="Script with update delay parameter.")

# Add update delay argument
parser.add_argument(
    "--update-delay",
    type=float,
    required=True,
    help="Delay in minutes for updates."
)

parser.add_argument(
    "--generate-mode",
    choices=['daily', 'monthly'],
    required=True,
    help="Generate either one day or entire month per iteration"
)

parser.add_argument(
    "--region",
    type=str,
    default='eu-central-1',
    help='The region we are currently operation from.'
)
import os

print(os.getenv("AWS_REGION"))  # Primary AWS region
print(os.getenv("AWS_DEFAULT_REGION"))  # Default region (if set)
# Parse arguments
args = parser.parse_args()
# AWS setup
s3_client = boto3.client('s3', region_name=args.region)

bucket = 'data-landing-zone-aboelela-entrix-task'

# Timezone and DST dates
tz = pytz.timezone('Europe/Berlin')
start_date = datetime(2018, 1, 1, tzinfo=tz)
end_date = datetime(2025, 4, 4, tzinfo=tz)

dst_start = {
    2018: datetime(2018, 3, 25, tzinfo=tz),
    2019: datetime(2019, 3, 31, tzinfo=tz),
    2020: datetime(2020, 3, 29, tzinfo=tz),
    2021: datetime(2021, 3, 28, tzinfo=tz),
    2022: datetime(2022, 3, 27, tzinfo=tz),
    2023: datetime(2023, 3, 26, tzinfo=tz),
    2024: datetime(2024, 3, 31, tzinfo=tz),
    2025: datetime(2025, 3, 30, tzinfo=tz)
}

dst_end = {
    2018: datetime(2018, 10, 28, tzinfo=tz),
    2019: datetime(2019, 10, 27, tzinfo=tz),
    2020: datetime(2020, 10, 25, tzinfo=tz),
    2021: datetime(2021, 10, 31, tzinfo=tz),
    2022: datetime(2022, 10, 30, tzinfo=tz),
    2023: datetime(2023, 10, 29, tzinfo=tz),
    2024: datetime(2024, 10, 27, tzinfo=tz),
    2025: datetime(2025, 10, 26, tzinfo=tz)
}

columns = ['Delivery day'] + [f'Hour {i}' for i in range(1, 3)] + ['Hour 3A', 'Hour 3B'] + [f'Hour {i}' for i in
                                                                                            range(4, 25)]


def handle_dst(date, prices_dict):
    year = date.year
    if date.date() == dst_start[year].date():
        prices_dict['Hour 3A'] = np.nan
        prices_dict['Hour 3B'] = np.nan
    elif date.date() == dst_end[year].date():
        pass  # Keep both 3A and 3B
    else:
        prices_dict['Hour 3B'] = np.nan
    return prices_dict


def generate_day_data(date):
    prices = np.round(np.random.uniform(0, 100, 25), 2)
    prices_dict = {col: price for col, price in zip(columns[1:], prices)}
    return handle_dst(date, prices_dict)


def process_daily_mode():
    global current_date
    if current_date > end_date:
        return {}

    date = current_date
    year = date.year
    month = date.month
    file_key = f"{year}/{month:02d}"

    # Load or create dataframe
    local_folder = f"daa_market/{file_key}"
    local_path = f"{local_folder}/daa_market.csv"

    if os.path.exists(local_path):
        df = pd.read_csv(local_path)
    else:
        df = pd.DataFrame(columns=columns)
        os.makedirs(local_folder, exist_ok=True)

    # Add new row
    new_row = {
        'Delivery day': date.strftime('%d/%m/%Y'),
        **generate_day_data(date)
    }
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    current_date += timedelta(days=1)
    return {file_key: df}


def process_monthly_mode():
    global current_date
    if current_date > end_date:
        return {}

    year = current_date.year
    month = current_date.month
    days_in_month = monthrange(year, month)[1]

    data = []
    for day in range(1, days_in_month + 1):
        date = current_date.replace(day=day)
        if date > end_date:
            break

        data.append({
            'Delivery day': date.strftime('%d/%m/%Y'),
            **generate_day_data(date)
        })

    # Move to next month
    if current_date.month == 12:
        current_date = current_date.replace(year=year + 1, month=1, day=1)
    else:
        current_date = current_date.replace(month=month + 1, day=1)

    return {f"{year}/{month:02d}": pd.DataFrame(data)}


def save_and_upload(files):
    if not files:
        return


    # Save locally first
    for file_key, df in files.items():
        os.makedirs(f"daa_market/{file_key}", exist_ok=True)
        local_path = f"daa_market/{file_key}/daa_market.csv"
        df.to_csv(local_path, index=False)

    # Upload to S3
    for file_key in files:
        local_path = f"daa_market/{file_key}/daa_market.csv"
        s3_key = f"daa_market/{file_key}/daa_market.csv"
        s3_client.upload_file(local_path, bucket, s3_key)


# Clean existing data
if os.path.exists('daa_market'):
    shutil.rmtree('daa_market')
os.mkdir('daa_market')

iteration = 0
current_date = datetime(2018, 1, 1, tzinfo=tz)
while current_date <= end_date:
    if args.generate_mode == 'daily':
        modified_files = process_daily_mode()
    else:
        modified_files = process_monthly_mode()

    if modified_files:
        save_and_upload(modified_files)
        print(f"Iteration {iteration}: Uploaded {len(modified_files)} files")
        iteration += 1

    time.sleep(args.update_delay * 60)

print("Simulation complete. Final date:", current_date.strftime('%Y-%m-%d'))