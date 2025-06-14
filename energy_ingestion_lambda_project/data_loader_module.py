
import pandas as pd
import boto3
import os
import io
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Union

# Initialize S3 client
s3_client = boto3.client('s3')

# Define your S3 bucket name from environment variables for flexibility
# IMPORTANT: Ensure 'S3_BUCKET_NAME' is set in your environment
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'walmart-sparkathon-energy-data')

# Base S3 prefix for the hourly raw data (matches your provided path)
HOURLY_DATA_BASE_PREFIX = 'historical_store1_energy_data/'


def _read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Helper function to read a CSV file from S3 into a pandas DataFrame."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_content))
        # Ensure 'timestamp' column is datetime type if it exists
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Ensure 'date' column is date type if it exists
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date
        return df
    except s3_client.exceptions.NoSuchKey:
        print(f"Warning: S3 object not found: s3://{bucket}/{key}")
        return pd.DataFrame()  # Return empty DataFrame if file not found
    except Exception as e:
        print(f"Error reading s3://{bucket}/{key}: {e}")
        raise  # Re-raise for upstream error handling


def get_latest_hourly_data() -> Dict[str, Any]:
    """
    Finds and loads the latest hourly CSV file from S3 based on the new path structure.
    It iterates backward from the current hour to find the most recent file.
    """
    current_time = datetime.now()
    # Look back a few hours/days in case the latest file is from yesterday
    # We'll construct potential paths and look for files
    for i in range(48):  # Check last 48 hours
        check_time = current_time - timedelta(hours=i)

        # S3 folder path for the specific day (e.g., historical_store1_energy_data/year=YYYY/month=MM/day=DD/)
        folder_prefix = (
            f"{HOURLY_DATA_BASE_PREFIX}"
            f"year={check_time.year}/"
            f"month={check_time.strftime('%m')}/"
            f"day={check_time.strftime('%d')}/"
        )

        # The specific filename we expect for this hour
        expected_filename = f"store_energy_data_{check_time.strftime('%H')}.csv"
        s3_key = folder_prefix + expected_filename

        try:
            # Check if the specific file exists
            response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=s3_key)
            if 'Contents' in response:
                # If Contents exist and the key matches (exact file found)
                if any(obj['Key'] == s3_key for obj in response['Contents']):
                    print(f"Found latest hourly data file: s3://{S3_BUCKET_NAME}/{s3_key}")
                    df = _read_csv_from_s3(S3_BUCKET_NAME, s3_key)
                    if not df.empty:
                        return df.iloc[0].to_dict()
        except Exception as e:
            print(f"Error checking S3 key {s3_key}: {e}")
            # Continue to previous hour in case of transient S3 errors

    print("No recent hourly data found in S3 with the expected path structure.")
    return {}


def get_recent_hourly_data(lookback_hours: int) -> List[Dict[str, Any]]:
    """
    Loads multiple recent hourly CSV files from S3 based on the new path structure.

    Args:
        lookback_hours (int): The number of recent hours to retrieve data for.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary represents an hourly data row.
    """
    all_recent_data = []
    current_time = datetime.now()

    for i in range(lookback_hours):
        check_time = current_time - timedelta(hours=i)
        folder_prefix = (
            f"{HOURLY_DATA_BASE_PREFIX}"
            f"year={check_time.year}/"
            f"month={check_time.strftime('%m')}/"
            f"day={check_time.strftime('%d')}/"
        )
        expected_filename = f"store_energy_data_{check_time.strftime('%H')}.csv"
        s3_key = folder_prefix + expected_filename

        try:
            response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=s3_key)
            if 'Contents' in response:
                if any(obj['Key'] == s3_key for obj in response['Contents']):
                    df = _read_csv_from_s3(S3_BUCKET_NAME, s3_key)
                    if not df.empty:
                        all_recent_data.append(df.iloc[0].to_dict())
        except Exception as e:
            print(f"Error fetching data for hour {check_time.isoformat()} at {s3_key}: {e}")

    # Sort data by timestamp in ascending order
    all_recent_data.sort(key=lambda x: x['timestamp'])
    return all_recent_data


def load_hourly_data(start_date: date, end_date: date) -> List[Dict[str, Any]]:
    """
    Loads all hourly CSVs from S3 within a specified date range, conforming to the new path structure.

    Args:
        start_date (date): The start date (inclusive).
        end_date (date): The end date (inclusive).

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary represents an hourly data row.
    """
    all_hourly_data = []
    current_date_iter = start_date  # Use a different variable name to avoid confusion

    while current_date_iter <= end_date:
        for hour in range(24):
            check_time = datetime(current_date_iter.year, current_date_iter.month, current_date_iter.day, hour)
            folder_prefix = (
                f"{HOURLY_DATA_BASE_PREFIX}"
                f"year={check_time.year}/"
                f"month={check_time.strftime('%m')}/"
                f"day={check_time.strftime('%d')}/"
            )
            expected_filename = f"store_energy_data_{check_time.strftime('%H')}.csv"
            s3_key = folder_prefix + expected_filename

            try:
                response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=s3_key)
                if 'Contents' in response:
                    if any(obj['Key'] == s3_key for obj in response['Contents']):
                        df = _read_csv_from_s3(S3_BUCKET_NAME, s3_key)
                        if not df.empty:
                            all_hourly_data.append(df.iloc[0].to_dict())
            except Exception as e:
                print(f"Error fetching data for {check_time.isoformat()} at {s3_key}: {e}")

        current_date_iter += timedelta(days=1)

    all_hourly_data.sort(key=lambda x: x['timestamp'])
    return all_hourly_data


# --- Example Usage (for local testing) ---
if __name__ == "__main__":
    # Ensure S3_BUCKET_NAME environment variable is set for local testing
    # Or, uncomment and set it directly for quick local tests:
    # os.environ['S3_BUCKET_NAME'] = 'walmart-sparkathon-energy-data'

    # --- Test get_latest_hourly_data ---
    print("--- Testing get_latest_hourly_data ---")
    try:
        latest_data = get_latest_hourly_data()
        if latest_data:
            print("Latest hourly data found:")
            print(pd.Series(latest_data).to_string())
        else:
            print("No latest hourly data available.")
    except Exception as e:
        print(f"Failed to get latest hourly data: {e}")

    # --- Test get_recent_hourly_data ---
    print("\n--- Testing get_recent_hourly_data (last 3 hours) ---")
    try:
        recent_data = get_recent_hourly_data(lookback_hours=3)
        if recent_data:
            print(f"Found {len(recent_data)} recent hourly data points:")
            for row in recent_data:
                print(f"  Timestamp: {row['timestamp']}, Total Energy: {row.get('Total_Energy(t)', 'N/A'):.2f}")
        else:
            print("No recent hourly data available.")
    except Exception as e:
        print(f"Failed to get recent hourly data: {e}")

    # --- Test load_hourly_data for a date range ---
    print("\n--- Testing load_hourly_data (specific date range) ---")
    try:
        # Adjust these dates based on when you've ingested data
        test_start_date_hourly = date(2025, 6, 13)
        test_end_date_hourly = date(2025, 6, 13)  # Just one day for testing

        hourly_range_data = load_hourly_data(test_start_date_hourly, test_end_date_hourly)
        if hourly_range_data:
            print(
                f"Found {len(hourly_range_data)} hourly data points for {test_start_date_hourly} to {test_end_date_hourly}:")
            for i, row in enumerate(hourly_range_data[:5]):
                print(
                    f"  {i + 1}: Timestamp: {row['timestamp']}, Solar Used to Charge: {row.get('Solar_Used_to_Charge_Battery(t)', 'N/A'):.2f}")
            if len(hourly_range_data) > 5:
                print("  ...")
        else:
            print(f"No hourly data found for {test_start_date_hourly} to {test_end_date_hourly}.")
    except Exception as e:
        print(f"Failed to load hourly range data: {e}")