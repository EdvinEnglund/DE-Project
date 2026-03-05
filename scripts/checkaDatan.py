import pandas as pd
import requests

# The source URL for the NYC Yellow Taxi trip data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2013-01.parquet"
file_name = "yellow_tripdata_2012-01.parquet"

def download_and_print(url, output_file):
    print(f"Downloading file from {url}...")
    
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        with open(output_file, 'wb') as f:
            f.write(response.content)
        print("Download complete.")
        
        # Read the parquet file using pandas
        # This requires the 'pyarrow' engine
        df = pd.read_parquet(output_file)
        
        # Print the data
        print("\n--- Data Preview ---")
        print(df.head())
        
        # Optional: Print basic info about columns and data types
        print("\n--- Dataset Info ---")
        print(df.info())
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

    return df

def clean_data(df):

    clean_df= pd.DataFrame()
    keep_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'PULocationID', 'DOLocationID', 'payment_type', 'tip_amount', 'total_amount']  
    clean_df = df[keep_columns]
    return clean_df

if __name__ == "__main__":
    df = download_and_print(url, file_name)
    clean_df = clean_data(df)
    print(clean_df.head())
    print(clean_df.columns)
    print(clean_df.info())