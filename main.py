import pandas as pd
import os
import logging

# Configure logging: This prints timestamps and messages so we can track the script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants: Define these at the top so if folder names change, you only update them here
INPUT_FILE = "raw_data/daily_stock_data.csv"
OUTPUT_DIR = "output"


def load_and_prepare_data(file_path: str) -> pd.DataFrame:
    """
    Step 1: Ingestion
    Reads the CSV and ensures 'date' is a datetime object and data is sorted.
    """
    logging.info(f"Reading data from {file_path}...")
    
    # Load the CSV
    df = pd.read_csv(file_path)
    
    # Convert date strings to actual Date objects
    df['date'] = pd.to_datetime(df['date'])
    
    # Sort by ticker and date. 
    # This is crucial for SMA/EMA math later!
    df = df.sort_values(by=['ticker', 'date'])
    
    logging.info("Data loaded and sorted successfully.")
    return df


def aggregate_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Step 2: Transformation
    Resamples daily data to monthly frequency using specific OHLC logic.
    Each ticker will have 24 rows (2 years of data).
    """
    logging.info("Aggregating daily data to monthly summaries...")

    # We group by 'ticker' and 'date'. 
    # 'ME' stands for Month End - it groups all days within a month together.
    monthly = df.groupby(['ticker', pd.Grouper(key='date', freq='ME')]).agg({
        'open': 'first',    # Snapshot: First trading day of the month
        'close': 'last',    # Snapshot: Last trading day of the month
        'high': 'max',      # Max price reached during the month
        'low': 'min',       # Min price reached during the month
        'volume': 'sum'     # Total volume for the month
    }).reset_index()

    logging.info(f"Aggregation complete. Total monthly rows: {len(monthly)}")
    return monthly


def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Step 3: Calculation Logic.
    Calculates SMA and EMA for each ticker based on the 'close' price.
    """
    logging.info("Calculating technical indicators (SMA & EMA)...")
    
    # Sort to ensure periods are in correct order for rolling math
    df = df.sort_values(by=['ticker', 'date'])
    
    # Group by ticker so math for one stock doesn't leak into another
    group = df.groupby('ticker')['close']
    
    # Simple Moving Averages (SMA 10 and 20)
    df['SMA_10'] = group.transform(lambda x: x.rolling(window=10).mean())
    df['SMA_20'] = group.transform(lambda x: x.rolling(window=20).mean())
    
    # Exponential Moving Averages (EMA 10 and 20)
    # adjust=False ensures we use the recursive formula mentioned in requirements
    df['EMA_10'] = group.transform(lambda x: x.ewm(span=10, adjust=False).mean())
    df['EMA_20'] = group.transform(lambda x: x.ewm(span=20, adjust=False).mean())
    
    return df


def save_by_ticker(df: pd.DataFrame, output_folder: str):
    """
    Step 4: File Writing Logic.
    Partitions the master dataset into 10 individual CSV files by ticker.
    Naming Convention: result_{SYMBOL}.csv
    """
    logging.info(f"Partitioning data into separate files in '{output_folder}'...")
    
    # Get the unique tickers (AAPL, TSLA, etc.)
    tickers = df['ticker'].unique()
    
    for symbol in tickers:
        # Filter the data for this specific ticker
        ticker_df = df[df['ticker'] == symbol]
        
        # Follow the requested naming convention
        file_name = f"result_{symbol}.csv"
        file_path = os.path.join(output_folder, file_name)
        
        # Save to CSV (index=False avoids adding an extra row-number column)
        ticker_df.to_csv(file_path, index=False)
        
    logging.info(f"Successfully generated {len(tickers)} files.")


############################################################

if __name__ == "__main__":
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    try:
        # 1. Ingestion
        raw_data = load_and_prepare_data(INPUT_FILE)
        
        # 2. Transformation
        monthly_data = aggregate_to_monthly(raw_data)
        
        # 3. Calculation
        final_data = add_technical_indicators(monthly_data)
        
        # 4. Partitioning & Saving <-- THE FINAL STEP
        save_by_ticker(final_data, OUTPUT_DIR)
        
        logging.info("ASSESSMENT COMPLETED SUCCESSFULLY!")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")        
        logging.info("ASSESSMENT FAILED.")