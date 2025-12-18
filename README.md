# Data Engineering Intern Task: Financial Data Pipeline

## Overview
This project is a Python-based data pipeline designed to transform daily historical stock prices into monthly summaries. It calculates key technical indicators (SMA and EMA) and partitions the final dataset into individual files per stock ticker.

## Technical Approach
- **Modular Design:** The script separates logic into distinct functions: `load_and_prepare_data`, `aggregate_to_monthly`, `add_technical_indicators`, and `save_by_ticker`.
- **Vectorization:** As per the evaluation criteria, the pipeline prioritizes native Pandas functions (like `.rolling()` and `.ewm()`) over manual loops to ensure high performance and scalability.
- **Data Integrity:** The pipeline uses snapshot logic for 'Open' and 'Close' values (first/last day) rather than averages, maintaining high accuracy for financial analysis.

## Practical Assumptions
Based on the provided requirements and standard financial data practices, the following assumptions were made:
1. **Indicator Windows:** Since SMA-10/20 and EMA-10/20 require 10 or 20 months of historical data, the first rows of each output file will contain `NaN` values where sufficient history is not yet available.
2. **First EMA Calculation:** The Exponential Moving Average (EMA) uses the first available price point as the initial seed, following the standard recursive formula: $EMA = (Price - Prev\_EMA) \times Multiplier + Prev\_EMA$.
3. **Monthly Volume:** Volume is calculated as the cumulative sum of all trading volume within the calendar month.
4. **Timeframe:** The dataset is assumed to cover a continuous 24-month period, resulting in exactly 24 rows per ticker.

## How to Run & Verify

Prerequisites
- Python 3.x
- Pandas library (`pip install pandas`)

1. Initial State: The repository includes a pre-populated output/ directory for immediate review of the 10 ticker files.

2. Setup: Ensure you have Python and Pandas installed (pip install pandas).

3. Execution: To verify the pipeline's automation:

    - Delete the existing CSV files in the output/ folder.
    - Run the main script:
        python main.py

4. Verification: The script will re-generate 10 CSV files (e.g., result_AAPL.csv) in the output/ folder, each containing exactly 24 rows of processed monthly data.

## Tools Used
1. Python 3.x

2. Pandas Library   
