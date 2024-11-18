import logging
import glob
import os
from app import (open_files, 
                 check_missing_values, 
                 remove_duplicates, 
                 save_to_csv, 
                 standardize_column_names,
                 )

logger = logging.getLogger(__name__)

filepath = "/Users/lorenzofilippini/Desktop/datareviewer-v1/data/processed/cleaned_data_20241115_152641.csv"

def log_dataframe_info(df, message=""):
    """
    Logs information about the DataFrame.
    """
    logger.info(f"{message} Shape: {df.shape}")

def main():
    # Load data
    df = open_files()
    if df.empty:
        logger.error(" [⛔️] No data loaded. Exiting program...")
        return
    log_dataframe_info(df, "Loaded data")
    
    # Standardize column names
    df = standardize_column_names(df)
    log_dataframe_info(df, "After standardizing column names")

    # Remove duplicates
    df_cleaned = remove_duplicates(df)
    log_dataframe_info(df_cleaned, "After removing duplicates")

    # Check for missing values
    missing_values = check_missing_values(df_cleaned)
    if missing_values:
        logger.warning(" [ℹ️] Missing values detected. Halting execution.")
        return
    
    # Save cleaned data
    save_to_csv(df_cleaned)
    logger.info(" [✅] Data Cleaning Process Completed Successfully.")

    # Load cleaned data
    list_of_files = glob.glob('/Users/lorenzofilippini/Desktop/datareviewer-v1/data/processed/*.csv')
    if not list_of_files:
        logger.error(" [⛔️] No files found in the processed directory.")
        return
        
    # Run Main Analysis

if __name__ == '__main__':
    main()
