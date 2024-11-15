import logging
from app import open_files, check_missing_values, remove_duplicates, save_to_csv

logger = logging.getLogger(__name__)

def main():
    # Load data
    df = open_files()
    if df is None:
        logger.error(" [⛔️] No data loaded. Exiting program...")
        return
    
    # Remove duplicates
    df_cleaned = remove_duplicates(df)
    
    # Check for missing values
    missing_values = check_missing_values(df_cleaned)
    if not missing_values:
        logger.info(" [✅] Data Quality Check Passed: No missing values found.")
    else:
        logger.warning(" [⛔️] Data Quality Issue: Missing values detected.")
    
    # Save the cleaned DataFrame to CSV
    save_to_csv(df_cleaned)

if __name__ == '__main__':
    main()
