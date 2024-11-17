import os
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

directory = os.getenv("DIRECTORY")

# ////////////////////////////////////////////////////////////////////////////////////

def open_files():
    """
    Extracts data from all files in the data directory,
    and processes them into a list of DataFrames.
    """
    data_frames = []
    try:
        for filename in os.listdir(directory):
            if filename.endswith(('.csv', '.json', '.xlsx')):
                date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info("--------------------------------------------------------------------------------")
                logger.info(f' [..] Opening File {filename} at: {date_time}...')
                
                filepath = os.path.join(directory, filename)
                df = None

                if filename.endswith('.csv'):
                    df = pd.read_csv(filepath)

                if df is not None:
                    df['fetched_at'] = date_time
                    logger.info(f' [✅] Opened File {filename} Successfully.')
                    data_frames.append(df)
                else:
                    logger.warning(f" [⚠️] File {filename} could not be processed.")
        
        if not data_frames:
            logger.warning(" [⚠️] No valid files found to process.")
        
    except Exception as e:
        logger.error(f' [⛔️] Error processing files in the directory: {directory}', exc_info=True)
    
    return pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()

# ////////////////////////////////////////////////////////////////////////////////////

def remove_duplicates(df, subset=None, keep='first', log_duplicates=True):
    """
    Finds and removes duplicate rows in the DataFrame.
    """
    logger.info(" [..] Checking for duplicate rows in the DataFrame...")
    duplicate_count = df.duplicated(subset=subset, keep=False).sum()
    
    if log_duplicates:
        if duplicate_count == 0:
            logger.info(" [✅] No duplicate rows found.")
        else:
            logger.warning(f" [{duplicate_count}] duplicate rows found. Removing duplicates.")
    
    # Remove duplicates
    df_cleaned = df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)
    return df_cleaned


# ////////////////////////////////////////////////////////////////////////////////////

def check_missing_values(df_cleaned):
    """
    Checks for missing values in the given DataFrame.
    """
    logger.info(" [..] Checking for missing values in the DataFrame...")
    missing_values = df_cleaned.isnull().sum()
    total_rows = len(df_cleaned)
    
    missing_values_info = {
        col: {'count': count, 'percentage': (count / total_rows) * 100}
        for col, count in missing_values.items() if count > 0
    }
    
    if missing_values_info:
        logger.info(" [⛔️] Missing values found in the following columns:")
        for col, info in missing_values_info.items():
            logger.info(f" [ℹ️ ] --- > {col}: {info['count']} missing values ({info['percentage']:.2f}%)")
    else:
        logger.info(" [✅] No missing values found in the DataFrame.")
    
    return missing_values_info

# ////////////////////////////////////////////////////////////////////////////////////

def standardize_column_names(df):
    """
    Standardizes column names to snake_case.
    """
    logger.info(" [..] Standardizing column names...")
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    logger.info(" [✅] Column names standardized.")
    return df

# ////////////////////////////////////////////////////////////////////////////////////

def save_to_csv(df, output_dir="data/processed", filename_prefix="cleaned_data", metadata=""):
    """
    Saves the cleaned DataFrame to a CSV file in the specified output directory.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{metadata}_{timestamp}.csv" if metadata else f"{filename_prefix}_{timestamp}.csv"
    file_path = os.path.join(output_dir, filename)
    
    try:
        df.to_csv(file_path, index=False)
        logger.info(f" [✅] Data saved successfully to {file_path}")
    except Exception as e:
        logger.error(f" [⛔️] Failed to save data to {file_path}", exc_info=True)
    
    return file_path

# ////////////////////////////////////////////////////////////////////////////////////

# def open_cleaned_file(filename, output_dir="data/processed"):
#     """
#     Reads the cleaned CSV file from the processed directory into a DataFrame.
#     """
#     filepath = os.path.join(output_dir, filename)
#     try:
#         df = pd.read_csv(filepath)
#         logger.info(f" [✅] Cleaned Data loaded from {filepath}")
#         return df
#     except Exception as e:
#         logger.error(f" [⛔️] Error loading cleaned data from {filepath}", exc_info=True)
#         return None