import os
import logging
import json
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
    and processes them into a single DataFrame.
    """
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
                    
                elif filename.endswith('.json'):
                    try:
                        with open(filepath, 'r') as file:
                            data = json.load(file)
                            if isinstance(data, dict):
                                first_list = next((value for value in data.values() if isinstance(value, list)), None)
                                
                                if first_list is not None:
                                    df = pd.json_normalize(first_list)
                                else:
                                    logger.error(f' [⛔️] No list found in JSON structure for file: {filename}')
                                    continue
                            else:
                                logger.error(f' [⛔️] Unexpected JSON structure for file: {filename}')
                                continue
                            
                    except json.JSONDecodeError as json_err:
                        logger.error(f' [⛔️] Error decoding JSON from file: {filename}', exc_info=json_err)
                        continue
                    
                elif filename.endswith('.xlsx'):
                    df = pd.read_excel(filepath)

                if df is not None:
                    df['fetched_at'] = date_time
                    logger.info(f' [✅] Opened File {filename} Successfully.')
                    logger.info(f' [✅] DataFrame Created Successfully.')
                    return df 
                    

    except Exception as e:
        logger.error(f' [⛔️] Error Processing Files in the directory: {directory}', exc_info=True)

# ////////////////////////////////////////////////////////////////////////////////////

def check_missing_values(df):
    """
    Checks for missing values in the given DataFrame (File).
    """
    logger.info(" [..] Checking for missing values in the DataFrame...")

    missing_values = df.isnull().sum()
    missing_values = missing_values[missing_values > 0]
    missing_values_dict = missing_values.to_dict()
    
    if missing_values_dict:
        logger.info(" [⛔️] Missing values found in the following columns:")
        for col, count in missing_values_dict.items():
            logger.info(f" [ℹ️ ] --- > {col}: {count} missing values")
    else:
        logger.info(" [✅] No missing values found in the DataFrame.")
    
    return missing_values_dict
