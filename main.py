import os 
import json
import pandas as pd
from datetime import datetime

directory = '/Users/lorenzofilippini/workspace/datareviewer-v1/data/'

# Extract Files from Directory of Files
def get_files(directory):
    date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f'Started at {date_time}')
    for filename in os.listdir(directory):
        if filename.endswith(('.csv', '.json', '.xlsx')):
            filepath = os.path.join(directory, filename)
            if filename.endswith('.csv'):
                df = pd.read_csv(filepath)
            elif filename.endswith('.json'):
                with open(filepath, 'r') as file:
                    data = json.load(file)
                df = pd.json_normalize(data)
            df['fetched_at'] = date_time
            print(df)










# Main Script Execution
def main():
    get_files(directory)

if __name__ == '__main__':
    main()