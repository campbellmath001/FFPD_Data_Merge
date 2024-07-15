from pathlib import Path
import pandas as pd

# Path to folder containing the csv files to merge
q = Path('./FFPD_Data')


csv_files = q.glob('*.csv')

# create a list of dataframes from the csv files
data = [pd.read_csv(f) for f in csv_files]

raw_df = pd.concat(data, ignore_index=True).drop(['Unnamed: 0'],axis = 1)



