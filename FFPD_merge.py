from pathlib import Path
import pandas as pd

# Path to folder containing the csv files to merge
q = Path('./FFPD_Data')

csv_files = q.glob('*.csv')

# create a list of dataframes from the csv files
data = [pd.read_csv(f) for f in csv_files]

raw_df = pd.concat(data, ignore_index=True).drop(['Unnamed: 0'],axis = 1)

df = (
        raw_df.rename(
            columns={'Date Time':'Incident_Time', 'inci #:':'Incident_Id'}
        )
        .assign(
            Incident_Time=lambda x: pd.to_datetime(x['Incident_Time'], format='%m/%d/%Y %I:%M:%S %p'),
            Block=lambda x: x['Address'].str.extract(r'(\d+) Block of (?P<street>.+)')[0],
            Street=lambda x: x['Address'].str.extract(r'(\d+) Block of (?P<street>.+)')['street'],
            Full_Address=lambda x: x['Block'].astype(str) + ' ' + x['Street'] + ' Fairfield, Ca'
        )
        .sort_values(
            by='Incident_Time'
        )
)



