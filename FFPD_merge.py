from pathlib import Path
import pandas as pd
import adbc_driver_sqlite.dbapi as db_driver

# Path to folder containing the csv files to merge
q = Path('./FFPD_Data')

csv_files = q.glob('*.csv')

# create a list of dataframes from the csv files
data = [pd.read_csv(f) for f in csv_files]

# concatenate the dataframes and then drop the excess index column
raw_df = pd.concat(data, ignore_index=True).drop(['Unnamed: 0'],axis = 1)

# clean the data and format it for merging with old data
df1 = (
        raw_df.rename(
            columns={'Date Time':'Incident_Time', 'inci #':'Incident_ID'}
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

# reload the dataframe of old data and rename the incident_id column for compatability
df2 = pd.read_pickle('ffpd_data_11_2_23_to_6_21_24.pickle').assign(Incident_ID=lambda x: x['Incident_ID'].astype(int))

# concatenate old and new data and then sort by datetime
combined_df = pd.concat([df1, df2], ignore_index=True).sort_values(by='Incident_Time')

# open a database connection
try:
    conn = db_driver.connect('FFPD.db')
except Exception as e:
    print("error when connecting to db: {e}")

try:
    with conn:
        combined_df.to_sql('Data_Log', conn, if_exists = "replace", index = False)
        print('Database write successful')
except Exception as e:
    print(f"error when writing to database: {e}")
finally:
    conn.close()

