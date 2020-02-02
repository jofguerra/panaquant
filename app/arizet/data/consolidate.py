import os
import sys
import pandas as pd
import datetime as dt
from pprint import pprint


def read_csv_files(remove_heading=False):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    scan_dir = os.path.join(dir_path, 'csv_files', '97_conf')
    out_df = pd.DataFrame()
    for root, dirs, files in os.walk(scan_dir):
        for filename in files:
            file_path = os.path.join(scan_dir, filename)
            if remove_heading:
                with open(file_path, 'r') as fin:
                    data = fin.read().splitlines(True)
                with open(file_path, 'w') as fout:
                    fout.writelines(data[2:])

            df = pd.read_csv(file_path)
            print(filename)
            df['frequency'] = df.groupby('Ticker')['Ticker'].transform('count')
            df['Price'] = df['Price'].replace(r'[\$,]', '', regex=True).astype(float)
            df_filtered = df.loc[df['Price'] >= 4]
            df_filtered = df_filtered.loc[df_filtered['Volume'] >= 1e5]
            df_filtered = df_filtered.loc[df_filtered['Trades'] >= 3]
            df_filtered = df_filtered.loc[df_filtered['frequency'] > 3]
            date_string = filename.split('.')[0][-8:]
            date = dt.datetime.strptime(date_string, '%Y%m%d')
            stocks = set(df_filtered['Ticker'])
            date_df = pd.DataFrame()
            date_df['ticker'] = list(stocks)
            date_df['date'] = date
            out_df = pd.concat([out_df, date_df])

    out_df = out_df.sort_values(by='date')
    out_df = out_df.reset_index(drop=True)
    pprint(out_df)
    out_df.to_csv(os.path.join(dir_path, 'out_df.csv'))


if __name__ == "__main__":
    read_csv_files()
