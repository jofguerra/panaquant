import os
import sys
import pandas as pd
import datetime as dt
from pprint import pprint


def read_csv_files(remove_heading=False):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    scan_dir = os.path.join(dir_path, 'new_csv_files', '75_conf')
    out_df = pd.DataFrame()
    for root, dirs, files in os.walk(scan_dir):
        for filename in files:
            if filename == '.DS_Store':
                continue
            file_path = os.path.join(scan_dir, filename)
            if remove_heading:
                with open(file_path, 'r') as fin:
                    data = fin.read().splitlines(True)
                with open(file_path, 'w') as fout:
                    fout.writelines(data[2:])
            try:
                df = pd.read_csv(file_path)
            except:
                continue
            # print(filename)
            # df['frequency'] = df.groupby('ticker')['ticker'].transform('count')
            # df['price'] = df['price'].replace(r'[\$,]', '', regex=True).astype(float)
            # df_filtered = df.loc[df['price'] >= 4]
            # df_filtered = df_filtered.loc[df_filtered['volume'] >= 1e5]
            # df_filtered = df_filtered.loc[df_filtered['trades'] >= 3]
            # df_filtered = df_filtered.loc[df_filtered['frequency'] > 5]
            # # df_filtered = df_filtered.loc[df_filtered['Signal'] == 'Buy']
            # date_string = filename.split('.')[0][-8:]
            # date = dt.datetime.strptime(date_string, '%y-%m-%d')
            # stocks = set(zip(df_filtered['ticker'], df_filtered['signal']))
            #
            #
            #
            # rows = []
            # for stock in stocks:
            #     rows.append({'ticker': stock[0], 'signal': stock[1], 'date': date, 'trade_date': date})
            # date_df = pd.DataFrame(rows)
            df = df[
                ['date', 'ticker', 'name', 'price',
                 'volume', 'signal', 'success',
                 'tscore', 'trades']]
            out_df = pd.concat([out_df, df])

    out_df = out_df.sort_values(by='date')
    out_df = out_df.reset_index(drop=True)
    out_df.index.name = 'index'
    pprint(out_df)
    out_df.to_csv(os.path.join(dir_path, 'out_df_75_all.csv'))


if __name__ == "__main__":
    read_csv_files()

