from alpha_vantage.timeseries import TimeSeries
from pprint import pprint

# Only returns 15 days
# ts = TimeSeries(key='4EIWH7LCJSVQVU7G', output_format='pandas')
# data, meta_data = ts.get_intraday(symbol='MSFT', interval='1min', outputsize='full')
#
# pprint(data)
# pprint(meta_data)


import requests
import pandas as pd


token = 'pk_466a19c8eff440c08530eeaf8ce4f331'
r = requests.get(
    'https://cloud.iexapis.com/time-series/REPORTED_FINANCIALS/AAPL/10-Q?from=2018-01-01&to=2019-06-01?token={}'.format(token)
)
pprint(r.text)
data = r.json()
pprint(data)
df = pd.DataFrame(data)

pprint(df)