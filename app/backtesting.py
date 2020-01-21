from zipline import TradingAlgorithm
from zipline.finance import commission, slippage
from zipline.data import bundles
from zipline.api import order, symbol
import zipline
import datetime as dt
from datetime import timezone
import matplotlib.pyplot as plt

stocks = ['AAPL', 'MSFT']

dist = {
    'AAPL': 0,
    'MSFT': 1
}


def initialize(context):

    context.has_ordered = False
    context.stocks = stocks

    context.set_commission(commission.PerShare(cost=.0075, min_trade_cost=1.0))
    context.set_slippage(slippage.VolumeShareSlippage())


def handle_data(context, data):
    if not context.has_ordered:
        for stock in context.stocks:
            if dist[stock] > 0:
                order(symbol(stock), 10 * dist[stock])


if __name__ == "__main__":
    fig, ax = plt.subplots()
    for i in range(10):
        dist['AAPL'] += 0.1
        dist['MSFT'] -= 0.1
        perf = zipline.run_algorithm(
            start=dt.datetime(2015, 1, 1, tzinfo=timezone.utc),
            end=dt.datetime(2018, 1, 1, tzinfo=timezone.utc),
            capital_base=1e8,
            initialize=initialize,
            handle_data=handle_data
        )
        perf.starting_value.plot(ax=ax,
                                 label='apl {:.2f}   msft: {:.2f}'.format(
                                     dist['AAPL'],
                                     dist['MSFT']))

    plt.legend()
    plt.show()
    plt.imsave('save.png', format('PNG'))
