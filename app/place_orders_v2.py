# -*- coding: utf-8 -*-
"""
Created on Sun Sep  8 20:33:55 2019

@author: Nanda
"""

# Load Libraries
from ibapi.utils import iswrapper
from ibapi import wrapper
from ibapi import utils
from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract as IBcontract
from ibapi.order import *
import queue
import pandas as pd
import numpy as np
import os
import inspect

import re
import time
import requests
import pandas as pd
import io
import os
import inspect

# Set working directory
filename = inspect.getframeinfo(inspect.currentframe()).filename
os.chdir(os.path.dirname(os.path.abspath(filename)))

# Load TWS Classes
FINISHED = object()
STARTED = object()
TIME_OUT = object()


def log_execution(fn):
    def fun(self):
        print("   doing", fn.__name__)
        fn(self)
        print("   done w/", fn.__name__)

    return fun


class FinishableQueue(object):

    def __init__(self, queue_to_finish):

        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout):
        """
        Returns a list of queue elements once timeout is finished, or a FINISHED flag is received in the queue
        :param timeout: how long to wait before giving up
        :return: list of queue elements
        """
        contents_of_queue = []
        finished = False

        while not finished:
            try:
                current_element = self._queue.get(timeout=timeout)
                if current_element is FINISHED:
                    finished = True
                    self.status = FINISHED
                else:
                    contents_of_queue.append(current_element)
                    # keep going and try and get more data

            except queue.Empty:
                # If we hit a time out it's most probable we're not getting a finished element any time soon
                # give up and return what we have
                finished = True
                self.status = TIME_OUT

        return contents_of_queue

    def timed_out(self):
        return self.status is TIME_OUT


class TestWrapper(EWrapper):
    """
    The wrapper deals with the action coming back from the IB gateway or TWS instance
    We override methods in EWrapper that will get called when this action happens, like currentTime
    Extra methods are added as we need to store the results in this object
    """

    def __init__(self):
        self._my_contract_details = {}
        self._my_historic_data_dict = {}

    # error handling code
    def init_error(self):
        error_queue = queue.Queue()
        self._my_errors = error_queue

    def get_error(self, timeout=5):
        if self.is_error():
            try:
                return self._my_errors.get(timeout=timeout)
            except queue.Empty:
                return None

        return None

    def is_error(self):
        an_error_if=not self._my_errors.empty()
        return an_error_if

    def error(self, id, errorCode, errorString):
        # Overriden method
        errormsg = "IB error id %d errorcode %d string %s" % (id, errorCode, errorString)
        self._my_errors.put(errormsg)

    # get contract details code
    def init_contractdetails(self, reqId):
        contract_details_queue = self._my_contract_details[reqId] = queue.Queue()

        return contract_details_queue

    def contractDetails(self, reqId, contractDetails):
        # overridden method

        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(contractDetails)

    def contractDetailsEnd(self, reqId):
        # overriden method
        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(FINISHED)


DEFAULT_GET_CONTRACT_ID = 43


class TestClient(EClient):
    """
    The client method
    We don't override native methods, but instead call them from our own wrappers
    """
    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)

    def resolve_ib_contract(self, ibcontract, reqId=DEFAULT_GET_CONTRACT_ID):
        """
        From a partially formed contract, returns a fully fledged version
        :returns fully resolved IB contract
        """

        # Make a place to store the data we're going to return
        contract_details_queue = FinishableQueue(self.init_contractdetails(reqId))

        print("Getting full contract details from the server... ")

        self.reqContractDetails(reqId, ibcontract)

        # Run until we get a valid contract(s) or get bored waiting
        MAX_WAIT_SECONDS = 10
        new_contract_details = contract_details_queue.get(timeout=MAX_WAIT_SECONDS)

        while self.wrapper.is_error():
            print(self.get_error())

        if contract_details_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished - seems to be normal behaviour")

        if len(new_contract_details)==0:
            print("Failed to get additional contract details: returning unresolved contract")
            return ibcontract

        if len(new_contract_details)>1:
            print("got multiple contracts using first one")

        new_contract_details=new_contract_details[0]

        resolved_ibcontract=new_contract_details.contract

        return resolved_ibcontract


class TestApp(TestWrapper, TestClient):
    def __init__(self):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)

        #thread = Thread(target = self.run)
        #thread.start()

        #setattr(self, "_thread", thread)
        
        self.nextValidOrderId = None

        self.init_error()
        
    def error(self, reqId, errorCode, errorString):
        print('Error: ', reqId, ' ', errorCode, ' ', errorString)
        
    @iswrapper
    # ! [nextvalidid]
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        self.nextValidOrderId = orderId
        print('nextValidId: ', orderId)
        
        # we can start now
        self.start()
        #self.stop()
    # ! [nextvalidid]
        
    def nextoid(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid
        
    def defineContract(self, symbol, secType, exchange, currency, primaryExchange):
        
        contract = IBcontract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        contract.primaryExchange = primaryExchange
        
        return contract
    
    def makeOrder(self, nextOrderId, contract, action, quantity, price=None):
        
        order = Order()
        order.action = action
        order.totalQuantity = quantity
        
        if price is not None:
            order.orderType = 'LMT'
            order.tif = 'GTC'
            order.lmtPrice = price
        else:
            order.orderType = 'MKT'
            order.tif = 'GTC'
        
        self.placeOrder(nextOrderId, contract, order)
        
    def makeStpOrder(self, nextOrderId, contract, action, quantity, stopPrice):
        
        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = 'STP'
        order.tif = "GTC"
        order.auxPrice = stopPrice
        
        self.placeOrder(nextOrderId, contract, order)
        
    def TrailingStop(self, nextOrderId, contract, action:str, quantity:float, 
                     trailingPercent:float, trailStopPrice:float):
    
        # ! [trailingstop]
        order = Order()
        order.action = action
        order.orderType = "TRAIL"
        order.totalQuantity = quantity
        order.trailingPercent = trailingPercent
        order.trailStopPrice = trailStopPrice
        # ! [trailingstop]
        self.placeOrder(nextOrderId, contract, order)
        
    def BracketOrder(self, parentOrderId:int, action:str, quantity:float,
                     parentOrderPrice:float, takeProfitLimitPrice:float,
                     stopLossPrice:float, contract, ordertype, trailingPercent=0, trailAmount=0):

        # This will be our main or "parent" order
        parent = Order()
        parent.orderId = parentOrderId
        parent.action = action
        parent.tif = "GTC"
        parent.totalQuantity = quantity
        if ordertype == 'TRAIL':
            parent.orderType = "TRAIL"
            parent.trailingPercent = trailingPercent
            parent.trailStopPrice = parentOrderPrice
        else:
            parent.orderType = "STP LMT"
            parent.auxPrice = parentOrderPrice
            if action == 'BUY':
                parent.lmtPrice = round(parentOrderPrice + trailAmount, 1)
            elif action == 'SELL':
                parent.lmtPrice = round(parentOrderPrice - trailAmount, 1)
        # The parent and children orders will need this attribute set to False to prevent accidental executions.
        # The LAST CHILD will have it set to True,
        parent.transmit = False
        
        takeProfit = Order()
        takeProfit.orderId = parent.orderId + 1
        takeProfit.action = "SELL" if action == "BUY" else "BUY"
        takeProfit.orderType = "LMT"
        takeProfit.tif = "GTC"
        takeProfit.totalQuantity = quantity
        takeProfit.lmtPrice = takeProfitLimitPrice
        takeProfit.parentId = parentOrderId
        takeProfit.transmit = False

        stopLoss = Order()
        stopLoss.orderId = parent.orderId + 2
        stopLoss.action = "SELL" if action == "BUY" else "BUY"
        stopLoss.orderType = "STP"
        stopLoss.tif = "GTC"
        # Stop trigger price
        stopLoss.auxPrice = stopLossPrice
        stopLoss.totalQuantity = quantity
        stopLoss.parentId = parentOrderId

        # In this case, the low side order will be the last child being sent.
        # Therefore, it needs to set this attribute to True
        # to activate all its predecessors
        stopLoss.transmit = True

        bracketOrder = [parent, takeProfit, stopLoss]
        
        for o in bracketOrder:
            self.placeOrder(o.orderId, contract, o)

    def get_orders(self):
        from trade import get_project_file_path
        excel_path = get_project_file_path('Orders.xlsx')
        order_df = pd.read_excel(excel_path, sheet_name='Orders')
        return order_df, order_df['order_type'].iloc[0]

    def start(self):
        orders_df, ordertype = self.get_orders()
        self.place_orders(orders_df, ordertype)
        
    def stop(self):
        self.done = True
        self.disconnect()
        
    def place_orders(self, orders_df, ordertype):
        # Requesting the next valid id
        # ! [reqids]
        # The parameter is always ignored.
        self.reqIds(-1)
        
        # Place orders
        for i in range(len(orders_df)):
            symbol = orders_df['ticker'].iloc[i].upper()
            try:
                signal = orders_df['signal'].iloc[i].upper()
                quantity = int(round(orders_df['quantity'].iloc[i], 0))
                SLPrice = orders_df['stop_loss'].iloc[i]
                TPPrice = orders_df['take_profit'].iloc[i]
                
                contract = self.defineContract(symbol, 'STK', 'SMART', 'USD', 'NYSE')
                
                if ordertype == 'TRAIL':
                    trailStopPrice = orders_df['stop_price'].iloc[i]
                    trailingPercent = round(orders_df['trail_amount'].iloc[i] / trailStopPrice, 4)*100
                    self.BracketOrder(self.nextoid(), signal, quantity, trailStopPrice,
                                      TPPrice, SLPrice, contract, ordertype, trailingPercent)
                    self.nextValidOrderId += 2
                else:
                    StopPrice = orders_df['stop_price'].iloc[i]
                    lmtAmount = orders_df['trail_amount'].iloc[i]
                    self.BracketOrder(self.nextoid(), signal, quantity, StopPrice,
                                      TPPrice, SLPrice, contract, ordertype, trailAmount = lmtAmount)
                    self.nextValidOrderId += 2
                    
                time.sleep(1)
                
            except:
                print('Order placing failed for ticker: ' + symbol)
                pass
            
        self.disconnect()

def run_app():
            
    app = TestApp()
    cid = 0
    app.connect('127.0.0.1', 7497, cid)
    
    #app.place_orders()
    
    #nextOrderId = app.nextOrderId()
    #print('nextOrderId', nextOrderId)
    
    app.run()
    
run_app()