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
from threading import Thread
import queue
import pandas as pd
import numpy as np
from threading import Timer
import random
import os
import inspect
import requests
import io
import time
import schedule

# Set working directory
filename = inspect.getframeinfo(inspect.currentframe()).filename
os.chdir(os.path.dirname(os.path.abspath(filename)))

# Load TWS Classes
FINISHED = object()
STARTED = object()
TIME_OUT = object()

def printWhenExecuting(fn):
    def fn2(self):
        print("   doing", fn.__name__)
        fn(self)
        print("   done w/", fn.__name__)

    return fn2

class finishableQueue(object):

    def __init__(self, queue_to_finish):

        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout):
        """
        Returns a list of queue elements once timeout is finished, or a FINISHED flag is received in the queue
        :param timeout: how long to wait before giving up
        :return: list of queue elements
        """
        contents_of_queue=[]
        finished=False

        while not finished:
            try:
                current_element = self._queue.get(timeout=timeout)
                if current_element is FINISHED:
                    finished = True
                    self.status = FINISHED
                else:
                    contents_of_queue.append(current_element)
                    ## keep going and try and get more data

            except queue.Empty:
                ## If we hit a time out it's most probable we're not getting a finished element any time soon
                ## give up and return what we have
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

    ## error handling code
    def init_error(self):
        error_queue=queue.Queue()
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
        ## Overriden method
        errormsg = "IB error id %d errorcode %d string %s" % (id, errorCode, errorString)
        self._my_errors.put(errormsg)

    ## get contract details code
    def init_contractdetails(self, reqId):
        contract_details_queue = self._my_contract_details[reqId] = queue.Queue()

        return contract_details_queue

    def contractDetails(self, reqId, contractDetails):
        ## overridden method

        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(contractDetails)

    def contractDetailsEnd(self, reqId):
        ## overriden method
        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(FINISHED)

    ## Historic data code
    def init_historicprices(self, tickerid):
        historic_data_queue = self._my_historic_data_dict[tickerid] = queue.Queue()

        return historic_data_queue

    def historicalData(self, tickerid , bar):

        ## Overriden method
        ## Note I'm choosing to ignore barCount, WAP and hasGaps but you could use them if you like
        bardata=(bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume)

        historic_data_dict=self._my_historic_data_dict

        ## Add on to the current data
        if tickerid not in historic_data_dict.keys():
            self.init_historicprices(tickerid)

        historic_data_dict[tickerid].put(bardata)

    def historicalDataEnd(self, tickerid, start:str, end:str):
        ## overriden method

        if tickerid not in self._my_historic_data_dict.keys():
            self.init_historicprices(tickerid)

        self._my_historic_data_dict[tickerid].put(FINISHED)

DEFAULT_GET_CONTRACT_ID=43
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

        ## Make a place to store the data we're going to return
        contract_details_queue = finishableQueue(self.init_contractdetails(reqId))

        print("Getting full contract details from the server... ")

        self.reqContractDetails(reqId, ibcontract)

        ## Run until we get a valid contract(s) or get bored waiting
        MAX_WAIT_SECONDS = 10
        new_contract_details = contract_details_queue.get(timeout = MAX_WAIT_SECONDS)

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
        
    def BracketOrder(self, parentOrderId:int, action:str, quantity:float,
                     limitPrice:float, 
                     stopLossPrice:float, contract):
    
        #This will be our main or "parent" order
        parent = Order()
        parent.orderId = parentOrderId
        parent.action = action
        parent.orderType = "LMT"
        parent.tif = "GTC"
        parent.totalQuantity = quantity
        parent.lmtPrice = limitPrice
        #The parent and children orders will need this attribute set to False to prevent accidental executions.
        #The LAST CHILD will have it set to True, 
        parent.transmit = False

#        takeProfit = Order()
#        takeProfit.orderId = parent.orderId + 1
#        takeProfit.action = "SELL" if action == "BUY" else "BUY"
#        takeProfit.orderType = "LMT"
#        takeProfit.tif = "GTC"
#        takeProfit.totalQuantity = quantity
#        takeProfit.lmtPrice = takeProfitLimitPrice
#        takeProfit.parentId = parentOrderId
#        takeProfit.transmit = False

        stopLoss = Order()
        stopLoss.orderId = parent.orderId + 1
        stopLoss.action = "SELL" if action == "BUY" else "BUY"
        stopLoss.orderType = "STP"
        stopLoss.tif = "GTC"
        #Stop trigger price
        stopLoss.auxPrice = stopLossPrice
        stopLoss.totalQuantity = quantity
        stopLoss.parentId = parentOrderId
        #In this case, the low side order will be the last child being sent. Therefore, it needs to set this attribute to True 
        #to activate all its predecessors
        stopLoss.transmit = True

        bracketOrder = [parent, stopLoss]
        
        for o in bracketOrder:
            self.placeOrder(o.orderId, contract, o)
                  
    def start(self):
        self.place_orders()
        
    def stop(self):
        self.done = True
        self.disconnect()
        
    def place_orders(self):
        # Requesting the next valid id
        # ! [reqids]
        # The parameter is always ignored.
        self.reqIds(-1)
        
        select_symbols = pd.read_csv('stocks_selected.csv')
        trade_symbols = select_symbols
        
        # Place orders
        ORDER_AMT = 7000
        ENTER_PRICE_THRESHOLD = 0.2
        STP_LOSS_THRESHOLD = 1.5
        nextOrderId = 1
        for i in range(len(trade_symbols)):
            symbol = trade_symbols['ticker'].iloc[i].upper()
            try:
                signal = trade_symbols['signal'].iloc[i].upper()
                if signal == 'BUY':
                    price = round(trade_symbols['price'].iloc[i] + trade_symbols['ATR'].iloc[i]*ENTER_PRICE_THRESHOLD,2)
                elif signal == 'SELL':
                    price = round(trade_symbols['price'].iloc[i] - trade_symbols['ATR'].iloc[i]*ENTER_PRICE_THRESHOLD,2)
                    
                contract = self.defineContract(symbol, 'STK', 'SMART', 'USD', 'NYSE')
                quantity = int(round(ORDER_AMT/price,0))
                
                if signal == 'BUY':
                    stop_price = round(-trade_symbols['ATR'].iloc[i]*STP_LOSS_THRESHOLD + price,2)
                elif signal == 'SELL':
                    stop_price = round(trade_symbols['ATR'].iloc[i]*STP_LOSS_THRESHOLD + price,2)
           
                nextOrderId = random.randint(nextOrderId,99*(i+1))
                print('Symbol: ', symbol)
                print('OrderId: ', nextOrderId)
                
                self.BracketOrder(self.nextoid(), signal, quantity, price, stop_price, contract)
                self.nextValidOrderId += 1
                
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
         
schedule.every().monday.at("20:38").do(run_app)
schedule.every().tuesday.at("19:50").do(run_app)
schedule.every().wednesday.at("22:00").do(run_app)
schedule.every().thursday.at("22:00").do(run_app)
schedule.every().friday.at("22:00").do(run_app)

while True:
    schedule.run_pending()
    time.sleep(1)