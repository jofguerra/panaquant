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
from ibapi.contract import Contract
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
        self.positions_df = pd.DataFrame()
        self.cancel_cnt = 0

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
        
        contract = Contract()
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
                  
    def start(self):
        self.close_orders()
        
    def stop(self):
        self.done = True
        self.disconnect()
        
    def close_orders(self):
        # Cancel all orders for all accounts
        # ! [reqglobalcancel]
        self.reqGlobalCancel()
        # ! [reqglobalcancel]
        
        # Subscribing to an account's information. Only one at a time!
        # ! [reqaaccountupdates]
        self.reqAccountUpdates(True, 0)
        # ! [reqaaccountupdates]
        
        #self.place_orders()
        
        #self.disconnect()
        
    @iswrapper
    # ! [updateportfolio]
    def updatePortfolio(self, contract: Contract, position: float,
                        marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float,
                        realizedPNL: float, accountName: str):
        super().updatePortfolio(contract, position, marketPrice, marketValue,
                                averageCost, unrealizedPNL, realizedPNL, accountName)
        
        print("UpdatePortfolio.", "Symbol:", contract.symbol, "SecType:", contract.secType, "Exchange:",
              contract.exchange, "Position:", position, "MarketPrice:", marketPrice,
              "MarketValue:", marketValue, "AverageCost:", averageCost,
              "UnrealizedPNL:", unrealizedPNL, "RealizedPNL:", realizedPNL,
              "AccountName:", accountName)
        position_series = pd.Series({"Symbol": contract.symbol, "SecType": contract.secType, 
                                     "Exchange": contract.exchange, "Position": position, 
                                     "MarketPrice": marketPrice, "MarketValue": marketValue, 
                                     "AverageCost": averageCost, "UnrealizedPNL": unrealizedPNL, 
                                     "RealizedPNL": realizedPNL, "AccountName": accountName})
    
        self.positions_df = self.positions_df.append(position_series, ignore_index=True).reset_index(drop=True)
    
        if self.cancel_cnt < len(self.positions_df):
            
            pos_symbol = contract.symbol
            pos_contract = self.defineContract(pos_symbol, 'STK', 'SMART', 'USD', 'NYSE')
            action = "BUY" if position < 0 else "SELL"
            quantity = abs(position)
            self.makeOrder(self.nextoid(), pos_contract, action, quantity)
            self.cancel_cnt += 1
    
    # ! [updateportfolio]
    

def run_app():
            
    app = TestApp()
    cid = 0
    app.connect('127.0.0.1', 7497, cid)
    
    app.run()
         
schedule.every().monday.at("13:00").do(run_app)
schedule.every().tuesday.at("13:24").do(run_app)
schedule.every().wednesday.at("13:00").do(run_app)
schedule.every().thursday.at("13:00").do(run_app)
schedule.every().friday.at("13:00").do(run_app)

while True:
    schedule.run_pending()
    time.sleep(1)