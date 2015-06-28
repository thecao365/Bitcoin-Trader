__author__ = 'Antares'

#  to install python packages, use "python -m pip install XXX"

from abc import ABCMeta, abstractmethod
import http.client
from http.client import HTTPException
import urllib.request
import urllib.parse
import json
import hmac
import base64
import hashlib
import time
from decimal import Decimal
import logging
import sys
import requests
from requests import ConnectionError

from util import Quote


class Exchange(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_authenticated_data(self, **kwargs):
        """
        Return the raw data from the server.
        """
        pass

    @abstractmethod
    def get_unauthenticated_data(self, **kwargs):
        """
        Return the raw data from the server.
        """
        pass

    @abstractmethod
    def get_quote(self, retry=False):
        """
        Return a Quote instance.
        """
        pass

    @abstractmethod
    def get_balance(self):
        """
        Return a map contains new balance info.
        """
        pass

    @abstractmethod
    def place_market_order(self, **kwargs):
        """
        Place a market order. Return a map contains new balance info.
        """
        pass

    @abstractmethod
    def market_buy(self, **kwargs):
        """
        Place a market buy order. Return a map contains new balance info.
        """
        pass

    @abstractmethod
    def market_sell(self, **kwargs):
        """
        Place a market sell order. Return a map contains new balance info.
        """
        pass


class BTCe(Exchange):
    """
    the BTCe class is a communication module to the exchange BTCe (https://btc-e.com).

    returns of all the public methods are B{STANDARDIZED}, so that the code of the main
    system does not have to change when it's trading between different exchanges.
    """
    name = "BTCe"

    __host = "btc-e.com"
    __api_base = "/tapi"
    __public_api_base = "https://btc-e.com/api/3"

    def __init__(self, api_key, secret, symbol, fee_rate, master_name=None, wait_time=1):
        """

        @param api_key: API key
        @param secret: API secret
        @param symbol: trading symbol
        @param fee_rate: fee rate (0.01 for 1%)
        @param master_name: name of the master who created this instance. used to setup logger.
        @param wait_time: how long to wait before retrying when fail to get nonce from BTCe
        @return:
        """
        Exchange.__init__(self)

        self.__api_key = api_key
        self.__secret = secret

        self.symbol = symbol
        self.fee_rate = Decimal(fee_rate)

        self.__nonce = None

        # set up logger
        if master_name is not None:
            self.logger = logging.getLogger("{}.{}".format(master_name, self.name))
        else:
            # master name is not given (usually while developing)
            self.logger = logging.getLogger(self.name)
            self.logger.setLevel(logging.DEBUG)
            formatter = logging.Formatter("{asctime}: {name}: {levelname}: {message}", style="{")
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setFormatter(formatter)
            stream_handler.setLevel(logging.DEBUG)
            self.logger.addHandler(stream_handler)

        # initiate nonce
        while True:
            self.logger.debug("get nonce key from server")
            answer = self.get_authenticated_data(method=None, params={})
            if (answer is not None) and ("error" in answer):
                # get valid nonce from error message
                print(answer)
                error_info = answer["error"].split(";")[1].strip()
                error_dict = dict((k.strip(), v.strip()) for k, v in (p.split(":") for p in error_info.split(",")))
                self.__nonce = int(error_dict["on key"])
                self.logger.debug("set nonce key to {}".format(self.__nonce))
                break
            time.sleep(wait_time)  # retry after wait_time (seconds)

        # initialize self.most_recent_quote
            # BTCe B{DO NOT} have market orders. To mimic market order,
            # order price will be set based on most recent quote.
        self.logger.debug("initialize self.most_recent_quote")
        self.most_recent_quote = self.get_quote(retry=True)
        self.logger.debug("set most_recent_quote: {}".format(self.most_recent_quote))

    def get_authenticated_data(self, method, params, timeout=None):
        """
        Get authenticated information from the exchange. B{NO retry}.

        BTCe Trade API v1 U{https://btc-e.com/tapi/docs#main}

        @param method: name of the API method. (e.g. "getInfo")
        @param params: parameters for this API method {"key": <>}
        @return:
        """

        # get nonce
        if self.__nonce is not None:
            self.__nonce += 1
        nonce = self.__nonce

        # prepare components
        params.update({"method": method, "nonce": nonce})
        query = urllib.parse.urlencode(params)
        signature = hmac.new(key=self.__secret.encode(),
                             msg=query.encode(),
                             digestmod=hashlib.sha512).hexdigest()
        headers = {"key": self.__api_key,
                   "Sign": signature,
                   "Content-type": "application/x-www-form-urlencoded"}

        # connect exchange
        conn = None
        try:
            conn = http.client.HTTPSConnection(self.__host, timeout=timeout)
            conn.request("POST", self.__api_base, query, headers)
            with conn.getresponse() as response:
                return json.loads(response.read().decode())
        except OSError as e:
            self.logger.error("get_authenticated_data() failure. Query: {}. Msg: {}".format(params, e),
                              exc_info=False)
            return None
        finally:
            if conn is not None:
                conn.close()

    def get_unauthenticated_data(self, method, pair, timeout=None) -> dict:
        """
        Get public data from exchange. B{NO retry}.

        BTCe Public API v3 U{https://btc-e.com/api/3/docs}
        "All information is cached every 2 seconds, so there's no point in making more frequent requests."

        @param method: name of the API method.
        @param pair: trading symbol.
        @return:
        """
        try:
            with urllib.request.urlopen(self.__public_api_base + "/{}/{}".format(method, pair),
                                        timeout=timeout) as response:
                return json.loads(response.read().decode())
        except OSError as e:
            self.logger.error("get_unauthenticated_data() failure. URL: /{}/{}. Msg: {}".format(method, pair, e),
                              exc_info=False)
            return None

    def get_quote(self, retry=False, timeout=None, sleep=0.5) -> Quote:
        """
        Get quote from BTCe.

        Bid, ask and time stamp are stored in class decimal.Decimal.
        "All information is cached every 2 seconds, so there's no point in making more frequent requests."

        @param retry: if fail to get quote, retry or not?
        @param timeout:
        @param sleep: pause (seconds) before retry
        @return: Quote
        """
        while True:
            self.logger.debug("start getting quote")

            # get ticker from BTCe
            answer = self.get_unauthenticated_data("ticker", self.symbol, timeout=timeout)

            # validate answer
            if (answer is not None) and (self.symbol in answer):
                info = answer[self.symbol]
                quote = Quote(Decimal(str(info["sell"])),
                              Decimal(str(info["buy"])),
                              Decimal(str(time.time())),
                              self.name,
                              self.symbol)
                self.logger.debug("successfully got quote: {}".format(quote))
                return quote

            # invalid answer
            self.logger.info("get_quote() failed. answer={}".format(answer))
            if retry:
                time.sleep(sleep)
                continue  # try again
            else:
                return Quote()  # return an empty Quote

    def get_balance(self) -> dict:
        """
        get current account balance.

        this method will keep trying until a valid return has been received.

        @return: a dict contains {'currency': Decimal('amount'), ..., 'time_stamp': Decimal('time_stamp')}
        """
        answer = None
        while True:
            self.logger.debug("get balance from server")
            answer = self.get_authenticated_data("getInfo", {})
            self.logger.debug("receive balance answer: {}".format(answer))

            # validate answer
            if ("success" in answer) and (answer["success"] == 1):
                break

            # got invalid answer
            self.logger.warning("fail to get balance. answer received: {}".format(answer))

        # prepare and return new balance
        funds = answer["return"]["funds"]
        for key, value in funds.items():  # convert numbers to Decimal
            funds[key] = Decimal(str(value))
        funds["time_stamp"] = Decimal(str(answer["return"]["server_time"]))

        self.logger.debug("new balance(get_balance): {}".format(funds))
        return funds

    def place_market_order(self, params: dict) -> dict:
        """
        place a market order.

        this method will keep trying until a valid return has been received.

        @param params: order details
        @return: new balance of account
        """
        answer = None
        while True:
            # place the order
            answer = self.get_authenticated_data("Trade", params)
            self.logger.info("receive market order answer: {}".format(answer))

            # validate answer
            if ("success" in answer) and (answer["success"] == 1):
                break

            # got invalid answer
            self.logger.critical("INVALID answer for order: {}. Place order again".format(answer))

        # check if order was filled completely
        order_id = answer["return"]["order_id"]
        if order_id != 0:
            self.logger.critical("Order was NOT filled completely. order_id = {}".format(order_id))

        # prepare and return new balance
        funds = answer["return"]["funds"]
        for key, value in funds.items():  # convert numbers to Decimal
            funds[key] = Decimal(str(value))
        funds["time_stamp"] = Decimal(str(time.time()))

        self.logger.info("receive new balance(trade): {}".format(funds))
        return funds

    def market_buy(self, amount) -> dict:
        """
        place a market BUY order.

        1. BTCe B{DO NOT} have market orders. To mimic market order,
        order price will be set based on most recent quote.
        price can only has 3 decimal places.

        2. BTCe takes fee from the currency received in a transaction,
        so in order to buy the right amount of asset, amount must be adjusted to amount/(1-feeRate).
        amount can only has 8 decimal places

        @param amount: the amount of asset to be B{RECEIVED}
        @return new balance
        """
        params = {"pair": self.symbol,
                  "type": "buy",
                  "rate": "{:0.3f}".format(self.most_recent_quote.ask * Decimal("1.5")),  # see 1
                  "amount": "{:0.8f}".format(amount/(1-self.fee_rate))}  # see 2
        self.logger.info("place market BUY order w/ params: {}".format(params))
        return self.place_market_order(params)

    def market_sell(self, amount) -> dict:
        """
        place a market SELL order.

        1. BTCe B{DO NOT} have market orders. To mimic market order,
        order price will be set based on most recent quote.
        price can only has 3 decimal places.

        2. BTCe takes fee from the currency received in a transaction,
        so no need to adjust sell amount.
        amount can only has 8 decimal places

        @param amount: the amount of asset to be B{SOLD}
        @return new balance
        """
        params = {"pair": self.symbol,
                  "type": "sell",
                  "rate": "{:0.3f}".format(self.most_recent_quote.bid * Decimal("0.6")),  # see 1
                  "amount": "{:0.8f}".format(amount)}  # see 2
        self.logger.info("place market SELL order w/ params: {}".format(params))
        return self.place_market_order(params)


class Bitfinex(Exchange):
    """
    the Bitfinex class is a communication module to the exchange Bitfinex (https://www.bitfinex.com).

    returns of all the public methods are B{STANDARDIZED}, so that the code of the main
    system does not have to change when it's trading between different exchanges.
    """
    name = "Bitfinex"

    __host = "api.bitfinex.com"
    __api_base = "/v1"
    __public_api_base = "https://api.bitfinex.com/v1"

    def __init__(self, api_key, secret, symbol, fee_rate, master_name=None):
        """

        @param api_key: API key
        @param secret: API secret
        @param symbol: trading symbol
        @param fee_rate: fee_rate fee rate (0.01 for 1%)
        @param master_name: name of the master who created this instance. used to setup logger.
        @return:
        """
        Exchange.__init__(self)

        self.__api_key = api_key
        self.__secret = secret

        self.symbol = symbol
        self.fee_rate = Decimal(fee_rate)

        # set up logger
        if master_name is not None:
            self.logger = logging.getLogger("{}.{}".format(master_name, self.name))
        else:
            # master name is not given (usually while developing)
            self.logger = logging.getLogger(self.name)
            self.logger.setLevel(logging.DEBUG)
            formatter = logging.Formatter("{asctime}: {name}: {levelname}: {message}", style="{")
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setFormatter(formatter)
            stream_handler.setLevel(logging.DEBUG)
            self.logger.addHandler(stream_handler)

    def get_authenticated_data(self, url, request, timeout=None):
        """
        Get authenticated information from the exchange. B{NO retry}.

        Bitfinex API v1 U{https://www.bitfinex.com/pages/api}

        @param url: target url of API. (e.g. "/balances")
        @param request: request parameters for this API method {"key": <>}
        @return:
        """

        # prepare components
        request.update({"request": self.__api_base+url, "nonce": str(time.time())})
        payload = base64.b64encode(json.dumps(request).encode())
        signature = hmac.new(key=self.__secret.encode(),
                             msg=payload,
                             digestmod=hashlib.sha384).hexdigest()
        headers = {"X-BFX-APIKEY": self.__api_key,
                   "X-BFX-SIGNATURE": signature,
                   "X-BFX-PAYLOAD": payload}

        # connect exchange
        conn = None
        try:
            conn = http.client.HTTPSConnection(self.__host, timeout=timeout)
            conn.request("POST", self.__api_base+url, "", headers)
            with conn.getresponse() as response:
                return json.loads(response.read().decode())
        except OSError as e:
            self.logger.error("get_authenticated_data() failure. Query: {}. Msg: {}".format(request, e),
                              exc_info=False)
            return None
        finally:
            if conn is not None:
                conn.close()

    def get_unauthenticated_data(self, url, symbol, timeout=None):
        """
        Get public data from exchange. NO retry.

        Bitfinex API v1 U{https://www.bitfinex.com/pages/api}

        @param url: target url of public API method
        @param symbol: trading symbol
        @return:
        """
        try:
            with urllib.request.urlopen(self.__public_api_base + "{}/{}".format(url, symbol),
                                        timeout=timeout) as response:
                return json.loads(response.read().decode())
        except OSError as e:
            self.logger.error("get_unauthenticated_data() failure. URL: /{}/{}. Msg: {}".format(url, symbol, e),
                              exc_info=False)
            return None

    def get_quote(self, retry=False, timeout=None, sleep=0.5) -> Quote:
        """
        Get quote from Bitfinex.

        Bid, ask and time stamp are stored in class decimal.Decimal.

        @param retry: if fail to get quote, retry or not?
        @param sleep: pause (seconds) before retry
        @return: Quote
        """
        while True:
            self.logger.debug("start getting quote")

            # get ticker from Bitfinex
            answer = self.get_unauthenticated_data("/pubticker", self.symbol, timeout=timeout)

            # validate answer
            if (answer is not None) and ("bid" in answer) and ("ask" in answer) and ("timestamp" in answer):
                quote = Quote(Decimal(answer["bid"]),
                              Decimal(answer["ask"]),
                              Decimal(str(time.time())),
                              self.name,
                              self.symbol)
                self.logger.debug("successfully got quote: {}".format(quote))
                return quote

            # invalid answer
            self.logger.info("get_quote() failed. answer={}".format(answer))
            if retry:
                time.sleep(sleep)
                continue  # try again
            else:
                return Quote()  # return an empty Quote

    def get_balance(self, context="get_balance") -> dict:
        """
        get current account balance.

        this method will keep trying until a valid return has been received.

        1. Bitfinex does not provide time stamp in the answer,
        so the "time_stamp" in the returned dict is actually local time

        @return: a dict contains {'currency': Decimal('amount'), ..., 'time_stamp': Decimal('time_stamp')}
        """
        answer = None
        while True:
            self.logger.debug("get balance from server")
            answer = self.get_authenticated_data("/balances", {})
            self.logger.debug("receive balance answer: {}".format(answer))

            # validate answer
            if isinstance(answer, list) and ("available" in answer[0]):
                break

            # got invalid answer
            self.logger.warning("fail to get balance. answer received: {}".format(answer))

        # prepare and return new balance
        funds = {}
        for entry in list(answer):  # collect all balance associated with account type 'exchange'
            if entry["type"] == "exchange":
                funds[entry["currency"]] = Decimal(entry["available"])  # convert string to Decimal
        funds["time_stamp"] = Decimal(int(time.time()))  # see 1

        self.logger.debug("new balance({1}): {0}".format(funds, context))
        return funds

    def place_market_order(self, params: dict):
        """
        place a market order.

        this method will keep trying until a valid return has been received.
        (1) Bitfinex returns orderID and time stamp in the answer, instead of returning new balance.
        So this method check the order status explicitly and get new balance after order was fully filled.
        (2) Bitfinex offers the option to choose the fee currency.

        @param params: order details
        @return: new balance of account
        """
        answer = None
        while True:
            # place the order
            answer = self.get_authenticated_data("/order/new", params)
            self.logger.info("receive market order answer: {}".format(answer))

            # validate answer
            if "order_id" in answer:
                break

            # got invalid answer
            self.logger.critical("INVALID answer for order: {}. Place order again".format(answer))

        # check if order was filled completely
        order_id = answer["order_id"]
        order_status = answer
        while ("is_live" not in answer) or (order_status["is_live"] is True):
            # check again
            order_status = self.get_authenticated_data("/order/status", {"order_id": order_id})

        # order was filled and no longer live
        self.logger.info("market order was filled: {}".format(order_status))

        # prepare and return new balance
        return self.get_balance(context="place_market_order")

    def market_buy(self, amount):
        """
        place a market BUY order.

        @param amount:
        @return:
        """
        # set params
        params = {"symbol": self.symbol,
                  "amount": "{:f}".format(amount),  # Key amount should be a decimal string.
                  "price": "0.01",
                  "exchange": "bitfinex",
                  "side": "buy",
                  "type": "exchange market"}
        self.logger.info("place market BUY order w/ params: {}".format(params))
        return self.place_market_order(params)

    def market_sell(self, amount) -> dict:
        """
        place a market SELL order.

        @param amount:
        @return:
        """
        # set params
        params = {"symbol": self.symbol,
                  "amount": "{:f}".format(amount),  # Key amount should be a decimal string.
                  "price": "100000.00",
                  "exchange": "bitfinex",
                  "side": "sell",
                  "type": "exchange market"}
        self.logger.info("place market SELL order w/ params: {}".format(params))
        return self.place_market_order(params)


class BTCChina(Exchange):
    pass


class OKCoin(Exchange):
    pass


def main():
    # set up logging
    logger = logging.getLogger(__name__)

    btce = BTCe(api_key="",
                secret="",
                symbol="ltc_usd",
                fee_rate="0.002")

    bitfinex = Bitfinex(api_key="",
                        secret="",
                        symbol="ltcusd",
                        fee_rate="0.001")

    if 0:
        bal = bitfinex.get_balance()
        print("USD: {usd:f}; LTC: {ltc:f}".format(**bal))
        bal = bitfinex.market_buy(Decimal("0.1"))
        print("USD: {usd:f}; LTC: {ltc:f}".format(**bal))
        bal = bitfinex.market_sell(Decimal("0.1"))
        print("USD: {usd:f}; LTC: {ltc:f}".format(**bal))

    if 0:
        btce.get_balance()
        btce.market_buy(Decimal("0.1"))
        btce.market_sell(Decimal("0.1"))

    if 0:
        funds = btce.get_balance()
        for key, value in funds.items():  # convert numbers to Decimal
            print("{}: {} ({})".format(key, value, type(value)))

    if 0:
        while True:
            print(btce.get_quote())
            print(bitfinex.get_quote())
            time.sleep(1)

    if 0:
        start = time.time()
        for i in range(0, 10000):
            logger.info("test %s" % i)
        end = time.time()
        print(end-start)

    if 0:
        print(bitfinex.get_authenticated_data("/balances", {}))
        print(bitfinex.get_unauthenticated_data("/pubticker", "ltcusd"))

    if 0:
        print(btce.get_authenticated_data("getInfo", {}))
        print(btce.get_unauthenticated_data("ticker", "ltc_usd"))


if __name__ == "__main__":
    # main()
