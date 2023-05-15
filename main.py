from dataclasses import dataclass, field
from heapq import heappush, heappop
from uuid import uuid4
from datetime import datetime
import math


@dataclass(order=True, kw_only=True)
class Order:
    price: int
    ts: int = field(default_factory=datetime.now)
    qty: int
    acc: str
    _id: str = field(default_factory=uuid4)


    def __repr__(self):
      return f"{self.price, self.qty, self.acc}"


@dataclass(order=True, kw_only=True)
class Transaction:
    ts: int = field(default_factory=datetime.now)
    price: int
    qty: int
    x: str
    y: str

    def __repr__(self):
      return f"{self.price, self.qty, self.x, self.y}"


class Market:

    def __init__(self, algoritms):
        self.bids = []
        self.asks = []
        self.algoritms = algoritms

    def buy(self, acc, qty, price=None):
        if price is None:
            if not self.asks:
                return []
            price = self.asks[0].price
        
        order = Order(price=-price, qty=qty, acc=acc)

        return self.try_match(order, self.bids, self.asks)


    def sell(self, acc, qty, price=None):
        if price is None:
            if not self.bids:
                return []
            price = -self.bids[0].price

        order = Order(price=price, qty=qty, acc=acc)
        return self.try_match(order, self.asks, self.bids)


    def try_match(self, order, X, Y):
        print("try", order)
        print("before:\n", self)
        transactions = []
        # sell @ 9, buy [-10, -9, -8, -7]
        # buy @ 9, sell [8, 9, 10, 11]
        while Y and order.qty > 0 and -order.price >= Y[0].price:
            order = self.match(order, Y, transactions)

        if order.qty > 0:
            heappush(X, order)

        if transactions:
          print(transactions)
          print("after:\n", self)
        return transactions


    def match(self, order, Y, transactions):
        while order.qty > 0 and Y:
            for algorithm in self.algoritms:
                if not (order.qty > 0 and Y):
                  break
                algorithm(order, Y, transactions)
        return order


    def __repr__(self):
      return "bid:{}\nask:{}".format(self.bids, self.asks)


def fifo(x, Y, transactions):
    print("fifo", x)
    y = Y[0]
    price = abs(y.price)
    qty = min(x.qty, y.qty)
    y.qty -= qty
    x.qty -= qty

    txn = Transaction(
        price=price,
        qty=qty,
        x=x.acc,
        y=y.acc
    )

    transactions.append(txn)

    if y.qty <= 0:
        heappop(Y)

    return transactions


def prorata(x, Y, transactions):
    print("prorata", x)
    y = Y[0]
    total = 0
    orders = []
    price = y.price
    while Y and Y[0].price == price:
        order = heappop(Y)
        total += order.qty
        orders.append(order)

    distributed = 0
    price = abs(price)
    for y in orders:
        ratio = (y.qty * 1.0) / total 
        filled = min(math.floor(ratio * x.qty), y.qty)
        txn = Transaction(
            price=price,
            qty=filled,
            x=x.acc,
            y=y.acc
        )
        transactions.append(txn)
        y.qty -= filled
        distributed += filled
    x.qty -= distributed
    for order in orders:
      if order.qty <= 0:
        continue
      heappush(Y, order)
    return transactions


market = Market([prorata, fifo])

t = market.buy("alice", 10, 10)
t = market.buy("bob", 10, 10)
t = market.sell("charlie", 25, 10)
t = market.buy("dave", 200)
t = market.sell("eli", 195)