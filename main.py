from dataclasses import dataclass, field
from datetime import datetime
from uuid import uuid4
from heapq import heappush, heappop


@dataclass(order=True, kw_only=True)
class Order:
    price: int
    ts: int = field(default_factory=datetime.now)
    qty: int
    acc: str
    _id: str = field(default_factory=uuid4)

    def __repr__(self):
        return f"({self.price, self.qty, self.acc})"


@dataclass(order=True, kw_only=True)
class Transaction:
    ts: int = field(default_factory=datetime.now)
    price: int
    qty: int
    x: str
    y: str
    _id: str = field(default_factory=uuid4)

    def __repr__(self):
        return f"({self.price}, {self.qty}, {self.x}, {self.y})"


class Market:

    def __init__(self, algoritms):
        self.bids = []
        self.asks = []
        self.algoritms = algoritms

  
    def __repr__(self):
        return f"\nbids:\n{self.bids}\nasks:\n{self.asks}\n"

  
    def buy(self, acc, qty, price=None): 
        if price is None:
            if self.asks:
                price = self.asks[0].price
            elif self.bids:
                price = -self.bids[0].price

        if qty <= 0:
            return []

        order = Order(price=-price, qty=qty, acc=acc)
        return self.try_match(order, self.bids, self.asks)


    def sell(self, acc, qty, price=None):
        if price is None:
            if self.bids:
                price = -self.bids[0].price
            elif self.bids:
                price = self.asks[0].price

        if qty <= 0:
           return []

        order = Order(price=price, qty=qty, acc=acc)
        return self.try_match(order, self.asks, self.bids)

  
    def try_match(self, x, X, Y):
        transactions = []
        while Y and x.qty > 0 and -x.price >= Y[0].price:
            self.match(x, Y, transactions)

        if x.qty > 0:
            heappush(X, x)
        
        return transactions


    def match(self, x, Y, transactions):
        price = Y[0].price
        
        for algorithm in self.algoritms:
            # break if order filled @ level
            if x.qty <= 0 or not Y or Y[0].price != price:
                break
            algorithm(x, Y, transactions)

        return transactions


def fifo(x, Y, transactions):
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
    orders = []
    price = Y[0].price
    total = 0

    while Y and Y[0].price == price:
        y = heappop(Y)
        orders.append(y)
        total += y.qty

    filled = 0
    price = abs(price)
    for y in orders:
        ratio = (y.qty * 1.0) / total
        qty = int(min(x.qty * ratio, y.qty))
        y.qty -= qty
        filled += qty

        txn = Transaction(
            price=price,
            qty=qty,
            x=x.acc,
            y=y.acc
        )
        transactions.append(txn)
        
        if y.qty > 0:
            heappush(Y, y)

    x.qty -= filled
    return transactions


market = Market([prorata, fifo])

t = market.buy("alice", 10, 10)
print(t)
t = market.buy("bob", 10, 10)
print(t)
t = market.buy("charlie", 10, 15)
print(t)
t = market.sell("dave", 25, 10)
print(t)
t = market.sell("eli", 10)
print(t)
print(market)