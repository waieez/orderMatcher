from dataclasses import dataclass, field
from datetime import datetime
from uuid import uuid4
from heapq import heappush, heappop
from typing import List, Callable


@dataclass(order=True, kw_only=True)
class Order:
    price: int
    ts: int = field(default_factory=datetime.now)
    qty: int
    acc: str
    _id: str = field(default_factory=uuid4)


@dataclass(order=True, kw_only=True)
class Transaction:
    price: int
    qty: int
    x: str
    y: str
    ts: int = field(default_factory=datetime.now)
    _id: str = field(default_factory=uuid4)


class Market:

    def __init__(self, algorithms: List[Callable]):
        self.bids = []
        self.asks = []

        self.algorithms = algorithms

    
    def buy(self, acc: str, qty: int, price: int = None) -> List[Transaction]:
        if not price:
            if self.asks:
                price = self.asks[0].price

        if qty <= 0 or not price:
            return []

        order = Order(price=-price, qty=qty, acc=acc)
        return self.try_match(order, self.bids, self.asks)

    
    def sell(self, acc: str, qty: int, price: int = None) -> List[Transaction]:
        if not price:
            if self.bids:
                price = -self.bids[0].price

        if qty <= 0 or not price:
            return []

        order = Order(price=price, qty=qty, acc=acc)
        return self.try_match(order, self.asks, self.bids)


    def try_match(self, x: Order, X: List[Order], Y: List[Order]):
        transactions = []
        while x.qty > 0 and Y and -x.price >= Y[0].price:
            self.match(x, Y, transactions)
        
        if x.qty > 0:
            heappush(X, x)

        return transactions
    

    def match(self, x: Order, Y: List[Order], transactions: List[Transaction]):
        price = Y[0].price

        for algorithm in self.algorithms:
            if x.qty <= 0 or not Y or Y[0].price != price:
                break
            algorithm(x, Y, transactions)

        return transactions


def best(Y: List[Order]) -> List[Order]:
    orders = []
    price = Y[0].price
    while Y and Y[0].price == price:
        y = heappop(Y)
        orders.append(y)
    return orders


def insert(orders, X: List[Order]) -> List[Order]:
    for x in orders:
        if x.qty <= 0:
            continue
        heappush(X, x)
    return X


def fifo(x: Order, Y: List[Order], transactions: List[Transaction]) -> List[Transaction]:
    y = Y[0]
    price = abs(y.price)
    qty = min(x.qty, y.qty)
    x.qty -= qty
    y.qty -= qty
    t = Transaction(price=price, qty=qty, x=x.acc, y=y.acc)
    transactions.append(t)

    if not y.qty:
        heappop(Y)

    return transactions


def prorata(x: Order, Y: List[Order], transactions: List[Transaction]) -> List[Transaction]:
    orders = best(Y)
    total = 0
    for y in orders:
        total += y.qty
    
    filled = 0
    price = abs(orders[0].price)
    for y in orders:
        ratio = 1.0 * y.qty / total
        qty = min(y.qty, int(ratio * x.qty))
        filled += qty
        y.qty -= qty
        t = Transaction(price=price, qty=qty, x=x.acc, y=y.acc)
        transactions.append(t)
    
    x.qty -= filled
    insert(orders, Y)
    return transactions
  

market = Market([prorata, fifo])

t = market.buy("alice", 10, 10)
print(t)
t = market.buy("bob", 10, 10)
print(t)
t = market.buy("charlie", 10, 12)
print(t)
t = market.sell("dave", 25, 10)
print(t)