from dataclasses import dataclass, field
from datetime import datetime
from uuid import uuid4
from heapq import heappush, heappop
from collections import defaultdict

# need to know the size of the market.
#
# price & qty @ price
# placing a limit/market order for buy/sell
    

@dataclass(order=True, kw_only=True)
class Order:
    price: int
    ts: int = field(default_factory=datetime.now)
    qty: int
    _id: str = field(default_factory=uuid4)
    acc: str


@dataclass
class Transaction:
    price: int
    qty: int
    _from: str
    _to: str
    _id: str = field(default_factory=uuid4)
    ts: int = field(default_factory=datetime.now) 

class Market:

    def __init__(self, algorithms):
        self.bids = []
        self.asks = []
        self.algorithms = algorithms


    def __repr__(self):
        bids = defaultdict(int)
        asks = defaultdict(int)

        for b in self.bids:
            bids[b.price] += b.qty
        
        for a in self.asks:
            asks[a.price] += a.qty

      
        
        bids = [(k, v) for k, v in bids.items()]
        asks = [(k, v) for k, v in asks.items()]
        
        bids.sort()
        asks.sort(reverse=True)
      
        bids = [f"{k}: {v}" for k, v in bids]
        asks = [f"{k}: {v}" for k, v in asks]

        return """###\nasks:\n{}\nbids:\n{}\n###""".format("\n".join(asks), "\n".join(bids))

    def buy(self, acc, qty, price=None):
        if price is None:
            if not self.asks:
                # can't market buy w/ no market
                return
            price = self.asks[0].price

        if price <= 0 or qty <= 0:
            return []

        order = Order(price=-price, qty=qty, acc=acc)
        print("buy:", order.price, order.qty)
        return self.try_match(order, self.bids, self.asks)

    def sell(self, acc, qty, price=None):
        if price is None:
            if not self.bids:
                # can't market buy w/ no market
                return
            price = -self.bids[0].price

        if price <= 0 or qty <= 0:
            return []

        order = Order(price=price, qty=qty, acc=acc)
        print("sell:", order.price, order.qty)
        return self.try_match(order, self.asks, self.bids)

    def try_match(self, order, A, B):
        if A and order.price > A[0].price:
            heappush(A, order)
            return []

        transactions = [] 

        # bids -price, asks price
        # buy @ -3, sell [2,3,4,5]
        # sell @ 3, buy [-5,-4,-3,-2]
        while B and order.qty > 0 and order.price <= -B[0].price:
            self.match(order, B, transactions)
        
        if order.qty:
            heappush(A, order)

        if transactions:
          print("\nTransactions\n", transactions, "\n")
        return transactions
    
    def match(self, order, B, transactions):
        print("match orders")
        for algorithm in self.algorithms:
            if not order.qty:
                break
            algorithm(order, B, transactions)


def fifo(order, B, transactions):
    print("FIFO")
    b = heappop(B)
    qty = min(order.qty, b.qty)
    price = min(abs(order.price), abs(b.price))
    order.qty -= qty
    b.qty -= qty

    if b.qty > 0:
        heappush(B, b)

    txn = Transaction(price, qty, order._id, b._id)
    transactions.append(txn)


def prorata(order, B, transactions, minimum=0):
    print("PRORATA")
    orders = []
    b_price = B[0].price
    price = max(abs(order.price), abs(b_price))

    total = 0

    while B and B[0].price == b_price:
        b = heappop(B)
        total += b.qty
        orders.append(b)
    matched = 0

    for b in orders:
        ratio = (b.qty * 1.0) / total
        qty = ratio * order.qty
        # print("RATIO", ratio, qty)
        if qty < minimum:
            continue
        matched += qty
        txn = Transaction(price, qty, order._id, b._id)
        transactions.append(txn)
        b.qty -= qty
        if b.qty > 0:
            heappush(B, b)

    order.qty -= matched


# fifoMarket = Market([fifo])

# print(fifoMarket)

# fifoMarket.buy("alice", 10, 10)
# fifoMarket.buy("alice", 10, 11)

# fifoMarket.sell("bob", 10, 12)
# fifoMarket.sell("bob", 10, 13)

# print(fifoMarket)

# fifoMarket.buy("charlie", 15, 13)

# print(fifoMarket)


prorataMarket = Market([prorata])

print(prorataMarket)

prorataMarket.sell("alice", 10, 13)
prorataMarket.sell("bob", 10, 13)

print(prorataMarket)

prorataMarket.buy("charlie", 10, 13)

print(prorataMarket)

# print(prorataMarket.bids)