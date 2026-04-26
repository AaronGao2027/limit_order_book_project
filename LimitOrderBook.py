from sortedcontainers import SortedDict


class LimitOrderBook:
    exchange_mapping = {
        2: 'Nasdaq (XNAS)',
        3: 'Nasdaq Texas (XBOS)',
        4: 'Nasdaq PSX (XPSX)',
        5: 'Cboe BZX (BATS)',
        6: 'Cboe BYX (BATY)',
        7: 'Cboe EDGA (EDGA)',
        8: 'Cboe EDGX (EDGX)',
        9: 'NYSE (XNYS)',
        11: 'NYSE American (XASE)',
        12: 'NYSE Texas (XCHI)',
        15: 'MEMX MEMOIR (MEMX)',
        16: 'MIAX Pearl (EPRL)',
        43: 'NYSE Arca (ARCX)'
    }
    publisher_id_list = list(exchange_mapping.keys())

    def __init__(self):
        # SortedDict keeps price levels in ascending order:
        #   best bid  = bids[pid].keys()[-1]  (O(1) max)
        #   best ask  = asks[pid].keys()[0]   (O(1) min)
        # Each price level maps (publisher_id, order_id) -> size for O(1) cancel/modify.
        self.bids = {pid: SortedDict() for pid in self.publisher_id_list}
        self.asks = {pid: SortedDict() for pid in self.publisher_id_list}
        # (publisher_id, order_id) -> (side, price)  — tuple avoids per-order string alloc
        self.order_tracker = {}
        self.order_processed_map = {pid: 0 for pid in self.publisher_id_list}
        self.total_orders_processed = 0
        self.last_update_time = -1

    def apply_order(self, order):
        key = (order['publisher_id'], order['order_id'])
        self.order_processed_map[order['publisher_id']] += 1
        self.total_orders_processed += 1
        self.last_update_time = order['ts_recv']

        action = order['action']
        if action == 'A':
            self._add(order, key)
        elif action == 'M':
            self._modify(order, key)
        elif action == 'C':
            self._cancel(order, key)
        elif action == 'R':
            self._clear(order)

    def get_best_bid(self):
        national_best_bid_price = 0
        national_best_bid_size = 0

        for pid in self.publisher_id_list:
            bids = self.bids[pid]
            if bids:
                best = bids.keys()[-1]  # O(1) — SortedDict last key
                if best > national_best_bid_price:
                    national_best_bid_price = best

        if national_best_bid_price > 0:
            for pid in self.publisher_id_list:
                bids = self.bids[pid]
                if bids and bids.keys()[-1] == national_best_bid_price:
                    national_best_bid_size += sum(bids[national_best_bid_price].values())

        return national_best_bid_price, national_best_bid_size

    def get_best_ask(self):
        national_best_ask_price = 10 ** 15
        national_best_ask_size = 0

        for pid in self.publisher_id_list:
            asks = self.asks[pid]
            if asks:
                best = asks.keys()[0]  # O(1) — SortedDict first key
                if best < national_best_ask_price:
                    national_best_ask_price = best

        if national_best_ask_price < 10 ** 15:
            for pid in self.publisher_id_list:
                asks = self.asks[pid]
                if asks and asks.keys()[0] == national_best_ask_price:
                    national_best_ask_size += sum(asks[national_best_ask_price].values())

        return national_best_ask_price, national_best_ask_size

    def _add(self, order, key):
        if key not in self.order_tracker and order['side'] in ('A', 'B') and order['price'] > 0:
            side = order['side']
            price = order['price']
            self.order_tracker[key] = (side, price)
            book = self.asks[order['publisher_id']] if side == 'A' else self.bids[order['publisher_id']]
            if price not in book:
                book[price] = {}
            book[price][key] = order['size']

    def _modify(self, order, key):
        if key not in self.order_tracker:
            if order['size'] > 0 and order['side'] in ('A', 'B') and order['price'] > 0:
                side = order['side']
                price = order['price']
                self.order_tracker[key] = (side, price)
                book = self.asks[order['publisher_id']] if side == 'A' else self.bids[order['publisher_id']]
                if price not in book:
                    book[price] = {}
                book[price][key] = order['size']
        else:
            old_side, old_price = self.order_tracker[key]
            pub_id = order['publisher_id']
            book = self.asks[pub_id] if old_side == 'A' else self.bids[pub_id]
            new_price = order['price'] if order['price'] > 0 else old_price

            if order['size'] == 0:
                if old_price in book:
                    book[old_price].pop(key, None)
                    if not book[old_price]:
                        del book[old_price]
                del self.order_tracker[key]

            elif old_price != new_price:
                if old_price in book:
                    book[old_price].pop(key, None)
                    if not book[old_price]:
                        del book[old_price]
                if new_price not in book:
                    book[new_price] = {}
                book[new_price][key] = order['size']
                self.order_tracker[key] = (old_side, new_price)

            else:
                if old_price in book and key in book[old_price]:
                    book[old_price][key] = order['size']

    def _cancel(self, order, key):
        if key in self.order_tracker:
            old_side, old_price = self.order_tracker[key]
            pub_id = order['publisher_id']
            book = self.asks[pub_id] if old_side == 'A' else self.bids[pub_id]

            if old_price in book and key in book[old_price]:
                current_size = book[old_price][key]
                if order['size'] > 0 and order['size'] < current_size:
                    book[old_price][key] -= order['size']
                else:
                    del book[old_price][key]
                    if not book[old_price]:
                        del book[old_price]
                    del self.order_tracker[key]

    def _clear(self, order):
        pub_id = order['publisher_id']
        self.bids[pub_id].clear()
        self.asks[pub_id].clear()
        self.order_tracker = {k: v for k, v in self.order_tracker.items() if k[0] != pub_id}
