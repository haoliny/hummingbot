import os
from datetime import datetime
from typing import Dict, List

import psycopg2
from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel, ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import OrderBookEvent, OrderBookTradeEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class ArchiveConfig(BaseClientModel):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    exchanges: List[str] = Field(["kucoin_paper_trade", "binance_paper_trade"], client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the exchanges to archive:"))
    symbols: List[str] = Field(["BTC-USDT", "ETH-USDT"], client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the trading pairs to archive:"))
    depth: int = Field(10, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the depth of the order book to archive:"))


class Archive(ScriptStrategyBase):
    order_book_temp_storage = {}
    trades_temp_storage = {}
    subscribed_to_order_book_trade_event: bool = False
    connection = None
    last_dump_timestamp = 0
    buffer_offset = 10
    exchanges = []

    @classmethod
    def init_markets(cls, config: ArchiveConfig):
        exchanges = config.exchanges
        symbols = config.symbols
        cls.markets = {}
        for exchange in exchanges:
            cls.markets[exchange] = set(symbols)

    def __init__(self, connectors: Dict[str, ConnectorBase], config: ArchiveConfig):
        super().__init__(connectors)
        self.config = config
        self.connect_to_db()
        self.order_book_trade_event = SourceInfoEventForwarder(self._process_public_trade)
        for exchange in self.config.exchanges:
            if "paper_trade" in exchange:
                self.exchanges.append('_'.join(exchange.split("_")[:-2]))
            else:
                self.exchanges.append(exchange)
        for exchange in self.exchanges:
            self.order_book_temp_storage[exchange] = {}
            self.trades_temp_storage[exchange] = {}
            for symbol in self.config.symbols:
                self.order_book_temp_storage[exchange][symbol] = []
                self.trades_temp_storage[exchange][symbol] = []
        self.create_db_table()

    def connect_to_db(self):
        connection_link = os.environ.get("ARCHIVE_DB_CONNECTION")
        self.connection = psycopg2.connect(connection_link)

    def subscribe_to_order_book_trade_event(self):
        for market in self.connectors.values():
            for order_book in market.order_books.values():
                order_book.add_listener(OrderBookEvent.TradeEvent, self.order_book_trade_event)
        self.subscribed_to_order_book_trade_event = True

    def on_tick(self):
        if not self.subscribed_to_order_book_trade_event:
            self.subscribe_to_order_book_trade_event()
        for exchange in self.exchanges:
            for symbol in self.config.symbols:
                order_book_data = self.get_order_book_dict(exchange, symbol, self.config.depth)
                self.order_book_temp_storage[exchange][symbol].append(order_book_data)
        if self.last_dump_timestamp < self.current_timestamp:
            self.write_to_db_table()

    def create_db_table(self):
        cursor = self.connection.cursor()
        for symbol in self.config.symbols:
            symbol_sql = symbol.replace("-", "_").lower()
            # TODO: check both tables exist
            cursor.execute(f"SELECT EXISTS (SELECT relname FROM pg_class WHERE relname='{symbol_sql}_book' AND relkind='r')")
            exists = cursor.fetchone()[0]
            if exists:
                self.logger().info(f"Table {symbol_sql}_book already exists.")
                continue
            book_schema = {"exchange": "VARCHAR(10)", "timestamp": "TIMESTAMPTZ NOT NULL"}
            for i in range(self.config.depth):
                book_schema[f"bid_price_{i}"] = "DOUBLE PRECISION"
                book_schema[f"bid_amount_{i}"] = "DOUBLE PRECISION"
                book_schema[f"ask_price_{i}"] = "DOUBLE PRECISION"
                book_schema[f"ask_amount_{i}"] = "DOUBLE PRECISION"

            trade_schema = {
                "exchange": "VARCHAR(10)",
                "timestamp": "TIMESTAMPTZ NOT NULL",
                "price": "DOUBLE PRECISION",
                "amount": "DOUBLE PRECISION",
                "is_buy": "BOOLEAN",
                "is_aggressive": "BOOLEAN",
            }
            query_create_order_book_table = f"""
                CREATE TABLE {symbol_sql}_book (
                    {', '.join(f'{col} {type}' for col, type in book_schema.items())}
                );
            """
            query_create_order_book_hypertable = f"SELECT create_hypertable('{symbol_sql}_book', by_range('timestamp'));"
            query_create_trades_table = f"""
                CREATE TABLE {symbol_sql}_trade (
                    {', '.join(f'{col} {type}' for col, type in trade_schema.items())}
                );
            """
            query_create_trades_hypertable = f"SELECT create_hypertable('{symbol_sql}_trade', by_range('timestamp'));"

            book_compress_query = f"ALTER TABLE {symbol_sql}_book SET (timescaledb.compress, timescaledb.compress_orderby = 'timestamp');"
            trade_compress_query = f"ALTER TABLE {symbol_sql}_trade SET (timescaledb.compress, timescaledb.compress_orderby = 'timestamp');"
            book_compress_rule_query = f"SELECT add_compression_policy('{symbol_sql}_book', compress_after => INTERVAL '30d');"
            trade_compress_rule_query = f"SELECT add_compression_policy('{symbol_sql}_trade', compress_after => INTERVAL '90d');"

            cursor.execute(query_create_order_book_table)
            cursor.execute(query_create_order_book_hypertable)
            cursor.execute(query_create_trades_table)
            cursor.execute(query_create_trades_hypertable)
            cursor.execute(book_compress_query)
            cursor.execute(trade_compress_query)
            cursor.execute(book_compress_rule_query)
            cursor.execute(trade_compress_rule_query)
            self.connection.commit()
            self.logger().info(f"Table {symbol}_book created.")
            self.logger().info(f"Table {symbol}_trade created.")

    def write_order_book_to_db(self, symbol, order_book):
        symbol_sql = symbol.replace("-", "_").lower()
        cursor = self.connection.cursor()
        query = f"INSERT INTO {symbol_sql}_book ({', '.join(order_book.keys())}) VALUES ({', '.join(['%s'] * len(order_book))})"
        try:
            cursor.execute(query, list(order_book.values()))
        except (Exception, psycopg2.Error) as e:
            self.logger().error(f"Error writing to db: {e.pgerror}")
        self.connection.commit()

    def write_trade_to_db(self, symbol, trade):
        symbol_sql = symbol.replace("-", "_").lower()
        cursor = self.connection.cursor()
        query = f"INSERT INTO {symbol_sql}_trade ({', '.join(trade.keys())}) VALUES ({', '.join(['%s'] * len(trade))})"
        try:
            cursor.execute(query, list(trade.values()))
        except (Exception, psycopg2.Error) as e:
            self.logger().error(f"Error writing to db: {e.pgerror}")
        self.connection.commit()

    def write_to_db_table(self):
        for exchange, order_book_info in self.order_book_temp_storage.items():
            for symbol, order_books in order_book_info.items():
                for order_book in order_books:
                    self.write_order_book_to_db(symbol, order_book)
                self.order_book_temp_storage[exchange][symbol] = []
        for exchange, trades_info in self.trades_temp_storage.items():
            for symbol, trades in trades_info.items():
                for trade in trades:
                    self.write_trade_to_db(symbol, trade)
                self.trades_temp_storage[exchange][symbol] = []
        self.last_dump_timestamp = self.current_timestamp + self.buffer_offset

    def get_order_book_dict(self, exchange: str, symbol: str, depth: int = 10):
        paper_trade_exchange = exchange + "_paper_trade"
        order_book = self.connectors[paper_trade_exchange].get_order_book(symbol)
        snapshot = order_book.snapshot
        bid_len = len(snapshot[0])
        ask_len = len(snapshot[1])
        book_dict = {"exchange": exchange, "timestamp": datetime.fromtimestamp(self.current_timestamp).isoformat()}
        for i in range(min(depth, bid_len)):
            book_dict[f"bid_price_{i}"] = snapshot[0].loc[i]["price"]
            book_dict[f"bid_amount_{i}"] = snapshot[0].loc[i]["amount"]
        for i in range(min(depth, ask_len)):
            book_dict[f"ask_price_{i}"] = snapshot[1].loc[i]["price"]
            book_dict[f"ask_amount_{i}"] = snapshot[1].loc[i]["amount"]
        return book_dict

    def _process_public_trade(self, event_tag: int, market: ConnectorBase, event: OrderBookTradeEvent):
        self.trades_temp_storage[event.exchange][event.trading_pair].append({
            "exchange": event.exchange,
            "timestamp": datetime.fromtimestamp(event.timestamp).isoformat(),
            "price": event.price,
            "amount": event.amount,
            "is_buy": True if event.type == TradeType.BUY else False,
            "is_aggressive": event.is_taker,
        })
