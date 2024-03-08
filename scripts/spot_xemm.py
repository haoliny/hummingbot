import os
from decimal import Decimal
from typing import Dict

import pandas as pd
from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel, ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class XEMMConfig(BaseClientModel):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    maker_exchange: str = Field("okx_paper_trade", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the maker exchange where the bot will trade:"))
    maker_pair: str = Field("DOGE-USDT", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the maker trading pair in which the bot will place orders:"))
    maker_buy_symbol: str = Field("USDT", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the maker buy symbol:"))
    maker_sell_symbol: str = Field("DOGE", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the maker sell symbol:"))
    taker_exchange: str = Field("binance_paper_trade", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the taker exchange where the bot will trade:"))
    taker_pair: str = Field("DOGE-USDT", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the taker trading pair in which the bot will place orders:"))
    taker_buy_symbol: str = Field("USDT", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the taker buy symbol:"))
    taker_sell_symbol: str = Field("DOGE", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the taker sell symbol:"))
    order_amount: Decimal = Field(500, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the order amount (denominated in base asset):"))
    spread_bps: Decimal = Field(32, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the spread in basis points:"))
    min_spread_bps: Decimal = Field(22, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the minimum spread in basis points:"))
    slippage_buffer_spread_bps: Decimal = Field(2, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the slippage buffer spread in basis points:"))
    max_order_age: int = Field(30, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the maximum order age (in seconds):"))
    taker_fee: Decimal = Field(0.00100, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the taker fee:"))
    maker_fee: Decimal = Field(0.00100, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the maker fee:"))


class XEMM(ScriptStrategyBase):
    # sell_inventory_size = 1200
    # buy_inventory_size = 1000
    @classmethod
    def init_markets(cls, config: XEMMConfig):
        cls.markets = {config.maker_exchange: {config.maker_pair}, config.taker_exchange: {config.taker_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: XEMMConfig):
        super().__init__(connectors)
        self.config = config
        self.buy_order_placed = False
        self.sell_order_placed = False
        self.inventory_ready = False
        self.total_potential_profit = Decimal(0.0)

    # TODO: make this automated process
    def build_inventory(self):
        self.logger().info("Building inventory...")
        # buy_balance = self.connectors[self.config.maker_exchange].get_available_balance(self.config.maker_buy_symbol)
        # sell_balance = self.connectors[self.config.maker_exchange].get_available_balance(self.config.maker_sell_symbol)
        # if buy_balance < self.config.buy_inventory_size:
        #     # place buy order
        #     return
        # if sell_balance < self.config.sell_inventory_size:
        #     # place sell order
        #     return
        self.config.inventory_ready = True
        self.logger().info("Inventory ready.")

    def clear_inventory(self):
        # delete all the orders
        # check current size
        self.logger().info("Clearing inventory...")
        self.config.inventory_ready = False
        self.logger().info("Inventory cleared.")

    balance_ratio = 0.3

    # TODO: add position limit
    def check_position_balance(self, is_buy: bool) -> bool:
        maker_buy_balance = self.connectors[self.config.maker_exchange].get_available_balance(self.config.maker_buy_symbol)
        maker_sell_balance = self.connectors[self.config.maker_exchange].get_available_balance(self.config.maker_sell_symbol)
        taker_buy_balance = self.connectors[self.config.taker_exchange].get_available_balance(self.config.taker_buy_symbol)
        taker_sell_balance = self.connectors[self.config.taker_exchange].get_available_balance(self.config.taker_sell_symbol)

        maker_buy_value = maker_buy_balance * self.connectors[self.config.maker_exchange].get_mid_price(
            self.config.maker_buy_symbol
        )  # USDT
        maker_sell_value = maker_sell_balance * self.connectors[self.config.maker_exchange].get_mid_price(
            self.config.maker_sell_symbol
        )  # DOGE
        taker_buy_value = taker_buy_balance * self.connectors[self.config.taker_exchange].get_mid_price(
            self.config.taker_buy_symbol
        )  # USD
        taker_sell_value = taker_sell_balance * self.connectors[self.config.taker_exchange].get_mid_price(
            self.config.taker_sell_symbol
        )  # DOGE

        def check_ratio(a, b):
            return (a / b) < (1 + self.config.balance_ratio) if a > b else (b / a) < (1 + self.config.balance_ratio)

        return check_ratio(maker_buy_value, maker_sell_value) and check_ratio(taker_sell_value, taker_buy_value)

    def check_position_limit(self, is_buy: bool) -> bool:
        mid_price = self.connectors[self.config.maker_exchange].get_mid_price(self.config.maker_pair)
        expected_value = self.config.order_amount * mid_price
        if is_buy:
            maker_buy_balance = self.connectors[self.config.maker_exchange].get_available_balance(self.config.maker_buy_symbol)
            taker_sell_balance = self.connectors[self.config.taker_exchange].get_available_balance(self.config.taker_sell_symbol)
            return maker_buy_balance >= expected_value and taker_sell_balance >= expected_value
        else:
            maker_sell_balance = self.connectors[self.config.maker_exchange].get_available_balance(self.config.maker_sell_symbol)
            taker_buy_balance = self.connectors[self.config.taker_exchange].get_available_balance(self.config.taker_buy_symbol)
            return maker_sell_balance >= expected_value and taker_buy_balance >= expected_value

    def on_tick(self):
        # if not self.config.inventory_ready:
        #     self.config.build_inventory()

        taker_buy_result = self.connectors[self.config.taker_exchange].get_price_for_volume(
            self.config.taker_pair, True, self.config.order_amount
        )
        taker_sell_result = self.connectors[self.config.taker_exchange].get_price_for_volume(
            self.config.taker_pair, False, self.config.order_amount
        )
        maker_buy_result = self.connectors[self.config.maker_exchange].get_price_for_volume(
            self.config.maker_pair, True, self.config.order_amount
        )
        maker_sell_result = self.connectors[self.config.maker_exchange].get_price_for_volume(
            self.config.maker_pair, False, self.config.order_amount
        )

        if not self.buy_order_placed:
            if not self.check_position_limit(True):
                self.logger().warning("Hitting position limit when putting buy order.")
            else:
                maker_buy_price = min(
                    taker_sell_result.result_price * Decimal(1 - self.config.spread_bps / 10000), maker_buy_result.result_price
                )
                buy_order_amount = min(self.config.order_amount, self.buy_hedging_budget())
                buy_order = OrderCandidate(
                    trading_pair=self.config.maker_pair,
                    is_maker=True,
                    order_type=OrderType.LIMIT,
                    order_side=TradeType.BUY,
                    amount=Decimal(buy_order_amount),
                    price=maker_buy_price,
                )
                buy_order_adjusted = self.connectors[self.config.maker_exchange].budget_checker.adjust_candidate(
                    buy_order, all_or_none=False
                )
                self.buy(
                    self.config.maker_exchange,
                    self.config.maker_pair,
                    buy_order_adjusted.amount,
                    buy_order_adjusted.order_type,
                    buy_order_adjusted.price,
                )
                self.buy_order_placed = True

        if not self.sell_order_placed:
            if not self.check_position_limit(False):
                self.logger().warning("Hitting position limit when putting sell order.")
            else:
                maker_sell_price = max(
                    taker_buy_result.result_price * Decimal(1 + self.config.spread_bps / 10000), maker_sell_result.result_price
                )
                sell_order_amount = min(self.config.order_amount, self.sell_hedging_budget())
                sell_order = OrderCandidate(
                    trading_pair=self.config.maker_pair,
                    is_maker=True,
                    order_type=OrderType.LIMIT,
                    order_side=TradeType.SELL,
                    amount=Decimal(sell_order_amount),
                    price=maker_sell_price,
                )
                sell_order_adjusted = self.connectors[self.config.maker_exchange].budget_checker.adjust_candidate(
                    sell_order, all_or_none=False
                )
                self.sell(
                    self.config.maker_exchange,
                    self.config.maker_pair,
                    sell_order_adjusted.amount,
                    sell_order_adjusted.order_type,
                    sell_order_adjusted.price,
                )
                self.sell_order_placed = True

        for order in self.get_active_orders(connector_name=self.config.maker_exchange):
            cancel_timestamp = order.creation_timestamp / 1000000 + self.config.max_order_age
            if order.is_buy:
                buy_cancel_threshold = taker_sell_result.result_price * Decimal(1 - self.config.min_spread_bps / 10000)
                if order.price > buy_cancel_threshold or cancel_timestamp < self.current_timestamp:
                    self.logger().info(f"Cancelling buy order: {order.client_order_id}")
                    self.cancel(self.config.maker_exchange, order.trading_pair, order.client_order_id)
                    self.buy_order_placed = False
            else:
                sell_cancel_threshold = taker_buy_result.result_price * Decimal(1 + self.config.min_spread_bps / 10000)
                if order.price < sell_cancel_threshold or cancel_timestamp < self.current_timestamp:
                    self.logger().info(f"Cancelling sell order: {order.client_order_id}")
                    self.cancel(self.config.maker_exchange, order.trading_pair, order.client_order_id)
                    self.sell_order_placed = False
        return

    def on_stop(self):
        # self.clear_inventory()
        self.logger().info(f"Total potential profit: {self.total_potential_profit}")

    def buy_hedging_budget(self) -> Decimal:
        balance = self.connectors[self.config.taker_exchange].get_available_balance(self.config.taker_sell_symbol)
        return balance

    # TODO: figure out why
    def sell_hedging_budget(self) -> Decimal:
        balance = self.connectors[self.config.taker_exchange].get_available_balance(self.config.taker_buy_symbol)
        taker_buy_result = self.connectors[self.config.taker_exchange].get_price_for_volume(
            self.config.taker_pair, True, self.config.order_amount
        )
        return balance / taker_buy_result.result_price

    def is_active_maker_order(self, event: OrderFilledEvent):
        """
        Helper function that checks if order is an active order on the maker exchange
        """
        # TODO: why loop here?
        for order in self.get_active_orders(connector_name=self.config.maker_exchange):
            if order.client_order_id == event.order_id:
                return True
        return False

    def did_fill_order(self, event: OrderFilledEvent):
        if event.trade_type == TradeType.BUY and self.is_active_maker_order(event):
            taker_sell_result = self.connectors[self.config.taker_exchange].get_price_for_volume(
                self.config.taker_pair, False, self.config.order_amount
            )
            sell_price_with_slippage = taker_sell_result.result_price * Decimal(
                1 - self.config.slippage_buffer_spread_bps / 10000
            )
            self.logger().info(f"Filled maker buy order with price: {event.price}")
            sell_order = OrderCandidate(
                trading_pair=self.config.taker_pair,
                is_maker=False,
                order_type=OrderType.LIMIT,
                order_side=TradeType.SELL,
                amount=Decimal(event.amount),
                price=sell_price_with_slippage,
            )
            # TODO: figure out what is doing inside adjust_candidate
            sell_order_adjusted = self.connectors[self.config.taker_exchange].budget_checker.adjust_candidate(
                sell_order, all_or_none=False
            )
            potential_profit = (sell_order_adjusted.price * Decimal(1 - self.config.taker_fee) - event.price * Decimal(1 + self.config.maker_fee)) * sell_order_adjusted.amount
            self.total_potential_profit += potential_profit
            self.logger().info(
                f"Sending taker sell order at price: {sell_price_with_slippage} ***potential profit***: {potential_profit}"
            )
            self.sell(
                self.config.taker_exchange,
                self.config.taker_pair,
                sell_order_adjusted.amount,
                sell_order_adjusted.order_type,
                sell_order_adjusted.price,
            )
            self.buy_order_placed = False
        else:
            if event.trade_type == TradeType.SELL and self.is_active_maker_order(event):
                taker_buy_result = self.connectors[self.config.taker_exchange].get_price_for_volume(
                    self.config.taker_pair, True, self.config.order_amount
                )
                buy_price_with_slippage = taker_buy_result.result_price * Decimal(
                    1 + self.config.slippage_buffer_spread_bps / 10000
                )
                self.logger().info(f"Filled maker sell order at price: {event.price}")
                buy_order = OrderCandidate(
                    trading_pair=self.config.taker_pair,
                    is_maker=False,
                    order_type=OrderType.LIMIT,
                    order_side=TradeType.BUY,
                    amount=Decimal(event.amount),
                    price=buy_price_with_slippage,
                )
                buy_order_adjusted = self.connectors[self.config.taker_exchange].budget_checker.adjust_candidate(
                    buy_order, all_or_none=False
                )
                potential_profit = (event.price * Decimal(1 - self.config.maker_fee) - buy_order_adjusted.price * Decimal(1 + self.config.taker_fee)) * buy_order_adjusted.amount
                self.total_potential_profit += potential_profit
                self.logger().info(
                    f"Sending taker buy order: {taker_buy_result.result_price} ***potential proft***: {potential_profit}"
                )
                self.buy(
                    self.config.taker_exchange,
                    self.config.taker_pair,
                    buy_order_adjusted.amount,
                    buy_order_adjusted.order_type,
                    buy_order_adjusted.price,
                )
                self.sell_order_placed = False

    def exchanges_df(self) -> pd.DataFrame:
        """
        Return a custom data frame of prices on maker vs taker exchanges for display purposes
        """
        mid_price = self.connectors[self.config.maker_exchange].get_mid_price(self.config.maker_pair)
        maker_buy_result = self.connectors[self.config.maker_exchange].get_price_for_volume(
            self.config.maker_pair, True, self.config.order_amount
        )
        maker_sell_result = self.connectors[self.config.maker_exchange].get_price_for_volume(
            self.config.maker_pair, False, self.config.order_amount
        )
        taker_buy_result = self.connectors[self.config.taker_exchange].get_price_for_volume(
            self.config.taker_pair, True, self.config.order_amount
        )
        taker_sell_result = self.connectors[self.config.taker_exchange].get_price_for_volume(
            self.config.taker_pair, False, self.config.order_amount
        )
        maker_buy_spread_bps = (maker_buy_result.result_price - taker_buy_result.result_price) / mid_price * 10000
        maker_sell_spread_bps = (taker_sell_result.result_price - maker_sell_result.result_price) / mid_price * 10000
        columns = ["Exchange", "Market", "Mid Price", "TOB Sell", "TOB Buy", "Buy Spread", "Sell Spread"]
        data = []
        data.append(
            [
                self.config.maker_exchange,
                self.config.maker_pair,
                float(self.connectors[self.config.maker_exchange].get_mid_price(self.config.maker_pair)),
                float(maker_buy_result.result_price),
                float(maker_sell_result.result_price),
                int(maker_buy_spread_bps),
                int(maker_sell_spread_bps),
            ]
        )
        data.append(
            [
                self.config.taker_exchange,
                self.config.taker_pair,
                float(self.connectors[self.config.taker_exchange].get_mid_price(self.config.taker_pair)),
                float(taker_buy_result.result_price),
                float(taker_sell_result.result_price),
                int(-maker_buy_spread_bps),
                int(-maker_sell_spread_bps),
            ]
        )
        df = pd.DataFrame(data=data, columns=columns)
        return df

    def active_orders_df(self) -> pd.DataFrame:
        """
        Returns a custom data frame of all active maker orders for display purposes
        """
        columns = ["Exchange", "Market", "Side", "Price", "Amount", "Spread Mid", "Spread Cancel", "Age"]
        data = []
        mid_price = self.connectors[self.config.maker_exchange].get_mid_price(self.config.maker_pair)
        taker_buy_result = self.connectors[self.config.taker_exchange].get_price_for_volume(
            self.config.taker_pair, True, self.config.order_amount
        )
        taker_sell_result = self.connectors[self.config.taker_exchange].get_price_for_volume(
            self.config.taker_pair, False, self.config.order_amount
        )
        buy_cancel_threshold = taker_sell_result.result_price * Decimal(1 - self.config.min_spread_bps / 10000)
        sell_cancel_threshold = taker_buy_result.result_price * Decimal(1 + self.config.min_spread_bps / 10000)
        for connector_name, _ in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                age_txt = "n/a" if order.age() <= 0.0 else pd.Timestamp(order.age(), unit="s").strftime("%H:%M:%S")
                spread_mid_bps = (
                    (mid_price - order.price) / mid_price * 10000
                    if order.is_buy
                    else (order.price - mid_price) / mid_price * 10000
                )
                spread_cancel_bps = (
                    (buy_cancel_threshold - order.price) / buy_cancel_threshold * 10000
                    if order.is_buy
                    else (order.price - sell_cancel_threshold) / sell_cancel_threshold * 10000
                )
                data.append(
                    [
                        self.config.maker_exchange,
                        order.trading_pair,
                        "buy" if order.is_buy else "sell",
                        float(order.price),
                        float(order.quantity),
                        int(spread_mid_bps),
                        int(spread_cancel_bps),
                        age_txt,
                    ]
                )
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Market", "Side"], inplace=True)
        return df

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        exchanges_df = self.exchanges_df()
        lines.extend(["", "  Exchanges:"] + ["    " + line for line in exchanges_df.to_string(index=False).split("\n")])

        try:
            orders_df = self.active_orders_df()
            lines.extend(
                ["", "  Active Orders:"] + ["    " + line for line in orders_df.to_string(index=False).split("\n")]
            )
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        lines.extend([f"Current total potential profit: {self.total_potential_profit}"])
        return "\n".join(lines)
