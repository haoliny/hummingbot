import logging
import os
from decimal import Decimal
from typing import Any, Dict

import pandas as pd
from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel, ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class XEArbConfig(BaseClientModel):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    exchange_A: str = Field("binance_paper_trade", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the first exchange where the bot will trade:"))
    exchange_B: str = Field("kucoin_paper_trade", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the second exchange where the bot will trade:"))
    base: str = Field("ETH", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the base asset:"))
    quote: str = Field("USDT", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the quote asset:"))
    trading_pair: str = Field("ETH-USDT", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the trading pair in which the bot will place orders:"))
    order_amount: Decimal = Field(0.01, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the order amount (denominated in base asset):"))
    min_profitability: Decimal = Field(0.002, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the minimum profitability (in percentage):"))
    exchange_A_fee: Decimal = Field(0.001, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the fee percentage for the first exchange:"))
    exchange_B_fee: Decimal = Field(0.001, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the fee percentage for the second exchange:"))


class XEArb(ScriptStrategyBase):
    @classmethod
    def init_markets(cls, config: XEArbConfig):
        cls.markets = {config.exchange_A: {config.trading_pair}, config.exchange_B: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: XEArbConfig):
        super().__init__(connectors)
        self.config = config
        self.total_potential_profit = Decimal(0.0)

    def on_tick(self):
        vwap_prices = self.get_vwap_prices_for_amount(self.config.order_amount)
        proposal = self.check_profitability_and_create_proposal(vwap_prices)
        if len(proposal) > 0:
            proposal_adjusted: Dict[str, OrderCandidate] = self.adjust_proposal_to_budget(proposal)
            self.place_orders(proposal_adjusted)

    def get_vwap_prices_for_amount(self, amount: Decimal):
        bid_ex_a = self.connectors[self.config.exchange_A].get_vwap_for_volume(self.config.trading_pair, False, amount)
        ask_ex_a = self.connectors[self.config.exchange_A].get_vwap_for_volume(self.config.trading_pair, True, amount)
        bid_ex_b = self.connectors[self.config.exchange_B].get_vwap_for_volume(self.config.trading_pair, False, amount)
        ask_ex_b = self.connectors[self.config.exchange_B].get_vwap_for_volume(self.config.trading_pair, True, amount)
        vwap_prices = {
            self.config.exchange_A: {
                "bid": bid_ex_a.result_price,
                "ask": ask_ex_a.result_price
            },
            self.config.exchange_B: {
                "bid": bid_ex_b.result_price,
                "ask": ask_ex_b.result_price
            }
        }
        return vwap_prices

    def get_profitability_analysis(self, vwap_prices: Dict[str, Any]) -> Dict:
        buy_a_sell_b_quote = vwap_prices[self.config.exchange_A]["ask"] * (1 - self.config.exchange_A_fee) * self.config.order_amount - \
            vwap_prices[self.config.exchange_B]["bid"] * (1 + self.config.exchange_B_fee) * self.config.order_amount
        buy_a_sell_b_base = buy_a_sell_b_quote / (
            (vwap_prices[self.config.exchange_A]["ask"] + vwap_prices[self.config.exchange_B]["bid"]) / 2)

        buy_b_sell_a_quote = vwap_prices[self.config.exchange_B]["ask"] * (1 - self.config.exchange_B_fee) * self.config.order_amount - \
            vwap_prices[self.config.exchange_A]["bid"] * (1 + self.config.exchange_A_fee) * self.config.order_amount

        buy_b_sell_a_base = buy_b_sell_a_quote / (
            (vwap_prices[self.config.exchange_B]["ask"] + vwap_prices[self.config.exchange_A]["bid"]) / 2)

        return {
            "buy_a_sell_b":
                {
                    "quote_diff": buy_a_sell_b_quote,
                    "base_diff": buy_a_sell_b_base,
                    "profitability_pct": buy_a_sell_b_base / self.config.order_amount
                },
            "buy_b_sell_a":
                {
                    "quote_diff": buy_b_sell_a_quote,
                    "base_diff": buy_b_sell_a_base,
                    "profitability_pct": buy_b_sell_a_base / self.config.order_amount
                },
        }

    def check_profitability_and_create_proposal(self, vwap_prices: Dict[str, Any]) -> Dict:
        proposal = {}
        profitability_analysis = self.get_profitability_analysis(vwap_prices)
        if profitability_analysis["buy_a_sell_b"]["profitability_pct"] > self.config.min_profitability:
            # This means that the ask of the first exchange is lower than the bid of the second one
            proposal[self.config.exchange_A] = OrderCandidate(trading_pair=self.trading_pair, is_maker=False,
                                                       order_type=OrderType.MARKET,
                                                       order_side=TradeType.BUY, amount=self.config.order_amount,
                                                       price=vwap_prices[self.config.exchange_A]["ask"])
            proposal[self.config.exchange_B] = OrderCandidate(trading_pair=self.trading_pair, is_maker=False,
                                                       order_type=OrderType.MARKET,
                                                       order_side=TradeType.SELL, amount=Decimal(self.config.order_amount),
                                                       price=vwap_prices[self.config.exchange_B]["bid"])
        elif profitability_analysis["buy_b_sell_a"]["profitability_pct"] > self.config.min_profitability:
            # This means that the ask of the second exchange is lower than the bid of the first one
            proposal[self.config.exchange_B] = OrderCandidate(trading_pair=self.trading_pair, is_maker=False,
                                                       order_type=OrderType.MARKET,
                                                       order_side=TradeType.BUY, amount=self.config.order_amount,
                                                       price=vwap_prices[self.config.exchange_B]["ask"])
            proposal[self.config.exchange_A] = OrderCandidate(trading_pair=self.trading_pair, is_maker=False,
                                                       order_type=OrderType.MARKET,
                                                       order_side=TradeType.SELL, amount=Decimal(self.config.order_amount),
                                                       price=vwap_prices[self.config.exchange_A]["bid"])

        return proposal

    def adjust_proposal_to_budget(self, proposal: Dict[str, OrderCandidate]) -> Dict[str, OrderCandidate]:
        for connector, order in proposal.items():
            proposal[connector] = self.connectors[connector].budget_checker.adjust_candidate(order, all_or_none=True)
        return proposal

    def place_orders(self, proposal: Dict[str, OrderCandidate]) -> None:
        for connector, order in proposal.items():
            self.place_order(connector_name=connector, order=order)

    def place_order(self, connector_name: str, order: OrderCandidate):
        if order.order_side == TradeType.SELL:
            self.sell(connector_name=connector_name, trading_pair=order.trading_pair, amount=order.amount,
                      order_type=order.order_type, price=order.price)
        elif order.order_side == TradeType.BUY:
            self.buy(connector_name=connector_name, trading_pair=order.trading_pair, amount=order.amount,
                     order_type=order.order_type, price=order.price)

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        vwap_prices = self.get_vwap_prices_for_amount(self.config.order_amount)
        lines.extend(["", "  VWAP Prices for amount"] + ["     " + line for line in
                                                         pd.DataFrame(vwap_prices).to_string().split("\n")])
        profitability_analysis = self.get_profitability_analysis(vwap_prices)
        lines.extend(["", "  Profitability (%)"] + [
            f"     Buy A: {self.config.exchange_A} --> Sell B: {self.config.exchange_B}"] + [
            f"          Quote Diff: {profitability_analysis['buy_a_sell_b']['quote_diff']:.7f}"] + [
            f"          Base Diff: {profitability_analysis['buy_a_sell_b']['base_diff']:.7f}"] + [
            f"          Percentage: {profitability_analysis['buy_a_sell_b']['profitability_pct'] * 100:.4f} %"] + [
            f"     Buy B: {self.config.exchange_B} --> Sell A: {self.config.exchange_A}"] + [
            f"          Quote Diff: {profitability_analysis['buy_b_sell_a']['quote_diff']:.7f}"] + [
            f"          Base Diff: {profitability_analysis['buy_b_sell_a']['base_diff']:.7f}"] + [
            f"          Percentage: {profitability_analysis['buy_b_sell_a']['profitability_pct'] * 100:.4f} %"
        ])

        warning_lines.extend(self.balance_warning(self.get_market_trading_pair_tuples()))
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)
        return "\n".join(lines)

    def did_fill_order(self, event: OrderFilledEvent):
        msg = (
            f"{event.trade_type.name} {round(event.amount, 2)} {event.trading_pair} at {round(event.price, 2)}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)
