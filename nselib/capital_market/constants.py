"""Constants for capital markets."""

PRICE_VOLUME_AND_DELIVERABLE_POSITION_DATA_COLUMNS = [
    "Symbol",
    "Series",
    "Date",
    "PrevClose",
    "OpenPrice",
    "HighPrice",
    "LowPrice",
    "LastPrice",
    "ClosePrice",
    "AveragePrice",
    "TotalTradedQuantity",
    "TurnoverInRs",
    "No.ofTrades",
    "DeliverableQty",
    "%DlyQttoTradedQty",
]

PRICE_VOLUME_DATA_COLUMNS = [
    "Symbol",
    "Series",
    "Date",
    "PrevClose",
    "OpenPrice",
    "HighPrice",
    "LowPrice",
    "LastPrice",
    "ClosePrice",
    "AveragePrice",
    "TotalTradedQuantity",
    "Turnover",
    "No.ofTrades",
]

DELIVERABLE_DATA_COLUMNS = [
    "Symbol",
    "Series",
    "Date",
    "TradedQty",
    "DeliverableQty",
    "%DlyQttoTradedQty",
]

BLOCK_DEALS_DATA_COLUMNS = [
    "Date",
    "Symbol",
    "SecurityName",
    "ClientName",
    "Buy/Sell",
    "QuantityTraded",
    "TradePrice/Wght.Avg.Price",
    "Remarks",
]

BULK_DEAL_DATA_COLUMNS = [
    "Date",
    "Symbol",
    "SecurityName",
    "ClientName",
    "Buy/Sell",
    "QuantityTraded",
    "TradePrice/Wght.Avg.Price",
    "Remarks",
]



INDIA_VIX_DATA_COLUMN = [
    "TIMESTAMP",
    "INDEX_NAME",
    "OPEN_INDEX_VAL",
    "CLOSE_INDEX_VAL",
    "HIGH_INDEX_VAL",
    "LOW_INDEX_VAL",
    "PREV_CLOSE",
    "VIX_PTS_CHG",
    "VIX_PERC_CHG",
]

INDEX_DATA_COLUMNS = [
    "INDEX_NAME",
    "OPEN_INDEX_VAL",
    "HIGH_INDEX_VAL",
    "CLOSE_INDEX_VAL",
    "LOW_INDEX_VAL",
    "TURN_OVER",
    "TRADED_QTY",
    "TIMESTAMP",
]

SHORT_SELL_DATA_COLUMNS = ["Date", "Symbol", "SecurityName", "Quantity"]

VAR_COLUMNS = [
    "RecordType",
    "Symbol",
    "Series",
    "Isin",
    "SecurityVaR",
    "IndexVaR",
    "VaRMargin",
    "ExtremeLossRate",
    "AdhocMargin",
    "ApplicableMarginRate",
]
