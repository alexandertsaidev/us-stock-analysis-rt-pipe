# config/period_config.py

# 假設全域預設的 INDICATOR 與 explain 參數

DEFAULT_INDICATOR_PARAMS = {
    "ADX": {"timeperiod":14},
    "MACD": {"fastperiod":12, "slowperiod":26, "signalperiod":9},
    "DI": {"timeperiod":13},
    "STOCH": {"fastk_period":5, "slowk_period":3, "slowd_period":3, "slowd_matype":0},
    "WILLR": {"timeperiod":7},
    "BBANDS": {"timeperiod":13, "nbdevup":2.7, "nbdevdn":2.7, "matype":1},
    "SD_17": {"timeperiod":13, "nbdevup":1.7, "nbdevdn":1.7, "matype":1},
    "EMA": {"ema_period":[13,26]},
    "SMA": {"sma_period":[50,200]},
    "ATR": {"timeperiod":14},
    "FI": {"ema_period":[2,13]},
    "Bull_Bear_Power": {"ema_period":13}
}

DEFAULT_EXPLAIN_PARAMS = {
    "ema_short": 13,
    "ema_long": 26,
    "input_price_1": "Close",
    "extrema_price_gap": 5,
    "extrema_FI_2_gap": 5,
    "extrema_price_prominence": 1,
    "extrema_FI_2_prominence": 5,
    "search_quantity": 2,
    "indicator_pair_1": ["MACD_hist", "FI_13"],
    "indicator_pair_2": ["EMA13", "FI_2"],
    "indicator_pair_3": ["Bull_Power", "Bear_Power"],
    "pair_1_threshold": [30,70],
    "side_pair_1": "Side_1",
    "side_pair_2": "Side_2",
    "side_pair_3": "Side_3"
}

# 7 種 period 配置
indicator_and_period_configs = {
    "D": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS}
    },
    "W": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS}
    },
    "2W": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS}
    },
    "3W": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS}
    },
    "ME": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS}
    },
    "2ME": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS}
    },
    "3ME": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS},
    }
}
