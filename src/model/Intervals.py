from enum import Enum

class IntervalTypes(Enum):
        Intraday = 0
        Daily = 1
        
INTERVALS = {
    "1m" : IntervalTypes.Intraday,
    "2m" : IntervalTypes.Intraday,
    "5m" : IntervalTypes.Intraday,
    "15m": IntervalTypes.Intraday,
    "30m": IntervalTypes.Intraday,
    "60m": IntervalTypes.Intraday,
    "90m": IntervalTypes.Intraday,
    "1h" : IntervalTypes.Intraday,
    "1d" : IntervalTypes.Daily,
    "5d" : IntervalTypes.Daily,
    "1wk": IntervalTypes.Daily,
    "1mo": IntervalTypes.Daily,
    "3mo": IntervalTypes.Daily,
    }