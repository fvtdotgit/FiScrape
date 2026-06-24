from fiscrape_core import technical_snapshot
import pandas as pd

# show all columns (no ... in the header)
pd.set_option("display.max_columns", None)

# show all rows (careful for huge frames)
pd.set_option("display.max_rows", None)

# don’t truncate long strings in cells
pd.set_option("display.max_colwidth", None)

# optional: widen the console width to reduce wrapping
pd.set_option("display.width", 0)  # or a large int, e.g. 200
pd.set_option("display.expand_frame_repr", False)  # keeps one-line rows when wide

df_plot, meta = technical_snapshot(
    symbol="AAPL",
    period="6mo",
    interval="1d",
    requests=[
        "SMA(200)",
        {"name": "EMA", "length": 69},
        "RSI(14)",
        "MACD(12,26,9)",
        "BB(20,2)",
        "VWAP",
    ],
    style_label="candle",  # or "line"
    tz="UTC",  # or your display TZ
)

print(df_plot.columns.tolist())  # see available columns
print(df_plot)
print(len(df_plot), "rows")
print(meta["warmupComplete"], meta["availablePreload"], meta["requiredWarmupBars"])
print("computed:", meta["computedSignals"])
print("skipped:", meta["skippedSignals"])
