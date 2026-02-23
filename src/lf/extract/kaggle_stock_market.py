import pandas as pd

try:
    df_stock = pd.read_csv(
        "/kaggle/input/stock-market-regimes-20002026/stock_market_regimes_2000_2026.csv",
        sep=",",
        encoding="ascii",
    )

    df_year = pd.read_csv(
        "/kaggle/input/stock-market-regimes-20002026/regime_by_year.csv",
        sep=",",
        encoding="ascii",
    )

    df_ticker = pd.read_csv(
        "/kaggle/input/stock-market-regimes-20002026/regime_analysis_by_ticker.csv",
        sep=",",
        encoding="ascii",
    )

    print("Datasets loaded successfully")
    print("df_stock:", df_stock.shape, "| df_year:", df_year.shape, "| df_ticker:", df_ticker.shape)

except FileNotFoundError as e:
    print("Arquivo não encontrado. Confere o path no /kaggle/input.")
    raise
except UnicodeDecodeError as e:
    print("Problema de encoding. Tenta encoding='utf-8' ou 'latin1'.")
    raise