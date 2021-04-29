import yaml
import cbpro
import pandas as pd

from time import sleep
from datetime import datetime
from os.path import isfile

with open("config.yaml", "r") as file:
    config = yaml.full_load(file)

auth_client = cbpro.AuthenticatedClient(
    config["key"], config["secret"], config["passphrase"])

products = auth_client.get_products()
download_list = []
for i in range(0, len(products)):
    ticker = products[i]["id"]
    traded = not products[i]['trading_disabled']
    if traded and ticker.endswith("USD"):
        download_list.append(ticker)

while True:

    for ticker in download_list:

        try:
            data = auth_client.get_product_historic_rates(
                ticker, granularity=60)
            now = datetime.now()
            print(f"{datetime.strftime(now, '%H:%M:%S')}: {ticker} downloaded.")
            if not data:
                continue

            df = pd.DataFrame(data=reversed(data), columns=[
                "datetime", "low", "high", "open", "close", "volume"])
            df["datetime"] = pd.to_datetime(df["datetime"], unit='s')
            df = df.sort_values(by=["datetime"])
            df["modified"] = now

            simple_path = f"data/data_simple/{ticker}.csv"
            if isfile(simple_path):
                df_old = pd.read_csv(simple_path)
                df_old["datetime"] = pd.to_datetime(df_old["datetime"])
                df_old = df_old[df_old["datetime"] < df["datetime"][0]]
                df_write = df_old.append(df)
                df_write.to_csv(simple_path, index=False)
            else:
                df.to_csv(simple_path, index=False)

            full_path = f"data/data_full/{ticker}.csv"

            if isfile(full_path):
                df_old = pd.read_csv(full_path)
                df_old["datetime"] = pd.to_datetime(df_old["datetime"])
                df_old["modified"] = pd.to_datetime(df_old["modified"])
                df_write = df_old.append(df)
                df_write = df_write.groupby(
                    by=["datetime", "low", "high", "open", "close", "volume"])
                df_write = df_write.agg({"modified": "min"})
                df_write = df_write.reset_index()
                df_write = df_write.sort_values(by=["datetime", "modified"])
                df_write.to_csv(full_path, index=False)
            else:
                df.to_csv(full_path, index=False)

        except Exception:
            print(f"{datetime.strftime(now, '%H:%M:%S')}: {ticker} failed!")
            pass

        sleep(0.5)

    sleep(30)
