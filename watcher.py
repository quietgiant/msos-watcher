from decimal import Decimal
import json
import boto3
from numpy import NaN
import pandas as pd

MSOS_HOLDINGS_CSV_URL = "https://advisorshares.com/wp-content/uploads/csv/holdings/AdvisorShares_MSOS_Holdings_File.csv"
TABLE_NAME = "Holdings"

def main():
    update_holdings()

def update_holdings():
    holdings = pd.read_csv(MSOS_HOLDINGS_CSV_URL).rename(columns=lambda x: x.strip()).dropna(how="all")
    today_date = holdings.iloc[0]["Date"]
    print(today_date)

    for (index, position) in holdings.iterrows():
        ticker = position["Stock Ticker"]
        if ticker is NaN:
            ticker = position["Security Description"]
        row = {
            "date": today_date,
            "ticker": ticker,
            "shares": position["Shares/Par (Full)"],
            "price": position["Price (Base)"],
            "value": position["Traded Market Value (Base)"],
            "weight": position["Portfolio Weight %"]
        }
        data = json.loads(json.dumps(row), parse_float=Decimal)
        write(data)

def write(data):
    dynamodb = boto3.resource("dynamodb")
    holdings_table = dynamodb.Table(TABLE_NAME)
    holdings_table.put_item(Item=data)


if __name__ == "main":
    main()