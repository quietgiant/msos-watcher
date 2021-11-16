from datetime import timedelta, datetime
from decimal import Decimal
import json
import boto3
from boto3.dynamodb.conditions import Attr
import pandas as pd

MSOS_HOLDINGS_CSV_URL = "https://advisorshares.com/wp-content/uploads/csv/holdings/AdvisorShares_MSOS_Holdings_File.csv"
TABLE_NAME = "Holdings"


def main():
    # update_holdings()
    diff = calculate_deltas()


def update_holdings():
    holdings = pd.read_csv(MSOS_HOLDINGS_CSV_URL).rename(columns=lambda x: x.strip()).dropna(how="all")
    today_date = holdings.iloc[0]["Date"]
    print(today_date)
    for (index, position) in holdings.iterrows():
        ticker = get_ticker(position)
        row = {
            "date": today_date,
            "ticker": ticker,
            "shares": position["Shares/Par (Full)"],
            "price": position["Price (Base)"],
            "value": position["Traded Market Value (Base)"],
            "weight": position["Portfolio Weight %"]
        }
        data = json.loads(json.dumps(row), parse_float=Decimal)
        print(data)
        write(data)
    print_all()


def calculate_deltas():
    holdings = get_holdings_between_today_and_previous()
    for position in holdings:
        print(position)


def get_holdings_between_today_and_previous():
    holdings_table = get_holdings_table()
    now = datetime.now()
    today = format_date(now)
    print(f"today: {today}")
    previous_trading_day = format_date(get_previous_trading_day(now))
    print(f"prev session: {previous_trading_day}")
    rows = holdings_table.scan(
        FilterExpression=Attr('date').eq(today) | Attr('date').eq(previous_trading_day)
    )
    return rows['Items']


def get_previous_trading_day(date):
    previous_trading_day = date - timedelta(days=1)
    friday_week_index = 4
    while datetime.weekday(previous_trading_day) <= friday_week_index:
        previous_trading_day = previous_trading_day - timedelta(days=1)
    return previous_trading_day


def format_date(date):
    return datetime.strftime(date, '%m/%d/%Y')


def get_ticker(row):
    ticker = str(row["Stock Ticker"]).strip()
    return ticker if ticker != "" and ticker != "nan" else row["Security Description"]


def print_all():
    dynamodb = boto3.resource("dynamodb")
    holdings_table = dynamodb.Table(TABLE_NAME)
    all_rows = holdings_table.scan()
    print(all_rows)


def write(data):
    holdings_table = get_holdings_table()
    holdings_table.put_item(Item=data)


def get_holdings_table():
    dynamodb = boto3.resource("dynamodb")
    return dynamodb.Table(TABLE_NAME)


if __name__ == "__main__":
    main()