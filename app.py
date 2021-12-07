import os
import json
from datetime import date, timedelta, datetime
from decimal import Decimal
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Attr
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


# AWS_REIGON = os.environ['AWS_REGION']
# AWS_PRIVATE_KEY = os.environ['AWS_PRIVATE_KEY']
# AWS_SECRET_KEY = os.environ['AWS_SECRET_KEY']
SLACK_API_TOKEN = os.environ['SLACK_API_TOKEN']

MSOS_HOLDINGS_CSV_URL = "https://advisorshares.com/wp-content/uploads/csv/holdings/AdvisorShares_MSOS_Holdings_File.csv"
TABLE_NAME = "Holdings"
TARGET_CHANNEL_NAME = "msos-watcher"


def main():
    # update_holdings()
    diff = calculate_deltas()
    print(diff)
    post_message_to_slack(diff)


def post_message_to_slack(diff):
    client = WebClient(token=SLACK_API_TOKEN)
    response = client.conversations_list()
    conversations = response["channels"]
    channel = [c for c in conversations if c["name"] == TARGET_CHANNEL_NAME][0]
    try:
        now = datetime.now()
        previous_trading_day = get_previous_trading_day(now)
        trading_day_before_previous = get_previous_trading_day(previous_trading_day)

        ticker_output_col = ""
        share_delta_output_col = ""

        diff = diff.sort_values('share_delta', ascending=False)

        for (index, position) in diff.iterrows():
            ticker_output_col += f"{position['ticker']}\n"
            share_delta_output_col += concatenate_share_delta(position)

        print(ticker_output_col)
        print(share_delta_output_col)
        result = client.chat_postMessage(
            channel=channel['id'],           
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*MSOS Holdings*\n*Changes from {format_date(trading_day_before_previous)} to {format_date(previous_trading_day)}*"
                    },
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": "*Ticker*"
                        },
                        {
                            "type": "mrkdwn",
                            "text": "*Share Delta*"
                        },
                        {
                            "type": "mrkdwn",
                            "text": ticker_output_col
                        },
                        {
                            "type": "mrkdwn",
                            "text": share_delta_output_col
                        }
                    ]
                }
            ]
        )
    except SlackApiError as e:
        print(f"Slack API error: {e}")


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


def calculate_deltas():
    now = datetime.now()
    previous_trading_day = get_previous_trading_day(now)
    print(f"previous_trading_day: {previous_trading_day}")
    trading_day_before_previous = get_previous_trading_day(previous_trading_day)
    print(f"trading_day_before_previous: {trading_day_before_previous}")

    holdings = get_holdings_for_dates(previous_trading_day, trading_day_before_previous)
    tickers = get_distinct_tickers(holdings)
    deltas = []
    for ticker in tickers:
        print(ticker)
        position_deltas = [h for h in holdings if h['ticker'] == ticker]
        if len(position_deltas) != 2:
            print("new position or exit position")
            print(position_deltas[0]['shares'])
            continue
        current_position = [p for p in position_deltas if p['date'] == format_date(previous_trading_day)][0]
        previous_position = [p for p in position_deltas if p['date'] == format_date(trading_day_before_previous)][0]
        print(current_position)
        print(previous_position)
        share_delta = calculate_share_delta(current_position, previous_position)
        print(share_delta)
        print("\n\n")
        deltas.append([ticker, share_delta])
    return pd.DataFrame(deltas, columns=["ticker", "share_delta"])


def calculate_share_delta(current_position, previous_position):
    current_shares = float(format_shares_float(current_position['shares']))
    previous_shares = float(format_shares_float(previous_position['shares']))
    return current_shares - previous_shares


def concatenate_share_delta(position):
    cash_tickers = [
        "CASH", 
        "BLACKROCK TREASURY TRUST INSTL 62"
    ]
    if position["ticker"] in cash_tickers:
        return f"{money_str(position['share_delta'])}\n"
    return f"{share_str(position['share_delta'])}\n"


def money_str(s):
    if s is None:
        return 'N/A'
    return "${:,.2f}".format(float(s))

def share_str(s):
    if s is None:
        return 'N/A'
    return "{:d}".format(int(s))


def format_shares_float(shares):
    replacement_chars = [",", "(", ")"]
    for c in replacement_chars:
        shares = shares.replace(c, "")
    return shares


def get_distinct_tickers(holdings):
    tickers = []
    for position in holdings:
        ticker = position['ticker']
        if ticker not in tickers:
            tickers.append(ticker)
    return tickers


def get_holdings_for_dates(day1, day2):
    holdings_table = get_holdings_table()
    rows = holdings_table.scan(
        FilterExpression=Attr('date').eq(format_date(day1)) | Attr('date').eq(format_date(day2))
    )
    return rows['Items']


def get_previous_trading_day(date):
    print(date)
    previous_trading_day = date - timedelta(days=1)
    friday_week_index = 4
    while datetime.weekday(previous_trading_day) > friday_week_index:
        previous_trading_day = previous_trading_day - timedelta(days=1)
    return previous_trading_day


def format_date(date):
    return datetime.strftime(date, '%m/%-d/%Y')


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
