import os
import json
from datetime import timedelta, datetime
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
        result = client.chat_postMessage(
            channel=channel['id'],
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "Danny Torrence left the following review for your property:"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "<https://example.com|Overlook Hotel> \n :star: \n Doors had too many axe holes, guest in room " +
                        "237 was far too rowdy, whole place felt stuck in the 1920s."
                    },
                    "accessory": {
                        "type": "image",
                        "image_url": "https://images.pexels.com/photos/750319/pexels-photo-750319.jpeg",
                        "alt_text": "Haunted hotel image"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": "*Average Rating*\n1.0"
                        }
                    ]
                }
            ]
        )
        print(result)
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
    print_all()


def calculate_deltas():
    now = datetime.now()
    today = format_date(now)
    print(f"today: {today}")
    previous_trading_day = format_date(get_previous_trading_day(now))
    print(f"prev session: {previous_trading_day}")

    holdings = get_holdings_for_dates(today, previous_trading_day)
    tickers = get_distinct_tickers(holdings)
    deltas = []
    for ticker in tickers:
        print(ticker)
        position_deltas = [h for h in holdings if h['ticker'] == ticker]
        if len(position_deltas) != 2:
            print("new position or exit position")
            continue
        current_position = [p for p in position_deltas if p['date'] == today][0]
        previous_position = [p for p in position_deltas if p['date'] == previous_trading_day][0]
        print(current_position)
        print(previous_position)
        share_delta = calculate_share_delta(current_position, previous_position)
        print(share_delta)
        print("\n\n")
        deltas.append([ticker, share_delta])
    return pd.DataFrame(deltas, columns=["ticker", "share_delta"])


def calculate_share_delta(current_position, previous_position):
    current_shares = float(format_shares_float(current_position['shares']))
    previous_shares = float(format_shares_float(current_position['shares']))
    return current_shares - previous_shares


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


def get_holdings_for_dates(today, previous_trading_day):
    holdings_table = get_holdings_table()
    rows = holdings_table.scan(
        FilterExpression=Attr('date').eq(today) | Attr('date').eq(previous_trading_day)
    )
    return rows['Items']


def get_previous_trading_day(date):
    previous_trading_day = date - timedelta(days=1)
    friday_week_index = 4
    while datetime.weekday(previous_trading_day) >= friday_week_index:
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
