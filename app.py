try:
    import os
    import json
    from datetime import timedelta, datetime
    from decimal import Decimal
    import pandas as pd
    import boto3
    from boto3.dynamodb.conditions import Attr
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    from dotenv import load_dotenv
except Exception as ex:
    print("Error Imports : {} ".format(ex))


load_dotenv()
print(os.environ)

MSOS_HOLDINGS_CSV_URL = "https://advisorshares.com/wp-content/uploads/csv/holdings/AdvisorShares_MSOS_Holdings_File.csv"
TABLE_NAME = "Holdings"
SLACK_TARGET_CHANNEL_NAME = "msos-watcher"
SLACK_API_TOKEN = os.environ['SLACK_API_TOKEN']


def handler(event, context):
    main()


def main():
    update_holdings()
    diff = calculate_deltas()
    post_message_to_slack(diff)


def post_message_to_slack(diff):
    client = WebClient(token=SLACK_API_TOKEN)
    channels = client.conversations_list()["channels"]
    slack_channel = [c for c in channels if c["name"] == SLACK_TARGET_CHANNEL_NAME][0]
    try:
        now = datetime.now()
        previous_trading_day = get_previous_trading_day(now)
        ticker_output_col = ""
        share_delta_output_col = ""
        diff = diff.sort_values('share_delta', ascending=False)

        for (index, position) in diff.iterrows():
            ticker_output_col += f"{position['ticker']}\n"
            share_delta_output_col += concatenate_share_delta(position)

        result = client.chat_postMessage(
            channel=slack_channel['id'],
            text="MSOS Holding Changes",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*MSOS Holdings*\n*Changes from {format_date(previous_trading_day)} to {format_date(now)}*"
                    },
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": "*Ticker*"
                        },
                        {
                            "type": "mrkdwn",
                            "text": "*Delta*"
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
        if valid_row(data):
            print(data)
            write(data)


def calculate_deltas():
    now = datetime.now()
    previous_trading_day = get_previous_trading_day(now)

    holdings = get_holdings_for_dates(now, previous_trading_day)
    tickers = get_distinct_tickers(holdings)
    deltas = []
    for ticker in tickers:
        position_deltas = [h for h in holdings if h['ticker'] == ticker]
        current_position = 0
        previous_position = 0
        if len(position_deltas) != 2:
            print(f"new position or exit position for {ticker}")
            print(position_deltas[0]['shares'])
            current_position = position_deltas[0]
        else:
            current_position = [p for p in position_deltas if p['date'] == format_date(previous_trading_day)][0]
            previous_position = [p for p in position_deltas if p['date'] == format_date(previous_trading_day)][0]
        share_delta = calculate_share_delta(current_position, previous_position)
        deltas.append([ticker, share_delta])
    return pd.DataFrame(deltas, columns=["ticker", "share_delta"])


def calculate_share_delta(current_position, previous_position):
    current_shares = float(format_shares_float(current_position['shares']))
    previous_shares = 0
    if (previous_position):
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
    num = int(float(s))
    status = '+'
    if num < 0:
        status = '-'
    result = "{:,d}".format(num)
    return result


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
    previous_trading_day = date - timedelta(days=1)
    friday_week_index = 4
    while datetime.weekday(previous_trading_day) > friday_week_index:
        previous_trading_day = previous_trading_day - timedelta(days=1)
    return previous_trading_day


def format_date(date):
    return datetime.strftime(date, '%-m/%-d/%Y')


def get_ticker(row):
    ticker = str(row["Stock Ticker"]).strip()
    return ticker if ticker != "" and ticker != "nan" else row["Security Description"]


def valid_row(row):
    if '-' in row['shares']:
        return False
    return True


def print_all():
    dynamodb = get_dynamodb()
    holdings_table = dynamodb.Table(TABLE_NAME)
    all_rows = holdings_table.scan()
    print(all_rows)


def write(data):
    holdings_table = get_holdings_table()
    holdings_table.put_item(Item=data)


def get_holdings_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table(TABLE_NAME)


def get_dynamodb():
    return boto3.resource("dynamodb",
                          aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
                          aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])


if __name__ == "__main__":
    main()
