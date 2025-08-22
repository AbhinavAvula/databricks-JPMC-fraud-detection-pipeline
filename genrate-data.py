import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
import faker

fake = faker.Faker()

# --- Parameters ---
NUM_CUSTOMERS = 50
NUM_TRANSACTIONS = 2000
HOME_COUNTRY = "US"
CURRENCIES = ["USD", "EUR", "GBP", "INR"]

# Merchant catalog (simplified realistic set)
MERCHANTS = [
    {"merchant_id": "M001", "merchant_name": "Starbucks", "mcc": 5812, "city": "New York", "country": "US", "lat": 40.7128, "lon": -74.0060},
    {"merchant_id": "M002", "merchant_name": "Amazon", "mcc": 5942, "city": "Seattle", "country": "US", "lat": 47.6062, "lon": -122.3321},
    {"merchant_id": "M003", "merchant_name": "McDonalds", "mcc": 5814, "city": "Chicago", "country": "US", "lat": 41.8781, "lon": -87.6298},
    {"merchant_id": "M004", "merchant_name": "British Airways", "mcc": 4511, "city": "London", "country": "GB", "lat": 51.4700, "lon": -0.4543},
    {"merchant_id": "M005", "merchant_name": "Reliance Retail", "mcc": 5411, "city": "Mumbai", "country": "IN", "lat": 19.0760, "lon": 72.8777},
]

ENTRY_CHANNEL_MAP = {
    "Chip": "POS",
    "Swipe": "POS",
    "Contactless": "POS",
    "Online": "Online",
    "ATM": "ATM"
}

# --- Generate customers ---
customers = []
for i in range(NUM_CUSTOMERS):
    cust_id = f"CUST{i+1:04d}"
    account_id = f"ACC{i+1:05d}"
    card_last4 = str(random.randint(1000, 9999))
    card_network = random.choice(["VISA", "MASTERCARD", "AMEX"])
    customers.append({
        "customer_id": cust_id,
        "account_id": account_id,
        "card_number_masked": f"****{card_last4}",
        "card_network": card_network
    })

# --- Generate transactions ---
transactions = []
start_date = datetime(2023, 1, 1)

for _ in range(NUM_TRANSACTIONS):
    cust = random.choice(customers)
    merchant = random.choice(MERCHANTS)

    # Transaction date & time
    txn_date = start_date + timedelta(days=random.randint(0, 600))
    txn_time = fake.time(pattern="%H:%M:%S")
    posting_date = txn_date + timedelta(days=random.choice([1, 2]))

    # Entry mode & channel
    entry_mode = random.choice(list(ENTRY_CHANNEL_MAP.keys()))
    channel = ENTRY_CHANNEL_MAP[entry_mode]

    # Amount logic
    base_amount = {
        5812: random.uniform(3, 15),   # Coffee
        5942: random.uniform(20, 200), # Online shopping
        5814: random.uniform(5, 30),   # Fast food
        4511: random.uniform(200, 900),# Flights
        5411: random.uniform(10, 100), # Grocery
    }[merchant["mcc"]]
    amount = round(base_amount, 2)

    # Refund logic
    txn_type = random.choices(["Purchase", "Refund"], weights=[0.9, 0.1])[0]
    if txn_type == "Refund":
        amount = -amount

    # Currency & exchange rate
    if merchant["country"] == HOME_COUNTRY:
        currency = "USD"
        exchange_rate = 1.0
    else:
        currency = random.choice([c for c in CURRENCIES if c != "USD"])
        exchange_rate = round(random.uniform(0.7, 1.3), 2)

    # Merchant fee ~ 1.5%
    merchant_fee = round(abs(amount) * random.uniform(0.005, 0.03), 2)

    # Authorization code
    auth_code = ''.join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=6))

    # Build record
    record = {
        "transaction_id": str(uuid.uuid4()),
        "card_number_masked": cust["card_number_masked"],
        "account_id": cust["account_id"],
        "customer_id": cust["customer_id"],
        "card_network": cust["card_network"],
        "transaction_date": txn_date.strftime("%Y-%m-%d"),
        "transaction_time": txn_time,
        "posting_date": posting_date.strftime("%Y-%m-%d"),
        "transaction_type": txn_type,
        "channel": channel,
        "amount": amount,
        "currency": currency,
        "exchange_rate": exchange_rate,
        "merchant_fee": merchant_fee,
        "authorization_code": auth_code,
        "merchant_id": merchant["merchant_id"],
        "merchant_name": merchant["merchant_name"],
        "merchant_category_code": merchant["mcc"],
        "merchant_city": merchant["city"],
        "merchant_country": merchant["country"],
        "terminal_id": f"TERM{random.randint(1000,9999)}",
        "transaction_country": merchant["country"],
        "transaction_latitude": merchant["lat"],
        "transaction_longitude": merchant["lon"],
        "entry_mode": entry_mode
    }
    transactions.append(record)

# --- Save to parquet & csv ---
df = pd.DataFrame(transactions)

df.to_csv("transactions_raw.csv", index=False)

print(df.head(5))
