import requests
import time
from dotenv import load_dotenv
import os
import pandas as pd
import matplotlib.pyplot as plt

load_dotenv()



BASE_URL = "https://api.dune.com/api/v1/"


def execute_dune_query(query_id, headers, parameters={}):
    url = f"{BASE_URL}query/{query_id}/execute"
    response = requests.post(
        url, json={"parameters": parameters}, headers=headers)

    if response.status_code == 200:
        execution_id = response.json().get("execution_id")
        print(f"Query execution started. Execution ID: {execution_id}")
        return execution_id
    else:
        print(f"Error: {response.json()}")
        return None

# Function to fetch query results


def fetch_query_results(execution_id, headers):
    url = f"{BASE_URL}execution/{execution_id}/results"

    while True:
        response = requests.get(url, headers=headers)
        data = response.json()

        if response.status_code == 200:
            state = data.get("state")
            if state == "QUERY_STATE_COMPLETED":
                return data["result"]["rows"]
            elif state in ["QUERY_STATE_RUNNING", "QUERY_STATE_EXECUTING", "QUERY_STATE_PENDING"]:
                print(f"Query still running ({state}), waiting...")
                time.sleep(5)  # Wait 5 seconds before checking again
            else:
                print(f"Query failed or cancelled: {state}")
                return None
        else:
            print(f"Error fetching results: {data}")
            return None
        

def prices_to_dataframe(data):
    records = []

    for token_entry in data['data']['tokenGetHistoricalPrices']:
        token = token_entry['address']
        for price_entry in token_entry['prices']:
            records.append({
                'timestamp': int(price_entry['timestamp']),  # convert to int if needed
                'token_address': token,
                'price': price_entry['price']
            })

    return pd.DataFrame(records)



if __name__ == "__main__":
    dune_api_key = os.getenv("DUNE_API_KEY")
    query_id = 4912143
    headers = {"x-dune-api-key": dune_api_key}
    execution_id = execute_dune_query(query_id, headers=headers)
    results = fetch_query_results(execution_id, headers=headers)
    if results:
        df = pd.DataFrame(results)

    token_addresses = df['token_address'].unique()
    
    query_price = """
    query GetTokenHistoricalPrices($addresses: [String!]!) {
    tokenGetHistoricalPrices(
        addresses: $addresses
        chain: BERACHAIN
        range: SEVEN_DAY
    ) {
        address
        chain
        prices {
        price
        timestamp
        updatedAt
        updatedBy
        }
    }
    }
    """

    variables = {
        "addresses": list(token_addresses)
    }
    
    response = requests.post(
        "https://api.berachain.com/",
        json={
            "query": query_price,
            "variables": variables
        },
        headers={"Content-Type": "application/json"}
    )

    prices = response.json()

    prices_formatted = prices_to_dataframe(prices)
    
    df['block_time'] = pd.to_datetime(df['block_time'], utc=True)

    df['block_time_rounded'] = df['block_time'].dt.round('H')
    df['timestamp'] = df['block_time_rounded'].astype('int64') // 10**9

    df = df.merge(prices_formatted, how='left', left_on=['timestamp', 'token_address'], right_on=['timestamp', 'token_address'], suffixes=('', '_price'))
    df = df[['timestamp', 'pubkey', 'token_address', 'amount', 'bgt_emitted', 'price']]
    df.loc[:, ['amount', 'bgt_emitted']] = df.loc[:, ['amount', 'bgt_emitted']].astype(float) * 1e-18
    df['amount_usd'] = df['amount'] * df['price']
    df['timestamp_df'] = pd.to_datetime(df['timestamp'], unit='s')
    df_agg = df.groupby('timestamp_df').agg({'amount_usd': 'sum', 'bgt_emitted': 'sum'}).reset_index()
    df_agg['incentives_per_bgt'] = df_agg['amount_usd'] / df_agg['bgt_emitted']
    
    plt.figure(figsize=(12, 6))
    df_agg.set_index('timestamp_df')['incentives_per_bgt'].plot()

    plt.title('Incentives Per BGT Per Hour', fontsize=16)
    plt.xlabel('Timestamp', fontsize=14)
    plt.ylabel('Incentives ($) Per BGT', fontsize=14)
    plt.xticks(fontsize=12) 
    plt.yticks(fontsize=12) 
    plt.legend([df.pubkey.iloc[0]], fontsize=12)  # Add a legend

    plt.grid(True)  # Add a grid for better readability
    plt.show()