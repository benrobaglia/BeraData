"""
Utility functions for the Berachain data processing system.
"""
import csv
import json
import os
import pickle
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
import shutil

from web3 import Web3
import requests

import config


def setup_web3():
    """Initialize and return a Web3 instance."""
    return Web3(Web3.HTTPProvider(config.RPC_ENDPOINT))


def get_contract_abi(abi_url: str) -> dict:
    """Fetch contract ABI from URL."""
    response = requests.get(abi_url)
    return response.json()


def get_contract(web3: Web3, address: str, abi_url: str):
    """Initialize and return a contract instance."""
    abi = get_contract_abi(abi_url)
    return web3.eth.contract(address=address, abi=abi)


def get_logs(web3: Web3, contract_address: str | List[str], event_signature: str | List[str], 
             from_block: int = 0, to_block: Optional[int] = None) -> List[Dict]:
    """
    Fetch logs for one or multiple event signatures and contract addresses in batches to avoid RPC limitations.
    
    Args:
        web3: Web3 instance
        contract_address: Single contract address or list of contract addresses to filter logs
        event_signature: Single event signature or list of event signatures to filter logs
        from_block: Starting block number
        to_block: Ending block number (defaults to latest block)
        
    Returns:
        List of log entries
    """
    if to_block is None:
        to_block = web3.eth.get_block("latest")["number"]
    
    all_logs = []
    current_block = from_block
    end_block = to_block
    batch_size = 10000  # Adjust based on RPC limitations
    
    # Convert single event signature to list for consistent handling
    if isinstance(event_signature, str):
        event_signatures = [event_signature]
    else:
        event_signatures = event_signature
    
    while current_block < end_block:
        batch_end = min(current_block + batch_size, end_block)
        try:
            logs = web3.eth.get_logs({
                "fromBlock": current_block,
                "toBlock": batch_end,
                "address": contract_address,  # This can be a single address or a list of addresses
                "topics": [[sig for sig in event_signatures]]
            })
            all_logs.extend(logs)
        except Exception as e:
            print(f"Error fetching logs from {current_block} to {batch_end}: {e}")
            # Reduce batch size on error
            batch_size = max(batch_size // 2, 1000)
            continue
        
        current_block = batch_end + 1
    
    return all_logs


class Web3Encoder(json.JSONEncoder):
    """Custom JSON encoder for Web3 objects."""
    def default(self, obj):
        # Handle HexBytes objects
        if hasattr(obj, 'hex'):
            return obj.hex()
        # Handle AttributeDict objects
        if hasattr(obj, '__dict__'):
            return {key: value for key, value in obj.__dict__.items() if not key.startswith('_')}
        # Handle other non-serializable objects
        if isinstance(obj, bytes):
            return obj.hex()
        # Let the base class handle other types
        return super().default(obj)


def save_raw_logs(logs: List[Dict], data_type: str, timestamp: int = None):
    """
    Save raw logs to a pickle file.
    
    Args:
        logs: List of log entries
        data_type: Type of data (validator_delegator, user_rewards_vault)
        timestamp: Unix timestamp (defaults to current time)
    """
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(" ", "_")
    
    # Create directory if it doesn't exist
    os.makedirs(config.RAW_LOGS_DIR[data_type], exist_ok=True)
    
    # Save to pickle file to preserve object structure
    filename = f"{config.RAW_LOGS_DIR[data_type]}/logs_{timestamp}.pkl"
    with open(filename, 'wb') as f:
        pickle.dump(logs, f, protocol=pickle.HIGHEST_PROTOCOL)
    
    print(f"Saved {len(logs)} raw logs to {filename}")
    return filename


def load_raw_logs(filename: str) -> List[Dict]:
    """
    Load raw logs from a file.
    
    Supports both pickle (.pkl) and JSON (.json) files for backward compatibility.
    
    Args:
        filename: Path to the log file
        
    Returns:
        List of log entries
    """
    if filename.endswith('.pkl'):
        # Load from pickle file
        with open(filename, 'rb') as f:
            return pickle.load(f)
    else:
        # Load from JSON file (for backward compatibility)
        with open(filename, 'r') as f:
            return json.load(f)


def save_csv_data(data: List[Dict], columns: List[str], data_type: str, 
                  is_latest: bool = True, timestamp: int = None):
    """
    Save processed data to a CSV file.
    
    Args:
        data: List of dictionaries with data
        columns: CSV column names
        data_type: Type of data (validator_delegator, user_rewards_vault, rewards_distribution)
        is_latest: Whether this is the latest data (True) or historical (False)
        timestamp: Unix timestamp (defaults to current time)
    """
    if timestamp is None:
        timestamp = int(time.time())
    
    # Create directories if they don't exist
    base_dir = config.PROCESSED_DATA_DIR[data_type]
    os.makedirs(base_dir, exist_ok=True)
    os.makedirs(os.path.join(base_dir, "historical"), exist_ok=True)
    
    # Determine filename
    if is_latest:
        filename = f"{base_dir}/latest.csv"
        # Backup current latest to historical before overwriting
        if os.path.exists(filename):
            historical_filename = f"{base_dir}/historical/data_{timestamp}.csv"
            shutil.copy2(filename, historical_filename)
            print(f"Backed up previous latest data to {historical_filename}")
    else:
        filename = f"{base_dir}/historical/data_{timestamp}.csv"
    
    # Write CSV file
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Saved {len(data)} records to {filename}")
    return filename


def load_csv_data(data_type: str, is_latest: bool = True, timestamp: int = None) -> List[Dict]:
    """
    Load data from a CSV file.
    
    Args:
        data_type: Type of data (validator_delegator, user_rewards_vault, rewards_distribution)
        is_latest: Whether to load the latest data (True) or historical (False)
        timestamp: Unix timestamp for historical data
        
    Returns:
        List of dictionaries with data
    """
    base_dir = config.PROCESSED_DATA_DIR[data_type]
    
    if is_latest:
        filename = f"{base_dir}/latest.csv"
    else:
        if timestamp is None:
            raise ValueError("Timestamp is required for historical data")
        filename = f"{base_dir}/historical/data_{timestamp}.csv"
    
    if not os.path.exists(filename):
        return []
    
    with open(filename, 'r', newline='') as f:
        reader = csv.DictReader(f)
        return list(reader)


def get_block_timestamp(web3: Web3, block_number: int) -> int:
    """Get the timestamp of a block."""
    block = web3.eth.get_block(block_number)
    return block.timestamp


def format_timestamp(timestamp: int) -> str:
    """Format a Unix timestamp as a human-readable string."""
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
