#!/usr/bin/env python3
"""
Script to process user-rewards vault logs and create CSV files.
"""
import os
import time
from typing import Dict, List, Optional
import pandas as pd
from tqdm import tqdm

import config
from scripts.utils import *


def decode_user_rewards_vault_log(log: Dict) -> Dict:
    """
    Decode a user-rewards vault log entry.
    
    Args:
        w3: Web3 instance
        log: Log entry to decode
        
    Returns:
        Decoded log entry
    """
    # This function will be implemented by the user
    # It should decode the log entry and return a dictionary with the decoded data
    # For now, return a placeholder
    event_signature = '0x' + log['topics'][0].hex() if isinstance(log['topics'][0], bytes) else log['topics'][0]
    log_data = log['topics']
    # Process the log based on the event signature
    if event_signature == config.REWARDS_VAULT.event_signatures['Staked']:
        amt = int(log['data'].hex(), 16) * 1e-18  # Convert to BGT units
    elif event_signature == config.REWARDS_VAULT.event_signatures['Withdrawn']:
        # Negative amount for undelegations
        amt = -int(log['data'].hex(), 16) * 1e-18
    else:
        raise ValueError(f"Unknown event signature: {event_signature}")

    return {
        "tx_hash": "0x"+ log.transactionHash.hex() if isinstance(log.transactionHash, bytes) else log.transactionHash,
        "block_number": log.blockNumber,
        "user_address": '0x' + log_data[1].hex()[-40:] if isinstance(log_data[1], bytes) else log_data[1],
        "rv_address": log.address,
        "amount": amt,
        "event_type": event_signature
    }


def decode_all_user_rewards_vault_logs(raw_logs: List[Dict]) -> pd.DataFrame:
    """
    Decode all user-rewards vault logs sequentially.
    
    Args:
        raw_logs: List of raw log entries
        
    Returns:
        DataFrame with decoded logs
    """
    decoded_logs = []
    for log in tqdm(raw_logs, desc="Decoding logs"):
        try:
            decoded_logs.append(decode_user_rewards_vault_log(log))
        except Exception as e:
            print(f"Error decoding log: {e}")
            continue
    
    return pd.DataFrame(decoded_logs)


def process_urv_log_for_multiprocessing(log: Dict) -> Dict:
    """
    Process a single log entry for multiprocessing.
    
    Args:
        log: Log entry to decode
        
    Returns:
        Decoded log entry
    """
    try:
        return decode_user_rewards_vault_log(log)
    except Exception as e:
        print(f"Error decoding log: {e}")
        return None


def decode_all_user_rewards_vault_logs_multiprocessing(raw_logs: List[Dict], num_processes: int = None) -> pd.DataFrame:
    """
    Decode all user-rewards vault logs using multiprocessing for improved performance.
    
    Args:
        raw_logs: List of raw log entries
        num_processes: Number of processes to use (defaults to CPU count)
        
    Returns:
        DataFrame with decoded logs
    """
    import multiprocessing
    from tqdm.auto import tqdm
    
    if num_processes is None:
        num_processes = multiprocessing.cpu_count()
    
    print(f"Decoding {len(raw_logs)} logs using {num_processes} processes...")
    
    # Use multiprocessing to decode logs
    with multiprocessing.Pool(processes=num_processes) as pool:
        # Use tqdm with imap_unordered for better performance
        # imap_unordered processes results as they become available, regardless of input order
        results = list(tqdm(
            pool.imap_unordered(process_urv_log_for_multiprocessing, raw_logs),
            total=len(raw_logs),
            desc="Decoding logs"
        ))
    
    # Filter out None values (failed decoding)
    results = [r for r in results if r is not None]
    
    return pd.DataFrame(results)


def process_user_rewards_vault_logs(input_path: str, output_dir: Optional[str] = None, use_multiprocessing: bool = True, num_processes: Optional[int] = None):
    """
    Process user-rewards vault logs and save the decoded logs to a CSV file.
    
    Args:
        input_path: Path to the raw logs file
        output_dir: Directory to save the decoded logs (defaults to processed_data/user_rewards_vault/decoded_logs)
        use_multiprocessing: Whether to use multiprocessing for decoding (defaults to True)
        num_processes: Number of processes to use for multiprocessing (defaults to CPU count)
    """
    # Load the raw logs
    print(f"Loading raw logs from {input_path}...")
    logs = load_raw_logs(input_path)
    print(f"Loaded {len(logs)} logs")
    
    # Determine the output directory
    if output_dir is None:
        output_dir = "processed_data/user_rewards_vault/decoded_logs"
    
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Decode the logs
    if use_multiprocessing:
        decoded_logs_df = decode_all_user_rewards_vault_logs_multiprocessing(logs, num_processes)
    else:
        decoded_logs_df = decode_all_user_rewards_vault_logs(logs)
    
    # Generate the output filename
    base_filename = os.path.basename(input_path)
    output_filename = os.path.splitext(base_filename)[0] + "_decoded.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    # Save the decoded logs to a CSV file
    print(f"Saving {len(decoded_logs_df)} decoded logs to {output_path}...")
    decoded_logs_df.to_csv(output_path, index=False)
    print(f"Decoded logs saved to {output_path}")
    
    return output_path


def calculate_user_rv_state(decoded_logs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the current state of user-rewards vault relationships.
    
    Args:
        decoded_logs_df: DataFrame with decoded logs
        
    Returns:
        DataFrame with user-rewards vault state
    """
    # Group by user and reward vault to calculate net stake
    user_rv_state = decoded_logs_df.groupby(['user_address', 'rv_address'])['amount'].sum().reset_index()
    
    # Filter out users with zero or negative stake
    user_rv_state = user_rv_state[user_rv_state['amount'] > 0]
    
    # Calculate total stake per reward vault
    rv_totals = user_rv_state.groupby('rv_address')['amount'].sum().reset_index()
    rv_totals.rename(columns={'amount': 'total_stake'}, inplace=True)
    
    # Merge to get total stake for each reward vault
    user_rv_state = pd.merge(user_rv_state, rv_totals, on='rv_address')
    
    # Calculate user share of each reward vault
    user_rv_state['user_rv_share'] = user_rv_state['amount'] / user_rv_state['total_stake']
    
    # Add timestamp
    user_rv_state['timestamp'] = int(time.time())
    
    return user_rv_state


def save_user_rv_state(user_rv_state: pd.DataFrame, timestamp: Optional[int] = None):
    """
    Save the user-rewards vault state to a CSV file.
    
    Args:
        user_rv_state: DataFrame with user-rewards vault state
        timestamp: Unix timestamp (defaults to current time)
    """
    if timestamp is None:
        timestamp = int(time.time())
    
    # Format timestamp for filename
    formatted_timestamp = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d_%H:%M:%S")
    
    # Create directory if it doesn't exist
    states_dir = os.path.join(config.PROCESSED_DATA_DIR['user_rewards_vault'], 'states')
    os.makedirs(states_dir, exist_ok=True)
    
    # Save to CSV file
    filename = f"{states_dir}/user_rewards_vault_state_{formatted_timestamp}.csv"
    user_rv_state.to_csv(filename, index=False)
    
    print(f"Saved user-rewards vault state to {filename}")
    return filename


