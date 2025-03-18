#!/usr/bin/env python3
"""
Script to process validator-delegator logs and create CSV files.
"""
import argparse
import json
import os
import time
from typing import Dict, List, Optional
import requests
from web3 import Web3
import pandas as pd
from tqdm import tqdm


import config
from scripts.utils import *


def decode_validator_delegator_log(log: Dict) -> Dict:

    # Convert the topic to a string with '0x' prefix for comparison
    event_signature = '0x' + \
        log['topics'][0].hex() if isinstance(
            log['topics'][0], bytes) else log['topics'][0]
    log_data = log['topics']
    # Process the log based on the event signature
    if event_signature == config.BGT_TOKEN.event_signatures['Delegation']:
        amt = int(log['data'].hex(), 16) * 1e-18  # Convert to BGT units
    elif event_signature == config.BGT_TOKEN.event_signatures['Undelegation']:
        # Negative amount for undelegations
        amt = -int(log['data'].hex(), 16) * 1e-18
    else:
        raise ValueError(f"Unknown event signature: {event_signature}")

    # Create the decoded log entry
    decoded_log = {
        "tx_hash": '0x' + log['transactionHash'].hex() if isinstance(log['transactionHash'], bytes) else log['transactionHash'],
        "block_number": log['blockNumber'],
        "validator_address": '0x' + log_data[3].hex() if isinstance(log_data[3], bytes) else log_data[3],
        "delegator_address": '0x' + log_data[2].hex()[-40:] if isinstance(log_data[2], bytes) else log_data[1],
        "amount_bgt_delegated": amt,
    }
    return decoded_log


def decode_all_validator_delegator_logs(raw_logs: List[Dict]) -> pd.DataFrame:
    decoded_logs = []
    for log in tqdm(raw_logs, desc="Decoding logs"):
        try:
            decoded_logs.append(decode_validator_delegator_log(log))
        except Exception as e:
            print(f"Error decoding log: {e}")
            continue
    return pd.DataFrame(decoded_logs)


def process_vd_log_for_multiprocessing(log):
    """
    Process a single validator-delegator log entry for multiprocessing.
    
    Args:
        args: Tuple containing (w3, log) where:
            - w3: Web3 instance
            - log: Log entry to decode
        
    Returns:
        Decoded log entry or None if decoding fails
    """
    
    try:
        return decode_validator_delegator_log(log)
    except Exception as e:
        print(f"Error decoding log: {e}")
        return None


def decode_all_validator_delegator_logs_multiprocessing(raw_logs: List[Dict], num_processes: int = None) -> pd.DataFrame:
    """
    Decode all validator-delegator logs using multiprocessing for improved performance.
    
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
            pool.imap_unordered(process_vd_log_for_multiprocessing, raw_logs),
            total=len(raw_logs),
            desc="Decoding logs"
        ))
    
    # Filter out None values (failed decoding)
    results = [r for r in results if r is not None]
    
    return pd.DataFrame(results)
