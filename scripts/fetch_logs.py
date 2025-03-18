#!/usr/bin/env python3
"""
Script to fetch logs from the Berachain blockchain and save them as raw logs.
"""
import os
import multiprocessing
from typing import Optional, List, Dict, Tuple
import pandas as pd
from tqdm.auto import tqdm

from web3 import Web3

import config
from scripts.utils import setup_web3, get_logs, save_raw_logs, load_csv_data, load_raw_logs
from scripts.process_validator_delegator import decode_validator_delegator_log, decode_all_validator_delegator_logs
from scripts.process_user_rewards_vault import process_user_rewards_vault_logs


def process_log(log: Dict) -> Dict:
    """
    Process a single log entry.
    This function creates a new Web3 instance for each worker process.
    
    Args:
        log: Log entry to decode
        
    Returns:
        Decoded log entry
    """
    # Create a new Web3 instance for each worker process
    web3 = setup_web3()
    return decode_validator_delegator_log(web3, log)


def decode_logs_with_multiprocessing(logs: List[Dict], num_processes: int = None) -> pd.DataFrame:
    """
    Decode logs using multiprocessing for improved performance.
    
    Args:
        logs: List of log entries to decode
        num_processes: Number of processes to use (defaults to CPU count)
        
    Returns:
        DataFrame with decoded logs
    """
    if num_processes is None:
        num_processes = multiprocessing.cpu_count()
    
    print(f"Decoding {len(logs)} logs using {num_processes} processes...")
    
    # Use multiprocessing to decode logs
    with multiprocessing.Pool(processes=num_processes) as pool:
        # Use tqdm with imap_unordered for better performance
        # imap_unordered processes results as they become available, regardless of input order
        results = list(tqdm(
            pool.imap_unordered(process_log, logs),
            total=len(logs),
            desc="Decoding logs"
        ))
    
    return pd.DataFrame(results)


def process_raw_logs_file(input_path: str, output_dir: str = None, num_processes: int = None):
    """
    Process a raw logs file and save the decoded logs to a CSV file.
    
    Args:
        input_path: Path to the raw logs file
        output_dir: Directory to save the decoded logs (defaults to processed_data/validator_delegator/decoded_logs)
        num_processes: Number of processes to use for decoding (defaults to CPU count)
    """
    # Load the raw logs
    print(f"Loading raw logs from {input_path}...")
    logs = load_raw_logs(input_path)
    print(f"Loaded {len(logs)} logs")
    
    # Determine the output directory
    if output_dir is None:
        if "validator_delegator" in input_path:
            output_dir = "processed_data/validator_delegator/decoded_logs"
        elif "user_rewards_vault" in input_path:
            output_dir = "processed_data/user_rewards_vault/decoded_logs"
        else:
            output_dir = "processed_data/decoded_logs"
    
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Decode the logs using multiprocessing
    decoded_logs_df = decode_logs_with_multiprocessing(logs, num_processes)
    
    # Generate the output filename
    base_filename = os.path.basename(input_path)
    output_filename = os.path.splitext(base_filename)[0] + "_decoded.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    # Save the decoded logs to a CSV file
    print(f"Saving {len(decoded_logs_df)} decoded logs to {output_path}...")
    decoded_logs_df.to_csv(output_path, index=False)
    print(f"Decoded logs saved to {output_path}")
    
    return output_path


def get_last_processed_block(data_type: str) -> int:
    """
    Get the last processed block number from the latest data.
    
    Args:
        data_type: Type of data (validator_delegator, user_rewards_vault, rewards_distribution)
        
    Returns:
        Last processed block number, or 0 if no data is available
    """
    # Load the latest data
    data = load_csv_data(data_type, is_latest=True)
    
    if not data:
        return 0
    
    # Find the maximum block number
    max_block = 0
    for record in data:
        try:
            block_number = int(record.get('block_number', 0))
            max_block = max(max_block, block_number)
        except (ValueError, TypeError):
            continue
    
    return max_block


def fetch_validator_delegator_logs(web3: Web3, from_block: Optional[int] = None, 
                                  to_block: Optional[int] = None, 
                                  timestamp: Optional[int] = None):
    """
    Fetch validator-delegator delegation and undelegation logs and save them.
    
    Args:
        web3: Web3 instance
        from_block: Starting block number (defaults to last processed block + 1 or 0)
        to_block: Ending block number (defaults to latest block)
        timestamp: Unix timestamp for the log file (defaults to current time)
    """
    # If from_block is not provided, use the last processed block + 1
    if from_block is None:
        last_block = get_last_processed_block("validator_delegator")
        from_block = last_block + 1 if last_block > 0 else 0
        print(f"Using last processed block + 1 as from_block: {from_block}")
    
    print(f"Fetching validator-delegator logs from block {from_block} to {to_block or 'latest'}...")
    
    # Get both Delegation and Undelegation event signatures
    event_signatures = [
        config.BGT_TOKEN.event_signatures["Delegation"],
        config.BGT_TOKEN.event_signatures["Undelegation"]
    ]
    
    logs = get_logs(
        web3=web3,
        contract_address=config.BGT_TOKEN.address,
        event_signature=event_signatures,
        from_block=from_block,
        to_block=to_block
    )
    
    print(f"Found {len(logs)} validator-delegator logs")
    
    if logs:
        save_raw_logs(logs, "validator_delegator", timestamp.replace(" ", "_"))


def fetch_user_rewards_vault_logs(web3: Web3, from_block: Optional[int] = None, 
                                 to_block: Optional[int] = None, 
                                 timestamp: Optional[int] = None):
    """
    Fetch user-rewards vault staking and withdrawal logs from all reward vaults and save them.
    
    Args:
        web3: Web3 instance
        from_block: Starting block number (defaults to last processed block + 1 or 0)
        to_block: Ending block number (defaults to latest block)
        timestamp: Unix timestamp for the log file (defaults to current time)
    """
    # If from_block is not provided, use the last processed block + 1
    if from_block is None:
        last_block = get_last_processed_block("user_rewards_vault")
        from_block = last_block + 1 if last_block > 0 else 0
        print(f"Using last processed block + 1 as from_block: {from_block}")
    
    print(f"Fetching user-rewards vault logs from block {from_block} to {to_block or 'latest'}...")
    
    # Get both Staked and Withdrawn event signatures
    event_signatures = [
        config.REWARDS_VAULT.event_signatures.get("Staked"),
        config.REWARDS_VAULT.event_signatures.get("Withdrawn")
    ]
    
    # Filter out None values
    event_signatures = [sig for sig in event_signatures if sig]
    
    if not event_signatures:
        print("No reward vault event signatures set. Skipping.")
        return
    
    # Get all reward vault addresses from the dictionary
    reward_vault_addresses = list(config.REWARD_VAULT_DIC.values())
    
    if not reward_vault_addresses:
        print("No reward vault addresses found in config.REWARD_VAULT_DIC. Skipping.")
        return
    
    print(f"Querying logs for {len(reward_vault_addresses)} reward vaults: {', '.join(reward_vault_addresses)}")
    
    # Query logs for all reward vault addresses at once
    logs = get_logs(
        web3=web3,
        contract_address=reward_vault_addresses,  # Pass the list of addresses
        event_signature=event_signatures,
        from_block=from_block,
        to_block=to_block
    )
    
    print(f"Found {len(logs)} user-rewards vault logs across all reward vaults")
    
    if logs:
        save_raw_logs(logs, "user_rewards_vault", timestamp.replace(" ", "_"))


def fetch_berachef_weight_update_logs(web3: Web3, from_block: Optional[int] = None, 
                                     to_block: Optional[int] = None, 
                                     timestamp: Optional[str] = None):
    """
    Fetch BeraChef validator weight update logs and save them.
    
    Args:
        web3: Web3 instance
        from_block: Starting block number (defaults to last processed block + 1 or 0)
        to_block: Ending block number (defaults to latest block)
        timestamp: Timestamp string for the log file (defaults to current time)
    """
    # If from_block is not provided, use the last processed block + 1
    if from_block is None:
        last_block = get_last_processed_block("berachef_weight_updates")
        from_block = last_block + 1 if last_block > 0 else 0
        print(f"Using last processed block + 1 as from_block: {from_block}")
    
    print(f"Fetching BeraChef weight update logs from block {from_block} to {to_block or 'latest'}...")

    # Get the ActivateRewardAllocation event signature
    event_signature = config.BERACHEF.event_signatures["ActivateRewardAllocation"]

    logs = get_logs(
        web3=web3,
        contract_address=config.BERACHEF.address,
        event_signature=event_signature,
        from_block=from_block,
        to_block=to_block
    )
    
    print(f"Found {len(logs)} BeraChef weight update logs")
    
    if logs:
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        save_raw_logs(logs, "berachef_weight_updates", timestamp.replace(" ", "_"))
        