import os
import time
from typing import Dict, List, Optional
import pandas as pd
from tqdm import tqdm

import config
from scripts.utils import *


def decode_weight_update_log(w3: Web3, log: Dict) -> Dict:
    event_signature = '0x' + log['topics'][0].hex() if isinstance(log['topics'][0], bytes) else log['topics'][0]
    assert event_signature == config.BERACHEF.event_signatures['ActivateRewardAllocation'], f"Invalid event signature: {event_signature}"
    # Load contract
    abi = get_contract_abi(config.BERACHEF.abi_url)
    berachef_contract = w3.eth.contract(address=config.BERACHEF.address, abi=abi)
    decoded_log = berachef_contract.events.ActivateRewardAllocation().process_log(log)
        
    return {
        "block_number": log['blockNumber'],
        "transaction_hash": '0x' + log['transactionHash'].hex() if isinstance(log['transactionHash'], bytes) else log['transactionHash'],
        "validator_address": '0x' + log['topics'][1].hex() if isinstance(log['topics'][1], bytes) else log['topics'][1],
        "start_block": decoded_log.args.startBlock,
        "weights": [(w.receiver, w.percentageNumerator) for w in decoded_log.args.weights]
    }

def process_weight_update_log_for_multiprocessing(w3: Web3, log: Dict) -> Dict:
    """
    Process a single log entry for multiprocessing.
    
    Args:
        log: Log entry to decode
        
    Returns:
        Decoded log entry
    """
    try:
        return decode_weight_update_log(w3, log)
    except Exception as e:
        print(f"Error decoding log: {e}")
        return None
