"""
Configuration settings for the Berachain data processing system.
"""
import os
from dataclasses import dataclass
from typing import Dict, List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# RPC endpoint
RPC_ENDPOINT = os.getenv('RPC_ENDPOINT')

# Contract addresses and ABIs


@dataclass
class ContractConfig:
    address: str
    abi_url: str
    event_signatures: Dict[str, str]


# BGT Token contract
BGT_TOKEN = ContractConfig(
    address="0x656b95E550C07a9ffe548bd4085c72418Ceb1dba",
    abi_url="https://raw.githubusercontent.com/berachain/doc-abis/refs/heads/main/core/BGT.json",
    event_signatures={
        "Delegation": "0x331ddcd7d9830ca2dd8d4e44a2ff8d00f690b6b56827cb02135ee066a702f65f",
        "Undelegation": "0x574d5117cce15ba0f7443c955ec7bfc5f3e9b50ac2314a0f33ab7a9ea94c3ab1"
    }
)

# Rewards vault contract

REWARD_VAULT_DIC = {
    "WBERA/HONEY": "0xC2BaA8443cDA8EBE51a640905A8E6bc4e1f9872c",
    "WBTC/BERA": "0x086f82fa0cA310Cc835a9DB4f53697687ef149c7",
    "WETH/BERA": "0x17376aD6167a5592FbEAA42e6068c132474a513d",
    "BYUSD/HONEY": "0x6649Bc987a7c0fB0199c523de1b1b330cd0457A8",
    "USDC.e/HONEY": "0xF99be47baf0c22B7eB5EAC42c8D91b9942Dc7e84"
}

REWARDS_VAULT = ContractConfig(
    address="",
    abi_url="https://raw.githubusercontent.com/berachain/doc-abis/refs/heads/main/core/RewardVault.json",
    event_signatures={
        "Staked": "0x9e71bc8eea02a63969f509818f2dafb9254532904319f9dbda79b67bd34a5f3d",
        "Withdrawn": "0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5",
    }
)

# Distributor contract
DISTRIBUTOR = ContractConfig(
    address="0xD2f19a79b026Fb636A7c300bF5947df113940761",
    abi_url="https://raw.githubusercontent.com/berachain/doc-abis/refs/heads/main/core/Distributor.json",
    event_signatures={
        "Distributed": "0x027042b00b5da1362792832f3775452610369da8ce2c07af183cdabd276e3a11"
    }
)

# BeraChef contract
BERACHEF = ContractConfig(
    address="0xdf960E8F3F19C481dDE769edEDD439ea1a63426a",
    abi_url="https://raw.githubusercontent.com/berachain/doc-abis/refs/heads/main/core/BeraChef.json",
    event_signatures={
        "ActivateRewardAllocation": "0x09fed3850dff4fef07a5284847da937f94021882ecab1c143fcacd69e5451bd8"
    }
)

# File paths
RAW_LOGS_DIR = {
    "validator_delegator": "raw_logs/validator_delegator",
    "user_rewards_vault": "raw_logs/user_rewards_vault"
}

PROCESSED_DATA_DIR = {
    "validator_delegator": "processed_data/validator_delegator",
    "user_rewards_vault": "processed_data/user_rewards_vault"
}

# CSV column definitions
VALIDATOR_DELEGATOR_COLUMNS = [
    "timestamp", "block_number", "validator_address", "delegator_address", "amount_bgt_delegated"
]

USER_REWARDS_VAULT_COLUMNS = [
    "timestamp", "block_number", "user_address", "rv_address", "user_rv_share"
]
