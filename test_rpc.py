from dataclasses import dataclass
from web3 import Web3
import requests

@dataclass
class Abi:
    contract_address: str
    abi_url: str
    abi: dict = None

    def __post_init__(self):
        """Get ABIs"""
        self.abi = self.get_abi()

    def get_abi(self):
        response = requests.get(self.abi_url)
        abi = response.json()
        return abi

bgt_token = Abi(
        contract_address="0x656b95E550C07a9ffe548bd4085c72418Ceb1dba",
        abi_url="https://raw.githubusercontent.com/berachain/doc-abis/refs/heads/main/core/BGT.json")

def _log_call(contract_address: str, event_signature:str, to_block:int, from_block:int):
    logs = web3.eth.get_logs({
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": contract_address,
            "topics": [[event_signature]]
        })
    return logs

def get_logs(config, event_signature:str, to_block:int, from_block:int = 0):
    all_logs = []
    current_block = from_block
    end_block = to_block
    while current_block < end_block:
        to_block = min(current_block + 10000, end_block)
        logs = _log_call(config.contract_address, event_signature, to_block, current_block)
        all_logs.extend(logs)
        current_block = to_block + 1
    return all_logs

rpc = 'http://100.64.48.20:58545'
web3 = Web3(Web3.HTTPProvider(rpc))
latest_block = web3.eth.get_block("latest")["number"]
bgt_token_contract = web3.eth.contract(address=bgt_token.contract_address, abi=bgt_token.abi)
delegations = get_logs(bgt_token, "0x331ddcd7d9830ca2dd8d4e44a2ff8d00f690b6b56827cb02135ee066a702f65f", to_block=latest_block)

print(delegations)

