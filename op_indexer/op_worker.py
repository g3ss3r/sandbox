import web3 as Web3
from web3.middleware import geth_poa_middleware
from datetime import datetime


def get_trx_by_block(block_number, provider):
    print("processing:", block_number)

    web3 = Web3.Web3(provider)
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)

    block = web3.eth.getBlock(block_number, True)

    block_number = block['number']
    block_timestamp = datetime.fromtimestamp(block['timestamp'])

    results = []
    for transaction in block['transactions']:
        # TO field may be Null in case contract creation
        to_field = "Empty" if transaction['to'] is None else transaction['to']

        results.append([block_number,
                        block_timestamp,
                        transaction['hash'].hex(),
                        transaction['from'],
                        to_field,
                        transaction['input'],
                        transaction['gas'],
                        transaction['gasPrice']])
    return results
