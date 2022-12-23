import logging
import time
from queue import Queue
from threading import Thread
from threading import Lock
import web3 as Web3

from op_worker import get_trx_by_block
from op_datasaver import DataSaver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Clickhouse default connection configuration
db_config = {
    'host': 'localhost',
    'port': '8123',
    'username': 'default',
    'password': ''
}

rpc_providers = []
RPC_KEYS = [
    {
        'type': 'alchemy',
        'url': 'https://opt-mainnet.g.alchemy.com/v2/{some_kay}'
    },
    {
        'type': 'blastapi',
        'url': 'https://optimism-mainnet.blastapi.io/{some_key}'
    }
]

# How many blocks must be processed before saving data
BATCH_SIZE = 100

# Threads for receiving data from blockchain
WORKERS_COUNT = 5


class DownloadWorker(Thread):

    def __init__(self, queue, lock, data_saver):
        Thread.__init__(self)
        self.queue = queue
        self.lock = lock
        self.data_saver = data_saver

    def run(self):
        while True:
            block_number = self.queue.get()
            try:
                provider = rpc_providers[block_number % len(rpc_providers)]
                transactions = get_trx_by_block(block_number, provider)
                self.data_saver.add(transactions)
            except Exception as e:
                print(e)
            finally:
                self.data_saver.push()
                self.queue.task_done()


def prepare_providers():
    if 0 == len(RPC_KEYS):
        print('At least one RPC key is required')
        exit()

    for provider_config in RPC_KEYS:
        web3provider = Web3.HTTPProvider(provider_config['url'])
        rpc_providers.append(web3provider)


def main():
    ts = time.time()

    queue = Queue()
    lock = Lock()

    prepare_providers()
    data_saver = DataSaver(db_config, lock, BATCH_SIZE)

    # Preparing queue
    state = data_saver.get_state()
    block_from = state['latest_block']
    block_to = block_from + 1000
    # web3 = Web3.Web3(rpc_providers[0])
    # web3.eth.block_number
    # block_to = block_from + 2000000

    for block_number in range(block_from, block_to):
        logger.info('Queueing {}'.format(block_number))
        queue.put(block_number)

    # Start workers
    for x in range(WORKERS_COUNT):
        worker = DownloadWorker(queue, lock, data_saver)
        worker.daemon = True
        worker.start()

    queue.join()
    logging.info('Took %s', time.time() - ts)


if __name__ == '__main__':
    main()
