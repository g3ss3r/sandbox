import os
import time
from datetime import datetime
import json
import logging
import asyncio
import websockets
import configparser
from threading import Lock

from op_datasaver import DataSaver


class OpParser:

    def __init__(self,
                 _tasks=None,
                 _websockets=None,
                 _data_saver=None,
                 _storage=None,
                 _timeout=0.06,
                 _logger=None,
                 _name="OpParser",
                 _pb=None):

        if _tasks is None:
            raise Exception("Tasks list required")

        if _websockets is None:
            raise Exception("Websocket object required")

        if _storage is None:
            raise Exception("DataSaver object required")

        # Saving params
        self.tasks = _tasks
        self.websockets = _websockets
        self.storage = _storage
        self.timeout = _timeout
        self.name = _name
        self.logger = _logger
        self.pb = _pb

        # Setting initial state
        self.sending_flag = False
        self.sent_count = 0
        self.receive_count = 0
        self.retries_count = 0

    # Entrypoint
    async def execute(self):
        self.logger.info("Worker {} start executing {} tasks".format(self.name, len(self.tasks)))
        time_start = time.time()

        # Concurrent executing sending and receiving processes
        await asyncio.gather(
            self.send_data(),
            self.receive_data()
        )

        duration = time.time() - time_start
        self.logger.info("Worker {} finished executing {} tasks. Duration {}s.".format(self.name,
                                                                                       len(self.tasks),
                                                                                       duration))

    # Method for pushing requests
    async def send_data(self):
        self.sending_flag = True
        self.sent_count = 0

        for task in self.tasks:
            await self.push_request(task)
            self.logger.info("{} -> eth_getBlockByNumber({})".format(self.name, task))
            await asyncio.sleep(self.timeout)

        self.sending_flag = False
        self.logger.info("{} -> Sending finished: sended {} requests".format(self.name, self.sent_count))

    # This method must be separated from send_data()
    # because of retry cases in receive_data()
    async def push_request(self, task):
        # Working with batches sample
        # tasks = []
        # for i in range(10*task, 10*task+10):
        #     tasks.append(dict(jsonrpc='2.0', id=i, method='eth_getBlockByNumber', params=[hex(i), True]))
        # print(tasks)
        # await self.websockets.send(json.dumps(tasks))
        # self.sent_count += 10

        request = dict(jsonrpc='2.0', id=task, method='eth_getBlockByNumber', params=[hex(task), True])
        await self.websockets.send(json.dumps(request))
        self.sent_count += 1

    # Method for receiving data and retries
    async def receive_data(self):
        self.receive_count = 0

        while self.continue_listening():
            responce_str = await asyncio.wait_for(self.websockets.recv(), timeout=60)
            responce_json = json.loads(responce_str)
            self.receive_count += 1

            # If data received correctly - process it
            block = responce_json.get('result')
            error = responce_json.get('error')
            if block:
                block_number = int(block.get('number'), base=16)
                block_timestamp = datetime.fromtimestamp(int(block.get('timestamp'), base=16))

                self.logger.info("{} -> receive_data({})".format(self.name, block_number))

                transactions = []
                if len(block['transactions']) > 0:
                    for transaction in block['transactions']:
                        # data preparing
                        _value = transaction.get('value')
                        trx_value = float.fromhex(_value) if _value else -1.0

                        _gas = transaction.get('gas')
                        trx_gas = int(_gas, base=16) if _gas else -1

                        _gas_price = transaction.get('gasPrice')
                        trx_gasprice = int(_gas_price, base=16) if _gas_price else -1

                        _to = transaction.get('to')
                        trx_to = _to if _to else 'Empty'

                        trx_size = len(json.dumps(transaction))

                        trx_hash = transaction.get('hash')
                        trx_from = transaction.get('from')
                        trx_input = transaction.get('input')

                        transaction = [
                            block_number,
                            block_timestamp,
                            trx_hash,
                            trx_from,
                            trx_to,
                            trx_gas,
                            trx_gasprice,
                            trx_input,
                            trx_value,
                            trx_size
                        ]
                        transactions.append(transaction)
                    self.storage.add(transactions)
                    self.logger.info("{} transactions from block {} added to storage".format(len(transactions), block_number))
                    self.update_progress()
                else:
                    self.logger.info("Block {} have no transactions".format(block_number))
            # If we exceeded the limits - increase timeout and retry
            elif error:
                task_id = responce_json.get('id')
                error_code = error.get('code')
                error_message = error.get('message')
                self.logger.info("{} ERROR {} WITH TASK {}: {}".format(self.name, error_code, task_id, error_message))

                time.sleep(2)
                self.logger.info("{} -> SLEEP for 2s")

                self.timeout += 0.01

                await self.push_request(task_id)
                self.retries_count += 1
                self.logger.info("{} RETRY -> eth_getBlockByNumber({})".format(self.name, task_id))

        self.logger.info("{} -> Receiving finished: received {} requests".format(self.name, self.receive_count))

    # Flag for receiving loop
    def continue_listening(self):
        return self.sending_flag or (self.sent_count - self.receive_count) > 0

    def get_state(self):
        return self.sent_count, self.receive_count, self.retries_count, self.timeout

    def update_progress(self):
        if self.pb is not None:
            self.pb.update_progress()


class ProgressBar:
    def __init__(self, _workers, _tasks):
        self.workers = _workers
        self.tasks_count = _tasks
        self.last_value = 0

        print('\rTotal tasks: {} starting ...'.format(self.tasks_count), end="")

    def update_progress(self):
        total_sent = 0
        total_receive = 0
        total_retries = []
        total_timeout = []
        for i in range(len(self.workers)):
            worker = self.workers[i]
            sent_count, receive_count, worker_retries, worker_timeout = worker.get_state()
            total_sent += sent_count
            total_receive += receive_count
            total_retries.append(worker_retries)
            total_timeout.append(worker_timeout)

            bar_text = '\rTotal {} tasks: [{} sended => {} received] retries => {} timeout => {}s '.format(self.tasks_count,
                                                                                                    total_sent,
                                                                                                    total_receive,
                                                                                                    json.dumps(total_retries),
                                                                                                    json.dumps(total_timeout))
            print(bar_text, end="")


async def main():
    lock = Lock()
    storage = DataSaver(config['DATASAVER'], lock)

    workers = []
    threads_count = int(config['WORKERS']['THREADS'])
    tasks_count = 1000000
    pb = ProgressBar(workers, tasks_count)

    for i in range(threads_count):
        worker_name = "{}-{}".format(config['WORKERS']['PREFIX'], str(i))
        ws_key = 'URL_{}'.format(i)

        worker_timeout = float(config['WORKERS']['TIMEOUT'])
        websocket = await websockets.connect(config['RPC'][ws_key])
        tasks = range(i * 200000, i * 200000 + 200000)
        worker = OpParser(_name=worker_name,
                          _tasks=tasks,
                          _websockets=websocket,
                          _storage=storage,
                          _logger=logger,
                          _timeout=worker_timeout,
                          _pb=pb)
        workers.append(worker)

    loop = asyncio.get_running_loop()
    future = loop.create_future()

    for worker in workers:
        loop.create_task(worker.execute())

    await future


if __name__ == '__main__':
    path = os.path.dirname(os.path.realpath(__file__))

    # Setup logger
    now = datetime.now()
    log_filename = path + now.strftime("/logs/%d.%m.%Y_%H.%M.%S.log")
    print("Log file: ", log_filename)
    logging.basicConfig(filename=log_filename, filemode='a', level=logging.INFO, format='%(asctime)s %(message)s')
    logger = logging.getLogger(__name__)

    # Setup config
    config_path = '/'.join([path, "conf.ini"])
    config = configparser.ConfigParser()
    config.read(config_path)

    asyncio.run(main())
