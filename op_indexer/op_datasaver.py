import clickhouse_connect


class DataSaver:

    def __init__(self, db_config, lock, batch_size):
        self.lock = lock
        self.batch_size = batch_size
        self.trxs = []
        self.trx_data = []

        self.client = clickhouse_connect.get_client(host=db_config['host'],
                                                    port=db_config['port'],
                                                    user=db_config['username'],
                                                    password=db_config['password'])

        init_query = '''CREATE TABLE IF NOT EXISTS default.trx
                        (
                            `blockNumber` UInt64,
                            `blockTimestamp` TIMESTAMP,
                            `hash` FixedString(66),
                            `from` FixedString(42),
                            `to` FixedString(42),
                            `input` TEXT,
                            `gas` UInt64,
                            `gasPrice` UInt64
                        )
                        ENGINE = Log;'''
        self.client.command(init_query)

    def add(self, transactions):
        self.trxs += transactions

        if len(self.trxs) > self.batch_size:
            self.push()

    def push(self):
        with self.lock:
            try:
                self.client.insert('trx', self.trxs, column_names=['blockNumber', 'blockTimestamp', 'hash', 'from', 'to', 'input', 'gas', 'gasPrice'])
                self.trxs = []
            except Exception as e:
                print("Exception:",  e)
                print("Transaction data:", self.trxs)
                exit()

    def get_state(self):
        state_query = '''SELECT
                            count(*) as records_count,
                            max(`blockNumber`) as latest_block,
                            max(`blockTimestamp`) as latest_timestamp
                         FROM trx'''
        result = self.client.query(state_query)
        records_count, latest_block, latest_timestamp = result.result_set[0]

        return {
                    'records_count': records_count,
                    'latest_block': latest_block,
                    'latest_timestamp': latest_timestamp
        }
