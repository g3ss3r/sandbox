import clickhouse_connect


class DataSaver:

    def __init__(self, _config, _lock):
        self.lock = _lock
        self.batch_size = int(_config['BATCH_SIZE'])
        self.trxs = []
        self.trx_data = []

        self.client = clickhouse_connect.get_client(host=_config['HOST'],
                                                    port=_config['PORT'],
                                                    user=_config['USERNAME'],
                                                    password=_config['PASSWORD'])

        init_query = '''CREATE TABLE IF NOT EXISTS default.optimism_trx
                        (
                            `blockNumber` Int64,
                            `blockTimestamp` TIMESTAMP,
                            `hash` FixedString(66),
                            `from` FixedString(42),
                            `to` FixedString(42),
                            `gas` Int64,
                            `gasPrice` Int64,
                            `input` TEXT,
                            `value` Float32,
                            `size` Int64
                        )
                        ENGINE = MergeTree
                        PRIMARY KEY `blockNumber`
                        ORDER BY `blockNumber`;'''
        self.client.command(init_query)

    def add(self, transactions):
        self.trxs += transactions

        if len(self.trxs) >= self.batch_size:
            self.push()

    def push(self):
        with self.lock:
            try:
                columns = ['blockNumber',
                            'blockTimestamp',
                            'hash',
                            'from',
                            'to',
                            'gas',
                            'gasPrice',
                            'input',
                            'value',
                            'size']

                self.client.insert('optimism_trx', self.trxs, column_names=columns)
                self.trxs = []
            except Exception as e:
                print("Exception:",  e)
                print("Transaction data:", self.trxs)


    def get_state(self):
        state_query = '''SELECT
                            count(*) as records_count,
                            max(`blockNumber`) as latest_block,
                            max(`blockTimestamp`) as latest_timestamp
                         FROM optimism_trx'''
        result = self.client.query(state_query)
        records_count, latest_block, latest_timestamp = result.result_set[0]

        return {
                    'records_count': records_count,
                    'latest_block': latest_block,
                    'latest_timestamp': latest_timestamp
        }
