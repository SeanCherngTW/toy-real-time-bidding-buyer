import psycopg2
import psycopg2.extras
from src.config.errors import UndefinedTableError
from src.config.errors import DuplicateTableError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class PostgreSqlDao(object):
    '''
    操作存放廣告的PostgreSQL關聯式資料庫

    Functions
    ----------
    connect_to_ad_db: 和PostgreSQL的廣告資料庫建立連線
    connect: 和PostgreSQL指定的資料庫建立連線
    create_db: 創建資料庫
    create_ad_table: 創建廣告表格
    insert_ad: 插入廣告至廣告表格
    get_available_ads: 從廣告表格取出status=True的所有廣告
    run_cmd: 執行SQL指令
    '''

    def __init__(self, ad_db_config, logger):
        self.logger = logger
        self.db_name = ad_db_config['db_name']
        self.db_host = ad_db_config['db_host']
        self.db_username = ad_db_config['db_username']
        self.db_password = ad_db_config['db_password']
        self.fetch_count = ad_db_config['fetch_count']
        self.connect_to_ad_db()
        self.logger.info('PostgreSQL connected')

    def __del__(self):
        self.conn.close()
        self.logger.info('PostgreSQL disconnected'.)

    def connect_to_ad_db(self):
        '''
        和PostgreSQL的廣告資料庫建立連線
        Exceptions
        ----------
        psycopg2.OperationalError:
        廣告資料庫不存在，需先連線到PostgreSQL後，再創建廣告資料庫，再把連線設定到廣告資料庫
        '''
        try:
            self.connect(db_name=self.db_name)
        except psycopg2.OperationalError as e:
            self.logger.info(e)
            self.connect(db_name=None)
            self.create_db(self.db_name)
            self.logger.info('Create database {} success'.format(self.db_name))
            self.connect(db_name=self.db_name)

    def connect(self, db_name):
        '''
        和PostgreSQL的資料庫建立連線，並設定Auto commit
        '''
        self.conn = psycopg2.connect(
            host=self.db_host,
            dbname=db_name,
            user=self.db_username,
            password=self.db_password,
        )
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    def create_db(self, db_name):
        cmd = 'CREATE DATABASE {}'.format(db_name)
        self.run_cmd(cmd)

    def create_ad_table(self):
        cmd = '''
            CREATE TABLE ad(
	            ad_id SERIAL PRIMARY KEY,
                status BOOLEAN,
                bidding_cpm INT
            )
        '''
        self.run_cmd(cmd)

    def insert_ad(self, schema):
        '''
        新增廣告到廣告表格中

        Parameters
        ----------
        schema: dict
            key: ad_id
            value: (status, bidding_cpm)
        '''
        keys = ','.join([str(key) for key in schema.keys()])
        values = ','.join([str(value) for value in schema.values()])
        cmd = f'INSERT INTO ad ({keys}) VALUES ({values})'
        self.run_cmd(cmd)

    def get_available_ads(self):
        '''
        從資料庫的廣告表格取得status=True的所有廣告
        使用ORDER BY bidding_cpm DESC讓廣告以bidding_cpm降冪排列
        先找出最大的bidding_cpm，然後迭代所有廣告，直到：
            1. 目前廣告的bidding_cpm * 10 < max_bidding_cpm，代表從這個廣告之後的廣告都不可能被選上
            2. 資料庫已無廣告
        則可以停止fetching

        Returns
        -------
        ads: list of RealDictRow, 從資料庫中廣告表格取得的廣告
        '''
        ads = []
        max_bidding_cpm = 0
        try:
            cmd = 'SELECT * FROM ad WHERE status = true ORDER BY bidding_cpm DESC'
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(cmd)
            while True:  # Batch fetching
                fetched_ads = cur.fetchmany(self.fetch_count)
                if fetched_ads:
                    self.logger.info(max_bidding_cpm)
                    self.logger.info(fetched_ads)
                    max_bidding_cpm = self.update_max_bidding_cpm(max_bidding_cpm, fetched_ads)
                    ads += fetched_ads
                if self.stop_fetching_ads(max_bidding_cpm, fetched_ads):
                    break
        except UndefinedTableError as e:
            raise UndefinedTableError('psycopg2.errors.UndefinedTable')
        return ads

    def stop_fetching_ads(self, max_bidding_cpm, batch_ads):
        '''
        判斷是否要繼續從資料庫取得資料
        取得廣告時有設定按照bidding_cpm降冪排列
        若：
            1. 目前廣告的bidding_cpm * 10 < max_bidding_cpm，代表從這個廣告之後的廣告都不可能被選上
            2. 資料庫已無廣告
        則可以停止fetching

        Parameters
        ----------
        max_bidding_cpm: int, 目前最高的bidding_cpm
        batch_ads: list of RealDictRow, 從資料庫中廣告表格取得的廣告

        Returns
        -------
        bool: 是否繼續fetch
        '''
        return not batch_ads or batch_ads[-1]['bidding_cpm'] * 10 < max_bidding_cpm

    def update_max_bidding_cpm(self, max_bidding_cpm, fetched_ads):
        '''
        更新資料庫中最高的bidding_cpm，只有在第一次執行時會更新值

        Parameters
        ----------
        max_bidding_cpm: int, 目前最高的bidding_cpm
        fetched_ads: list of RealDictRow, 從資料庫中廣告表格取得的廣告

        Returns
        -------
        max_bidding_cpm: 目前最高的bidding_cpm
        '''
        max_bidding_cpm = max_bidding_cpm if max_bidding_cpm > 0 else fetched_ads[0]['bidding_cpm']
        return max_bidding_cpm

    def run_cmd(self, cmd):
        try:
            cur = self.conn.cursor()
            cur.execute(cmd)
            self.logger.info('{} Success'.format(cmd))
        except UndefinedTableError as e:
            raise UndefinedTableError('psycopg2.errors.UndefinedTable')
        except DuplicateTableError as e:
            raise DuplicateTableError('psycopg2.errors.DuplicateTable')
