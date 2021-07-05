import os
import yaml
from dotenv import load_dotenv
from src.config.logger import DebugLog
from src.config import DEFAULT_CONFIG_FILE
from src.db.redis_dao import RedisDao
from src.db.postgresql_dao import PostgreSqlDao


class ApplicationContext(object):
    '''
    設定環境變數、PostgreSQL DAO、Logger

    Functions
    ----------
    __load_active_profiles: 設定yaml檔的環境變數
    __load_env: 設定.env檔的環境變數
    __load_postgresql_dao: 設定PostgreSQL的存取物件
    __load_redis_dao: 設定Redis的存取物件
    __load_logger: 設定Logger
    '''

    def __init__(self):
        self.server_config = {}
        self.ad_path_config = {}
        self.ad_db_config = {}
        self.ad_redis_config = {}
        self.postgresql_dao = None
        self.redis_dao = None
        self.logger = None

        self.__load_active_profiles()

        if not (self.server_config and self.ad_path_config):
            raise EnvironmentError("Can not load application.yml file")

        self.__load_env()

        if not (self.ad_db_config and self.ad_redis_config):
            raise EnvironmentError("Can not load .env file")

        self.__load_logger()
        self.__load_postgresql_dao()
        self.__load_redis_dao()

    def __load_active_profiles(self):
        with open(DEFAULT_CONFIG_FILE, encoding="utf-8") as stream:
            data = yaml.safe_load(stream)
        self.server_config = data['server']
        self.ad_path_config = data['ad']['path']
        self.ad_db_config = data['db']
        self.ad_redis_config = data['redis']

    def __load_env(self):
        load_dotenv()
        self.ad_db_config['db_host'] = os.getenv('DB_HOST')
        self.ad_db_config['db_name'] = os.getenv('DB_NAME')
        self.ad_db_config['db_username'] = os.getenv('DB_USERNAME')
        self.ad_db_config['db_password'] = os.getenv('DB_PASSWORD')
        self.ad_redis_config['redis_url'] = os.getenv('REDIS_URL')

    def __load_postgresql_dao(self):
        self.postgresql_dao = PostgreSqlDao(self.ad_db_config, self.logger)

    def __load_redis_dao(self):
        self.redis_dao = RedisDao(self.ad_redis_config, self.logger)

    def __load_logger(self):
        self.logger = DebugLog(self.ad_path_config).logger


app_context = ApplicationContext()
