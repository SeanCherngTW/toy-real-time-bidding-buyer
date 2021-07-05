import redis
import pickle


class RedisDao(object):
    '''
    操作存放廣告的Redis
    會儲存「可能用得到的」廣告，並按照bidding_cpm降冪排列，需要符合：
    1. status=True
    2. bidding_cpm * 10 > 最高的bidding_cpm
    若資料庫更新時，會全部重置快取(應該可以再想辦法優化)

    Functions
    ----------
    get_ads: 從Redis取得所有廣告資料
    set_ads: 將廣告資料存放到Redis
    delete_ads: 刪除快取的所有廣告
    '''

    def __init__(self, ad_redis_config, logger):
        self.logger = logger
        self.redis_url = ad_redis_config['redis_url']
        self.AD_KEY = ad_redis_config['ad_key']
        self.redis_client = redis.StrictRedis.from_url(self.redis_url)
        ads = self.redis_client.get(self.AD_KEY)
        self.logger.info('Redis connected')

    def __del__(self):
        self.redis_client.close()
        self.logger.info('Redis disconnected')

    def get_ads(self):
        ads = self.redis_client.get(self.AD_KEY)
        ads = pickle.loads(ads) if ads else []
        return ads

    def set_ads(self, ads):
        if ads:
            self.logger.info('Ads are set to Redis')
            self.redis_client.set(self.AD_KEY, pickle.dumps(ads))

    def delete_ads(self):
        self.logger.info('Delete all ads in Redis')
        self.redis_client.delete(self.AD_KEY)
