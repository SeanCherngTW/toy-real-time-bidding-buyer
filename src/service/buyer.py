import random
from src.config.errors import NoAdException


class Buyer(object):
    '''
    從資料庫取得廣告與價格資料，並根據bid_floor進行出價

    Functions
    ----------
    bid_ad: 出價
    choose_ad: 選擇要出價的廣告與價格
    compute_bid_price: 計算廣告的價格
    '''

    def __init__(self, app_context):
        self.redis_dao = app_context.redis_dao
        self.postgresql_dao = app_context.postgresql_dao

    def bid_ad(self, bid_floor):
        '''
        選出最高價的廣告，並判斷其價格是否大於bid_floor

        Parameters
        ----------
        bid_floor: float, 廣告欄位的底價

        Returns
        -------
        result: dict
            ad_id: string, 要出價的廣告ID
            price: float, 要出價的廣告價格

        Exceptions
        ----------
        NoAdException: 資料庫沒有廣告，或所有廣告出價皆小於bid_floor
        '''
        ad = self.choose_ad()
        if ad:
            ad_id = ad['ad_id']
            price = ad['bid_price']
            if price >= bid_floor:
                result = {'ad_id': ad_id, 'price': price}
                return result
        raise NoAdException('No available ads with price >= bid_floor')

    def choose_ad(self):
        '''
        從PostgreSQL取得廣告資料，並計算每個廣告的bid_price

        Returns
        -------
        selected_ad: dict
            ad_id: string, 要出價的廣告ID
            price: float, 要出價的廣告價格
        '''
        ads = self.load_ads()
        for ad in ads:
            ad['ad_id'] = str(ad['ad_id'])
            ad['bid_price'] = self.compute_bid_price(ad)
        selected_ad = max(ads, key=lambda x: x['bid_price'])
        return selected_ad

    def compute_bid_price(self, ad):
        '''
        計算廣告的價格：bidding_cpm乘上1~10的隨機整數

        Returns
        -------
        bid_price: float, 廣告的價格
        '''
        bid_price = ad['bidding_cpm'] * random.randint(1, 10)
        return bid_price

    def load_ads(self):
        '''
        讀取廣告
        先檢查快取有沒有，若無，再從DB讀取

        Returns
        -------
        ads: list of RealDictRow, 廣告列表

        Exceptions
        ----------
        NoAdException: 快取和資料庫皆沒有廣告
        '''
        ads = self.redis_dao.get_ads()
        if ads:
            return ads
        ads = self.postgresql_dao.get_available_ads()
        if ads:
            self.redis_dao.set_ads(ads)
            return ads
        raise NoAdException('No available ads in database')
