from flask import Blueprint
from flask import request
from src.config.errors import error_message
from src.config.errors import no_content
from src.config.errors import NoAdException
from src.config.application import app_context
from src.service.buyer import Buyer

ad_api = Blueprint('ad_api', __name__)


@ad_api.route('/bw_dsp', methods=['POST'])
def index():
    '''
    接收bid_floor並回傳要標價的廣告ID與價格
    若出價小於bid_floor，則回傳204

    Parameters
    ----------
    bid_floor: float, 廣告欄位的底價

    Returns
    -------
    ad_id: string, 要競標的廣告
    price: float, 要競標的廣告的出價
    '''
    try:
        buyer = Buyer(app_context)
        post = request.get_json()
        bid_floor = post.get('bid_floor')
        result = buyer.bid_ad(bid_floor)
        return result
    except NoAdException as e:
        return no_content(e)
    except Exception as e:
        return error_message(e)
