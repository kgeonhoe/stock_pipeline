#%% 
from pykis import * 
from itertools import chain 
import pandas as pd 
import numpy as np 
import time 
from datetime import datetime, timedelta 
from dateutil.relativedelta import relativedelta


class Collector: 
    def __init__(self, appkey , appsecret , virtual_accountYN : bool): 
        print("__init__ 함수에 들어왔습니다.")
    
        self.kis = PyKis(
                        appkey= appkey
                       ,appsecret= appsecret
                       ,virtual_account= virtual_accountYN
                         )
        
        
    # kis_get_values 함수 정의
    def kis_get_values(self, code, time_from, time_to):
        time.sleep(0.5)
        time_from = datetime.strptime(time_from, "%Y%m%d")
        time_to = datetime.strptime(time_to, "%Y%m%d")
        try:
            prices = self.kis.stock(code).period_price(time_from, time_to)
            stockdate = [p.stck_bsop_date.strftime("%Y%m%d") for p in prices.prices]
            stockclose = [p.stck_clpr for p in prices.prices]
            stockopen = [p.stck_oprc for p in prices.prices]
            stockhigh = [p.stck_hgpr for p in prices.prices]
            stocklow = [p.stck_lwpr for p in prices.prices]
            stockvolume = [p.acml_vol for p in prices.prices]
            stockpricevolume = [p.acml_tr_pbmn for p in prices.prices]
            return stockdate, stockclose, stockopen, stockhigh, stocklow, stockvolume, stockpricevolume
        except Exception as e:
            print(f"Error fetching data for {code}: {e}")
            return None
        
    def generate_date_ranges(self, start_date, end_date, delta_days):
        start_date = datetime.strptime(start_date, "%Y%m%d")
        end_date = datetime.strptime(end_date, "%Y%m%d")
        date_ranges = []
        current_start = start_date
        while current_start < end_date:
            current_end = current_start + timedelta(days=delta_days - 1)
            if current_end > end_date:
                current_end = end_date
            date_ranges.append((current_start.strftime("%Y%m%d"), current_end.strftime("%Y%m%d")))
            current_start = current_end + timedelta(days=1)
        return date_ranges

    def kis_get_all_stock(self): 
        kis = self.kis
        # stock_df = pd.DataFrame()
        stockCode = []
        stockName = []
        priceChange = []
        lastdate = []
        for stock in chain(kis.market.kospi.all(), kis.market.kosdaq.all()) : 
            if (len(stock.mksc_shrn_iscd) == 6) & (stock.prst_cls_code == '0') & (stock.scrt_grp_cls_code == 'ST'): 
            # prst_cls_code : 우선주 보통주 여부 
                
                stockCode.append(stock.mksc_shrn_iscd) # 코드 번호 
                stockName.append(stock.hts_kor_isnm) # 코드 이름 
                priceChange.append(stock.fcam_mod_cls_code) #  액면가 변경 구분 코드 (00:해당없음 01:액면분할 02:액면병합 99:기타   
                lastdate.append(stock.stck_lstn_date) # 주식의 마지막 날짜
                
        kis_stock_df = pd.DataFrame({"stockCode": stockCode, "stockName" : stockName, "priceChange" : priceChange, "lastdate": lastdate})       
        return kis_stock_df