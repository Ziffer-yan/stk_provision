# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 16:07:39 2022

@author: yhzhang
"""
import numpy as np
import pandas as pd 
from pathlib import Path
import time 
import datetime as dtm
import requests

from AuxFunc import t_now, add_cbse, add_uase, formal_day

#%%
def dc_get_snap(symbol):
    '''
    symbol同时支持可转债与股票的盘口数据，输入格式为: sz128106, sh600036
    return: dict格式的数据
    '''
    market_type = {"sh": "1", "sz": "0"}
    params = {
        "fltt": '2',
        "invt": '2',
        "fields": "f57,f58,f86,f530," \
                #代码，名称, 3S时间，十档买卖价量
                "f60,f43,f44,f45,f46,f47,f48,f71," \
                #昨收, 最新last, high, low, 今开open, volume, amount, 均价
                "f169,f170,f171,f50,f49,f161,f191,f192",
                #涨幅，涨跌额，振幅，量比，外盘，内盘，委比，委差
        "secid": f"{market_type[symbol[:2]]}.{symbol[2:]}",
        "_": str(time.time()),
    }
    url = 'http://push2.eastmoney.com/api/qt/stock/get'

    resp = requests.get(url, params)
    # data = json.loads(resp.text)['data']
    data = resp.json()['data']
    
    dt = dtm.datetime.fromtimestamp(data['f86'])
    dt_str = f'{dt.year}-{dt.month:02d}-{dt.day:02d} {dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}'
    
    rst = { 'code': data['f57'], 'name': data['f58'], 
            'timeindex': dt_str,
            'localtime': t_now(1,0),
            'preclose': data['f60'], 'open': data['f46'], 'high': data['f44'], 'low': data['f45'],
            'last': data['f43'], 'avgp': data['f71'], 
            'volume': data['f47'], 'amount': data['f48'],
            'ask5p': data['f31'], 'ask5v': data['f32'], 'ask4p': data['f33'], 'ask4v': data['f34'],
            'ask3p': data['f35'], 'ask3v': data['f36'], 'ask2p': data['f37'], 'ask2v': data['f38'],
            'ask1p': data['f39'], 'ask1v': data['f40'],
            'bid1p': data['f19'], 'bid1v': data['f20'], 'bid2p': data['f17'], 'bid2v': data['f18'],
            'bid3p': data['f15'], 'bid3v': data['f16'], 'bid4p': data['f13'], 'bid4v': data['f14'],
            'bid5p': data['f11'], 'bid5v': data['f12']
            }
    
    int_list = ['volume']
    float_list = ['preclose', 'open', 'high', 'low', 'last', 'avgp', 'amount']
    p_list = [f'{typ}{i}p' for typ in ['ask', 'bid'] for i in range(1,6)]
    v_list = [f'{typ}{i}v' for typ in ['ask', 'bid'] for i in range(1,6)]
    typ_dict = {k:np.float64 for k in float_list + p_list}
    typ_dict.update({k:np.int32 for k in int_list + v_list})
    
    for k,typ in typ_dict.items():
        v = rst[k]
        if isinstance(v, str):
            rst[k] = 0 if typ == np.int32 else np.nan
        else:
            rst[k] = typ(v)
    rst['amount'] = np.round(rst['amount']/10000, 2)
    return rst


#%%
#### 东方财富网，实时买卖五档盘口的数据，3S频率
class DCsnap:
    
    def __init__(self, db_dir):
        '''数据库文件存储路径db_dir'''
        self.db_dir = Path(db_dir)
        self.cached = {} #key=股票代码（str, no-SE格式）
    
    def get_snap(self, stk_str):
        '''获取可转债的盘口数据'''
        return dc_get_snap(stk_str)
    
    def cache_snap(self, stk_str):
        '''只保存到self.cached中'''
        rst = self.get_snap(stk_str)
        k = rst['code']
        self.cached.setdefault(k, [])
        self.cached[k].append(rst)
        return 
        
    def save_snap(self, stk_str, cached=False):
        '''保存到本地的文件中，同时可选是否保存到self.cached中'''
        rst = dc_get_snap(stk_str)
        if cached:
            k = rst['code']
            self.cached.setdefault(k, [])
            self.cached[k].append(rst)

        day = rst['timeindex'][0:10]
        fn_dir = self.db_dir.joinpath(f'{day}')
        if not fn_dir.exists():
            fn_dir.mkdir(parents=True)
        fn = fn_dir.joinpath(f'{stk_str}.csv')
        if not fn.exists():
            pd.DataFrame(rst, index=[0]).to_csv(fn, encoding='gbk', index=False)
        else: #追加模式进行数据录入
            pd.DataFrame(rst, index=[0]).to_csv(fn, encoding='gbk', mode='a', index=False, header=False)
        return rst
    
    def save_snap_multi(self, stk_iter, cached=False):
        '''存储含有多个stk_str的iteration'''
        rst_list = []
        for i,stk_str in enumerate(stk_iter):
            try:
                rst = self.save_snap(stk_str, cached)
                rst_list.append(rst)
                dt,lt = rst['timeindex'], rst['localtime']
                print(f'\rNo.{i:3d} {stk_str} {dt} @ local {lt} snapdata saved.', end='') 
            except Exception as e:
                print('\n', repr(e))
                print(f'\tError of No.{i} {stk_str}.')
        print()
        return 
    
    def save_cached(self, stk_str):
        '''存储cached中的某一只转债的多个snap数据, stk_str:128139'''
        dct = self.cached[stk_str]
        day = dct[0]['timeindex'][0:10]
        fn_dir = self.db_dir.joinpath(f'{day}')
        if not fn_dir.exists():
            fn_dir.mkdir(parents=True)
        fn = fn_dir.joinpath(f'{stk_str}.csv')
        pd.DataFrame(dct).to_csv(fn, encoding='gbk')
        return 
    
    def load_snap(self, day_str, stk_str):
        '''导入某只转债一天的snap数据, stk_str:128139'''
        fn = self.db_dir.joinpath(day_str, f'{stk_str}.csv')
        df = pd.read_csv(fn, encoding='gbk', engine='c')
        return df





    
#%% main
if __name__ == '__main__':
    dc = DCsnap('./DCsnap')
    pass











