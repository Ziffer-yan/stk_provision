# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 10:45:06 2022

@author: yhzhang
"""
import numpy as np 
import pandas as pd 
from pathlib import Path
import _pickle as cp
import multiprocessing as mp

import akshare as ak 
from AuxFunc import formal_day

#%% akshare class
'''可转债的数据接口，但是同时也支持对于可转债对应正股的数据获取
即stk_str可以为sz128106, sh600036, 代码前缀交给外部代码处理'''

class AKminute:
    
    def __init__(self, db_dir, mp_mng=False):
        self.db_dir = Path(db_dir)
        self.mp_mng = mp_mng
        self.cached = mp.Manager().dict() if mp_mng else {}
    
    def get_minute(self, stk_str):
        '''获取当天的可转债min数据（实时close数据，3S频率），使用ak.bond_zh_hs_cov_min接口'''
        df = ak.bond_zh_hs_cov_min(stk_str, period='1')
        col_dict = {'时间':'timeindex', '昨收':'preclose', '开盘':'open', '收盘':'close', '最高':'high',
                    '最低':'low', '成交量':'volume', '成交额':'amount', '最新价':'avg_price'} #最新价为累计成交均价
        df = df.rename(columns=col_dict)
        df['amount'] = np.round(df['amount'].values/10000, 2)
        return df
    
    def cache_minute(self, stk_str):
        '''把1min的数据保存在类的实例中, 返回最新数据的datetime'''
        df = self.get_minute(stk_str)
        self.cached[stk_str[2:]] = df
        dt = df['timeindex'].iat[-1]
        return dt
    
    def save_minute(self, stk_str, cached=False):
        '''存储1min的数据到csv文件中，可选是否同时保存至类实例中，返回最新数据的datetime'''
        df = self.get_minute(stk_str)
        if cached:
            self.cached[stk_str[2:]] = df
            
        dt = df['timeindex'].iat[-1]
        day = dt[:10]
        day_path = self.db_dir.joinpath(day)
        if not day_path.exists():
            day_path.mkdir(parents=True)
        fn = day_path.joinpath(f'{stk_str}.csv')
        df.to_csv(fn)
        return dt

    def cache_minute_multi(self, stk_iter):
        '''存储含有多个stk_str的iteration'''
        for i,stk_str in enumerate(stk_iter):
            try:
                dt = self.cache_minute(stk_str)
                print(f'\rNo.{i:3d} {stk_str} {dt} minute data cached.', end='')
            except Exception as e:
                print(repr(e))
                print(f'\tError of No.{i} {stk_str}.')
        print()
        return
    
    def save_minute_multi(self, stk_iter, cached=False):
        '''存储含有多个stk_str的iteration'''
        for i,stk_str in enumerate(stk_iter):
            try:
                dt = self.save_minute(stk_str, cached)
                print(f'\rNo.{i:3d} {stk_str} {dt} minute data saved.', end='')
            except Exception as e:
                print(repr(e))
                print(f'\tError of No.{i} {stk_str}.')
        print()
        return
    
    def load_minute(self, day_str, stk_str):
        '''读取day_str, stk_str返回df'''
        day_str = formal_day(day_str)
        fn = self.db_dir.joinpath(day_str, f'{stk_str}.csv')
        df = pd.read_csv(fn, index_col=0, engine='c')
        return df
    
    def load_minute_multi(self, day_list, stk_str):
        '''返回多天的day_str合并的df'''
        day_list = [formal_day(day_str) for day_str in day_list]
        fn_list = [self.db_dir.joinpath(day_str, f'{stk_str}.csv') for day_str in day_list]
        df_list = [pd.read_csv(fn, index_col=0, engine='c') for fn in fn_list]
        df = pd.concat(df_list, axis=0)
        return df
    
    def pickle_cache(self, day_str, prefix='data'):
        fn = self.db_dir.joinpath(f'{prefix}{day_str}.pkl')
        with open(fn, 'wb') as f:
            if self.mp_mng:
                cp.dump(self.cached.copy(), f)
            else:
                cp.dump(self.cached, f)
        return 

    def load_pkl(self, day_str, prefix='data'):
        fn = self.db_dir.joinpath(f'{prefix}{day_str}.pkl')
        with open(fn, 'rb') as f:
            rst = cp.load(f)
        return rst
        


class AKdaily:
    
    def __init__(self, db_dir):
        '''数据库文件存储路径db_dir'''
        self.db_dir = Path(db_dir)
        if not self.db_dir.exists():
            self.db_dir.mkdir(parents=True)
    
    def get_daily(self, stk_str):
        '''获取可转债的日线数据, 使用ak.bond_zh_hs_cov_daily接口'''
        df = ak.bond_zh_hs_cov_daily(stk_str)
        return df
    
    def save_daily(self, stk_str):
        '''存储获取的可转债数据'''
        df = self.get_daily(stk_str)
        fn = self.db_dir.joinpath(f'{stk_str}.csv')
        df.to_csv(fn)
        return 
    
    def save_daily_multi(self, stk_iter):
        '''存储含有多个stk_str的iteration'''
        for i,stk_str in enumerate(stk_iter):
            try:
                self.save_daily(stk_str)
                print(f'\rNo.{i} {stk_str} daily data saved.', end='') #\r在一行一直刷新
            except Exception as e:
                print(repr(e))
                print(f'\tError of No.{i} {stk_str}.')
        print()
        return 
        
    def load_daily(self, stk_str):
        '''读取stk_str返回df'''
        fn = self.db_dir.joinpath(f'{stk_str}.csv')
        df = pd.read_csv(fn, index_col=0, engine='c')
        return df
    
#%% main
if __name__ == '__main__':
    akm = AKminute('./AK1T')
    akd = AKdaily('./AK1D')
    pass








