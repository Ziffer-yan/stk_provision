# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 13:14:26 2022

@author: yhzhang
"""
import numpy as np
import pandas as pd 
from pathlib import Path
import _pickle as cp
import multiprocessing as mp

import efinance as ef
from AuxFunc import formal_day


#%% efinance api
def ef_get_data(stk_str, klt=1, ts='20200101', te='20230101', fqt=0):
    '''
    同时可以获取股票，可转债的数据
    stk_str不能有se信息，正确输入: 128106, 600036
    ts,te输入不能有-，2022-08-29的输入方式无法获取数据（但不报错）
        参数对于klt != 1的数据均有用，但是对于分钟级别的数据能获取的历史数据有限
    fqt=0,1,2分别为不复权，前复权，后复权，复权的方式类似与同花顺
    klt=1,5,15,30,60,101,102,103分别为[1,5,15,30,60]min，1d，1w，1m
        其中5~60min只能返回最近30个左右交易日的数据，日、周、月的数据可以返回全部数据.
    '''
    if klt == 1:
        df = ef.stock.get_quote_history(stk_str, klt=1)
    else:
        df = ef.stock.get_quote_history(stk_str, beg=ts, end=te, klt=klt, fqt=fqt)
    col_dict = {'股票名称':'stk_nm', '股票代码':'stk_str', '日期':'timeindex', 
                '昨收':'preclose', '开盘':'open', '收盘':'close', '最高':'high',
                '最低':'low', '成交量':'volume', '成交额':'amount', '振幅':'amp', 
                '涨跌幅':'rtn', '涨跌额':'rtn_v', '换手率':'turnover'} #振幅 amplitude
    df = df.rename(columns=col_dict)
    if klt < 100:
        df['timeindex'] = df['timeindex'] + ':00'
    df['amount'] = np.round(df['amount'].values/10000, 2)
    return df


#%%
'''使用efinance对于minute以及day级别的数据进行存储
stk_str不能有SE信息，正确输入：128106, 600036'''

class EFminute:
    
    def __init__(self, db_dir, mp_mng=False):
        self.db_dir = Path(db_dir)
        self.mp_mng = mp_mng
        self.cached = mp.Manager().dict() if mp_mng else {}
    
    def get_minute(self, stk_str):
        '''获取当天的可转债min数据（实时close数据，3S频率），使用ef.stock.get_quote_history接口'''
        df = ef_get_data(stk_str, klt=1)
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
        if len(df) == 0:
            raise Exception(f'Empty df for {stk_str}')
        if cached:
            self.cached[stk_str[2:]] = df
            
        dt = df['timeindex'].iat[-1]
        day = dt[:10]
        day_path = self.db_dir.joinpath(day)
        if not day_path.exists():
            day_path.mkdir(parents=True)
        fn = day_path.joinpath(f'{stk_str}.csv')
        df.to_csv(fn, encoding='gbk')
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
    
    def load_minute(self, stk_str, day_str):
        '''读取day_str, stk_str返回df'''
        day_str = formal_day(day_str)
        fn = self.db_dir.joinpath(day_str, f'{stk_str}.csv')
        df = pd.read_csv(fn, index_col=0, encoding='gbk', engine='c')
        return df
    
    def load_minute_multi(self, stk_str, day_list):
        '''返回多天的day_str合并的df'''
        day_list = [formal_day(day_str) for day_str in day_list]
        fn_list = [self.db_dir.joinpath(day_str, f'{stk_str}.csv') for day_str in day_list]
        df_list = [pd.read_csv(fn, index_col=0, encoding='gbk', engine='c') for fn in fn_list]
        df = pd.concat(df_list, axis=0)
        return df
    
    def pickle_cache(self, day_str, prefix='data'):
        '''存储day_str的所有可转债的pkl数据'''
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



class EFdaily:
    
    def __init__(self, db_dir):
        '''数据库文件存储路径db_dir'''
        self.db_dir = Path(db_dir)

    def get_daily(self, stk_str, ts='20200101', te='20230101', fqt=0):
        '''
        获取可转债的日线数据, 使用ef.stock.get_quote_history接口
        klt=101为日频数据，fqt=0为不复权，1为前复权，2为后复权
        '''
        df = ef_get_data(stk_str, 101, ts, te, fqt)
        return df
    
    def save_daily(self, stk_str, ts='20200101', te='20230101', fqt=0):
        '''存储获取的可转债数据'''
        df = self.get_daily(stk_str, ts, te, fqt)
        if not self.db_dir.exists():
            self.db_dir.mkdir(parents=True)
        fn = self.db_dir.joinpath(f'{stk_str}.csv')
        df.to_csv(fn, encoding='gbk')
        return 
    
    def save_daily_multi(self, stk_iter, ts='20200101', te='20230101', fqt=0):
        '''存储含有多个stk_str的iteration'''
        for i,stk_str in enumerate(stk_iter):
            try:
                self.save_daily(stk_str, ts, te, fqt)
                print(f'\rNo.{i} {stk_str} daily data saved.', end='') #\r在一行一直刷新
            except Exception as e:
                print(repr(e))
                print(f'\tError of No.{i} {stk_str}.')
        print()
        return 
        
    def load_daily(self, stk_str):
        '''读取stk_str返回df'''
        fn = self.db_dir.joinpath(f'{stk_str}.csv')
        df = pd.read_csv(fn, index_col=0, encoding='gbk', engine='c')
        return df


# 只针对某一个klt的数据进行获取和存储
class EFkline:
    
    def __init__(self, db_dir, klt=5):
        self.db_dir = Path(db_dir)
        self.klt = klt
        self.fq_dict = {1:'1T', 5:'5T', 15:'15T', 30:'30T', 
                     60:'1H', 101:'1D', 102:'1W', 103:'1M'}
        
    def get_data(self, stk_str, ts='20200101', te='20230101', fqt=0):
        df = ef_get_data(stk_str, self.klt, ts, te, fqt)
        return df

    def save_data(self, stk_str, ts='20200101', te='20230101', fqt=0):
        df = self.get_data(stk_str, ts, te, fqt)
        if not self.db_dir.exists():
            self.db_dir.mkdir(parents=True)
        fn = self.db_dir.joinpath(f'{stk_str}.csv')
        df.to_csv(fn, encoding='gbk')
        return 
    
    def save_data_multi(self, stk_iter, ts='20200101', te='20230101', fqt=0):
        '''存储含有多个stk_str的iteration'''
        for i,stk_str in enumerate(stk_iter):
            try:
                self.save_daily(stk_str, ts, te, fqt)
                print(f'\rNo.{i} {stk_str} daily data saved.', end='') #\r在一行一直刷新
            except Exception as e:
                print(repr(e))
                print(f'\tError of No.{i} {stk_str}.')
        print()
        return 
    
    def load_data(self, stk_str):
        fn = self.db_dir.joinpath(f'{stk_str}.csv')
        df = pd.read_csv(fn, index_col=0, encoding='gbk', engine='c')
        return df

#%% main
if __name__ == '__main__':
    
    efm = EFminute('./EF1T')
    efd = EFdaily('./EF1D')
    efk = EFkline('./EF5T', 5)
    pass


