# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 10:47:40 2022

@author: yhzhang
"""
import datetime as dtm

#%%
def t_now(day=0, us=0):
    '''获取当前时间datetime/time并将其转变为字符串
    '''
    t = dtm.datetime.now()
    X = f'{t.hour:02d}:{t.minute:02d}:{t.second:02d}'
    if day == 0: 
        if us == 0:
            t_str = X
        else:
            f = f'{t.microsecond:06d}'
            t_str = X + '.' + f
    else:
        Y = f'{t.year}-{t.month:02d}-{t.day:02d}'
        if us == 0:
            t_str = Y + ' ' + X
        else:
            f = f'{t.microsecond:06d}'
            t_str = Y + ' ' + X + '.' + f
    return t_str

def add_cbse(cb_str, prefix=True):
    '''给可转债添加上交易所信息, prefix决定是添加前缀还是后缀'''
    if cb_str[:2] == '11':
        s = 'sh'
    elif cb_str[:2] == '12':
        s = 'sz'
    else:
        raise Exception(f'Can not figure out SE for {cb_str}')
    if prefix:
        rst = s + cb_str
    else:
        rst = cb_str + '.' + s.upper()
    return rst

def add_uase(ua_str, prefix=True):
    '''给股票添加上交易所信息'''
    if ua_str[0] == '6':
        s = 'sh'
    else:
        s = 'sz'
    if prefix:
        rst = s + ua_str
    else:
        rst = ua_str + '.' + s.upper()
    return rst

def formal_day(day_str):
    '''给day_str加上-符号'''
    if '-' in day_str:
        return day_str
    else:
        return '-'.join([day_str[:4], day_str[4:6], day_str[6:8]])
    

