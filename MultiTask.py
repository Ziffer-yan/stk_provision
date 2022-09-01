# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 11:07:51 2022

@author: yhzhang
"""
import numpy as np 
import pandas as pd 
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, as_completed
from AuxFunc import t_now


#%% multi task
def mp_process(func, stk_list, N_procs=8, N_loops=1, end_func=None, *args, **kwargs):  
    '''func需要进行wrapper，只留下接受一个参数的位置。
    多进程的cpu占用不高，但是内存的开销很大'''
    stk_idx = np.linspace(0, len(stk_list), N_procs+1).astype(int)
    print('program start @', t_now(1,1))
    
    with mp.Pool(processes=N_procs) as p:
        for time_i in range(N_loops):
            print(time_i, 'start @', t_now())
            rst_list = []
            for i in range(N_procs):
                arg = stk_list[stk_idx[i]:stk_idx[i+1]]
                rst = p.apply_async(func, (arg,))
                rst_list.append(rst)
            rst_end = [rst.get() for rst in rst_list] #单次立即执行
            if not end_func is None:
                end_func(*args, **kwargs)
            print(time_i, 'end @', t_now())
            print('-'*100)
        p.close()
        p.join()
        
    print('program end @', t_now(1,1))
    return 


def mp_thread(func, stk_list, N_thread=16, N_loops=1, end_func=None, *args, **kwargs):
    '''func需要进行wrapper，只留下接受一个参数的位置。
    多线程的cpu占用稍高，但是内存占用显著降低'''
    stk_idx = np.linspace(0, len(stk_list), N_thread+1).astype(int)
    print('program start @', t_now(1,1))
    
    with ThreadPoolExecutor(max_workers=N_thread) as p:
        for time_i in range(N_loops):
            print(time_i, 'start @', t_now())
            rst_list = []
            for i in range(N_thread):
                arg = stk_list[stk_idx[i]:stk_idx[i+1]]
                future = p.submit(func, arg)
                rst_list.append(future)
            rst_end = [future.result() for future in as_completed(rst_list)] #调用result阻塞
                
            if not end_func is None:
                end_func(*args, **kwargs)
            print(time_i, 'end @', t_now())
            print('-'*100)
        p.shutdown(wait=False)
        
    print('program end @', t_now(1,1))
    return 

#%% run here
from AKapi import AKminute
from EFapi import EFminute, EFdaily
from DCapi import DCsnap
from functools import partial
from pathlib import Path
from AuxFunc import add_cbse, add_uase

def AK1T_save(stk_list, day_str, db_dir='./AK1T', pkl_prfx='data',
              N_thread=16, N_loops=1):
    akm = AKminute(db_dir)
    mp_thread(partial(akm.save_minute_multi, cached=True), 
              stk_list, N_thread=N_thread, N_loops=N_loops, 
              end_func=partial(akm.pickle_cache, day_str, prefix=pkl_prfx))
    return akm

def EF1T_save(stk_list, day_str, db_dir='./EF1T', pkl_prfx='data',
              N_thread=16, N_loops=1):
    efm = EFminute(db_dir)
    mp_thread(partial(efm.save_minute_multi, cached=True), 
              stk_list, N_thread=N_thread, N_loops=N_loops, 
              end_func = partial(efm.pickle_cache, day_str, prefix=pkl_prfx))
    return efm

def EF1D_save(stk_list, db_dir='./EF1D', ts='20210101', te='20230101', fqt=0,
              N_thread=16, N_loops=1):
    efd = EFdaily('./EF1D')
    mp_thread(partial(efd.save_daily_multi, ts=ts, te=te, fqt=fqt),
              stk_list, 
              N_thread=N_thread, N_loops=N_loops, end_func=None)
    return efd

def DCsnap_save(stk_list, cached=False, db_dir='./DCsnap',
                N_thread=16, N_loops=1):
    dc = DCsnap(db_dir)
    mp_thread(partial(dc.save_snap_multi, cached=cached),
              stk_list, 
              N_thread=N_thread, N_loops=N_loops, end_func=None)
    return dc

#%% main_func


if __name__ == '__main__':
    day_str = '2022-09-01'
    for f in Path(f'../VSWS/jisilu/{day_str}').iterdir():
        params = pd.read_csv(f, index_col=0, encoding='gbk', engine='c')
        break
    assert len(params)>300, "Error for not reading params_df."
    cb_list = params['转债代码'].map(str).tolist()
    cbse_list = [add_cbse(s) for s in cb_list]
    ua_list = params['正股代码'].map(str).str[1:].tolist()
    uase_list = [add_uase(s) for s in ua_list]
    
    # raise Exception
    
    # akm = AK1T_save(cbse_list, day_str, db_dir='./DataMinute', pkl_prfx='cb',
    #                 N_thread=16, N_loops=1)
    # akm = AK1T_save(uase_list, day_str, db_dir='./DataMinute', pkl_prfx='ua',
    #                 N_thread=16, N_loops=1)
    
    # efm = EF1T_save(cb_list, day_str, db_dir='./EF1T', pkl_prfx='cb',
    #                 N_thread=16, N_loops=1)
    # efm = EF1T_save(ua_list, day_str, db_dir='./EF1T', pkl_prfx='ua',
    #                 N_thread=16, N_loops=1)
    
    # efd = EF1D_save(cb_list+ua_list, db_dir='./EF1D', ts='20210101', te='20230101', fqt=0,
    #                 N_thread=16, N_loops=1)
    
    dc = DCsnap_save(cbse_list, cached=False, db_dir='./DCsnap',
                     N_thread=16, N_loops=1)
    
    
    pass
