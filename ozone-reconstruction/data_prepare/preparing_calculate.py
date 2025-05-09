import numpy as np
import pandas as pd
import os
import netCDF4 as nc
from datetime import datetime, timedelta
import xarray as xr
from dateutil.relativedelta import relativedelta
import multiprocessing

def ear_to_time(x):
    delta = timedelta(hours=x)

# Define the reference date
    ref_date = datetime(1900, 1, 1, 0, 0, 0)

# Add the timedelta to the reference date
    dt = ref_date + delta
    return dt

def hours_since(year, month):
    # 创建一个 datetime 对象，表示 1900 年 1 月 1 日 00:00:00
    start = datetime(1900, 1, 1)

    # 创建一个 datetime 对象，表示给定的年份和月份的第一天
    end = datetime(year, month, 1)

    # 计算两个 datetime 对象之间的差值（以秒为单位），然后转换为小时
    hours = (end - start).total_seconds() / 3600

    return hours

#这段代码在分类好年份后，用来将EAR5的数据整合进O3的dataframe

level_path = "E:/2023/weather/main/eralevel"
for filename in os.listdir(level_path):
    file_path = os.path.join(level_path, filename)
    if os.path.isfile(file_path):
        nf = nc.Dataset(file_path, 'r')
        xf = xr.open_dataset(file_path)
        

O3_path="E:/2023/weather/main/O3finish_2012-"
level_path = "F:/ERA/eragrid_high0.5"
def factor_prepare (ear_name,layer_name):
    for filename in os.listdir(O3_path):
        file_path = os.path.join(O3_path, filename)
        if os.path.isfile(file_path):
            a=pd.read_csv(file_path,index_col=0)
            a = a.reset_index(drop=True)
            a[ear_name]=None
            year_want=str(filename)[6:10]
            k=0
            for i in range(1,13):
                print("month:",i,"year",year_want)
                if i<=9:
                    levename="Era5_level_"+str(year_want)+"0"+str(i)+".nc"
                else:
                    levename="Era5_level_"+str(year_want)+str(i)+".nc"
                levelfile_path=os.path.join(level_path,levename)
                xf = xr.open_dataset(levelfile_path)
                start = datetime(1900, 1, 1)
                endmax = datetime(int(year_want), int(i), 1) + relativedelta(months=1)
                endmin = datetime(int(year_want), int(i), 1)
                # 计算两个 datetime 对象之间的差值（以秒为单位），然后转换为小时
                hoursmax = (endmax - start).total_seconds() / 3600-1
                hoursmin= (endmin - start).total_seconds() / 3600-1
                if k>=len(a):
                    break
                while a["time"][k]<hoursmax:
                    longitude2=a["longitude"][k];latitude2=a["latitude"][k];time2=a["time"][k]
                    try:
                        datd = xf[ear_name].sel(longitude=longitude2, latitude=latitude2, level=layer_name, time=ear_to_time(int(time2)))
                        if datd!=-32767:
                            a.loc[k, ear_name] = float(datd)
                            k=k+1
                            if k>=len(a):
                                break
                        else:
                            k=k+1
                            if k>=len(a):
                                break
                    except:
                        k=k+1
                        if k>=len(a):
                                break

            filename=str(ear_name)+str(layer_name)+'_'+str(year_want)+'.csv'

            #current_dir = os.getcwd() # 获取当前工作目录
            current_dir="F:\\ozone_reconstrution"
            file_path = os.path.join(current_dir, 'forest_prepare', filename) # 拼接文件路径
            a.to_csv(file_path) # 传入文件路径作为参数

            
            
#这一段代码用来循环读取EAR5数据中的各类参数,分多线程处理成csv。
factor_list=['d', 'z', 'r', 't', 'u', 'v', 'w']
layer_list=[200,500,700,850,925,1000]
def prepare_threading(arg):
    i=arg[0];j=arg[1]
    factor_prepare(factor_list[i],layer_list[j])

if __name__ == '__main__':
    processes = []

    for j in range(0,2):
        for i in range(0,len(factor_list)):
            p = multiprocessing.Process(target=prepare_threading, args=([i,j],))
            processes.append(p)
            p.start()

        # Ensure all of the processes have finished
    for p in processes:
        p.join()

    for j in range(2,4):
        for i in range(0,len(factor_list)):
            p = multiprocessing.Process(target=prepare_threading, args=([i,j],))
            processes.append(p)
            p.start()

        # Ensure all of the processes have finished
    for p in processes:
        p.join()

    for j in range(4,6):
        for i in range(0,len(factor_list)):
            p = multiprocessing.Process(target=prepare_threading, args=([i,j],))
            processes.append(p)
            p.start()

        # Ensure all of the processes have finished
    for p in processes:
        p.join()