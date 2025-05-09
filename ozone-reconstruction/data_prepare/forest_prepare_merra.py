import numpy as np
import pandas as pd
import os
import netCDF4 as nc
from datetime import datetime, timedelta
import xarray as xr
from dateutil.relativedelta import relativedelta
import multiprocessing
time_units = 'hours since 1900-01-01 00:00:00.0'
calendar = 'gregorian'
O3_path="E:/2023/weather/main/O3finish_2012-"
surface_path = "F:/ERA/eragrid_surface0.5"
merra_path="F:/merra_aer"
def factor_prepare (merra_name,year):
    file_path = "E:\\2023\\weather\\main\\O3finish_2012-\\allO3_"+str(year)+".csv"
    a=pd.read_csv(file_path,index_col=0)
    a = a.reset_index(drop=True)
    a[merra_name]=None

    for k in range(0,len(a)):
        time_value = a["time"][k]  
        # 将时间转换为日期
        date = nc.num2date(time_value, units=time_units, calendar=calendar)
        # 生成带日期的文件名
        file_template = "MERRA2_100.tavg1_2d_aer_Nx.{}.nc4.nc4"
        file_name = file_template.format(date.strftime("%Y%m%d"))

        filename="F:\\merra_aer\\"+file_name


        dataset = nc.Dataset(filename, 'r')

        # 获取经度、纬度、时间的数据
        lon = dataset.variables['lon'][:]
        lat = dataset.variables['lat'][:]

        # 设置你感兴趣的时间、经度、纬度
        selected_time_index = 3  # 第四个时间点
        selected_lon=a["longitude"][k];selected_lat=a["latitude"][k]

        # 找到最接近指定经度和纬度的索引
        lon_index = np.abs(lon - selected_lon).argmin()
        lat_index = np.abs(lat - selected_lat).argmin()

        # 提取第一个变量在指定时间和经纬度下的数据
        first_variable = merra_name
        datd = dataset.variables[first_variable][selected_time_index, lat_index, lon_index]
        a.loc[k, merra_name] = float(datd)

        if k%1000==1:
            print(k/len(a))


        

    filename2=str(merra_name)+'_'+str(year)+'.csv'

    current_dir = "E:\\2023\\weather\\main"
    file_path = os.path.join(current_dir, 'forest_prepare_merra', filename2) # 拼接文件路径
    a.to_csv(file_path) # 传入文件路径作为参数

#这一段代码用来循环读取EAR5数据中的各类参数,分多线程处理成csv。
factor_list= ['SSSMASS25','BCSMASS','SO4CMASS','DUSMASS']
layer_list=[1990]


def prepare_threading(arg):
    i=arg[0];j=arg[1]
    factor_prepare(factor_list[i],layer_list[j])

if __name__ == '__main__':
    processes = []




    for j in range(0,1):
        for i in range(0,len(factor_list)):
            p = multiprocessing.Process(target=prepare_threading, args=([i,j],))
            processes.append(p)
            p.start() 
    for p in processes:
        p.join()
