import os
import pandas as pd
import multiprocessing
from datetime import datetime, timedelta
import xarray as xr
O3_path="E:/2023/weather/main/O3finish_2012-"

def convert_hours_since_1900_to_1980(hours_since_1900: int) :
    hours_12 = (datetime(1900, 1, 1, 0, 0) + timedelta(hours=hours_since_1900))
    hours_since_1980 = (hours_12 - datetime(1980, 1, 1, 8, 0)).total_seconds() / 3600
    return int(hours_since_1980)



xf = xr.open_dataset('E:\\2023\\weather\\main\\ERA_ozone_1980-2022.nc')
def O3_get(year):
    file_path=O3_path+"/allO3_"+str(year)+".csv"
    a=pd.read_csv(file_path,index_col=0)
    a = a.reset_index(drop=True)
    a["O3column"]=None
    for i in range(0,len(a)):
        timew=int(a.iloc[i,3])

        lat2=a.iloc[i,1]*2-40
        lon2=a.iloc[i,0]*2-200


        datd = xf['O3'].sel(longitude=int(lon2), latitude=int(lat2), time=convert_hours_since_1900_to_1980(timew)).values

        a.loc[i,"O3column"]=datd

    
        if i%1000==1:
            print(i/(len(a)))
    
    
    
    filename="O3column"+'_'+str(year)+'.csv'

    current_dir = os.getcwd() # 获取当前工作目录
    file_path = os.path.join(current_dir, 'forest_prepare2', filename) # 拼接文件路径
    a.to_csv(file_path) # 传入文件路径作为参数

if __name__ == '__main__':
    processes = []


    for i in range(1980,1990):
        p = multiprocessing.Process(target=O3_get, args=(i,))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()

