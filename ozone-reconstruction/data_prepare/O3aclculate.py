import numpy as np
import pandas as pd
import os
import netCDF4 as nc
from datetime import datetime, timedelta
import xarray as xr
import multiprocessing
level_path = "E:/2023/weather/main/eralevel"
for filename in os.listdir(level_path):
    file_path = os.path.join(level_path, filename)
    if os.path.isfile(file_path):
        nf = nc.Dataset(file_path, 'r')


print(nf.variables.keys())
print(nf.variables['time'][:])

o3_path = 'E:/2023/weather/main/erasurface'
dataset=nf
# 获取时间变量
time_var = dataset.variables["time"]
# 获取时间单位和日历类型
time_units = time_var.units
time_calendar = time_var.calendar
# 将时间变量转换为datetime对象
time_data = time_var[:]
time_datetime = nc.num2date(time_data, units=time_units, calendar=time_calendar)
# 将datetime对象转换为北京时间
time_datetime_beijing = time_datetime 
# 打印结果
print( time_datetime_beijing[1])

def get_05(op):
    df = pd.DataFrame(columns=[f"col{i}" for i in range(0, 6)])
    for i in range(0,len(op)):
        if op.iloc[i,1]<99.9:
            continue
        if op.iloc[i,1]>123.1:
            continue
        if op.iloc[i,2]<19.9:
            continue
        if op.iloc[i,2]>43.1:
            continue
        integer_part0, decimal_part0 = divmod(op.iloc[i,1], 1)
        decimal_part0 = round(decimal_part0, 4)
        if decimal_part0 == 0.95:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.95:
                lie=str(integer_part0+1)+str(integer_part1+1)
                df.loc[lie,"col0"]=op.iloc[i,3]

                df.loc[lie,"col4"]=integer_part0+1
                df.loc[lie,"col5"]=integer_part1+1
        
        if decimal_part0 == 0.95:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.05:
                lie=str(integer_part0+1)+str(integer_part1)
                df.loc[lie,"col1"]=op.iloc[i,3]    

        if decimal_part0 == 0.05:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.95:
                lie=str(integer_part0)+str(integer_part1+1)
                df.loc[lie,"col2"]=op.iloc[i,3]

        if decimal_part0 == 0.05:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.05:
                lie=str(integer_part0)+str(integer_part1)
                df.loc[lie,"col3"]=op.iloc[i,3]



        if decimal_part0 == 0.55:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.95:
                lie=str(integer_part0+0.5)+str(integer_part1+1)
                df.loc[lie,"col0"]=op.iloc[i,3]

                df.loc[lie,"col4"]=integer_part0+0.5
                df.loc[lie,"col5"]=integer_part1+1
        
        if decimal_part0 == 0.55:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.05:
                lie=str(integer_part0+0.5)+str(integer_part1)
                df.loc[lie,"col1"]=op.iloc[i,3]    

        if decimal_part0 == 0.45:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.95:
                lie=str(integer_part0+0.5)+str(integer_part1+1)
                df.loc[lie,"col2"]=op.iloc[i,3]

        if decimal_part0 == 0.45:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.05:
                lie=str(integer_part0+0.5)+str(integer_part1)
                df.loc[lie,"col3"]=op.iloc[i,3]


        if decimal_part0 == 0.95:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.55:
                lie=str(integer_part0+1)+str(integer_part1+0.5)
                df.loc[lie,"col0"]=op.iloc[i,3]

                df.loc[lie,"col4"]=integer_part0+1
                df.loc[lie,"col5"]=integer_part1+0.5
        
        if decimal_part0 == 0.95:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.45:
                lie=str(integer_part0+1)+str(integer_part1+0.5)
                df.loc[lie,"col1"]=op.iloc[i,3]    

        if decimal_part0 == 0.05:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.55:
                lie=str(integer_part0)+str(integer_part1+0.5)
                df.loc[lie,"col2"]=op.iloc[i,3]

        if decimal_part0 == 0.05:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.45:
                lie=str(integer_part0)+str(integer_part1+0.5)
                df.loc[lie,"col3"]=op.iloc[i,3]


        if decimal_part0 == 0.55:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.55:
                lie=str(integer_part0+0.5)+str(integer_part1+0.5)
                df.loc[lie,"col0"]=op.iloc[i,3]

                df.loc[lie,"col4"]=integer_part0+0.5
                df.loc[lie,"col5"]=integer_part1+0.5
        
        if decimal_part0 == 0.55:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.45:
                lie=str(integer_part0+0.5)+str(integer_part1+0.5)
                df.loc[lie,"col1"]=op.iloc[i,3]    

        if decimal_part0 == 0.45:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.55:
                lie=str(integer_part0+0.5)+str(integer_part1+0.5)
                df.loc[lie,"col2"]=op.iloc[i,3]

        if decimal_part0 == 0.45:
            integer_part1, decimal_part1 = divmod(op.iloc[i,2], 1)
            decimal_part1 = round(decimal_part1, 4)
            if decimal_part1 == 0.45:
                lie=str(integer_part0+0.5)+str(integer_part1+0.5)
                df.loc[lie,"col3"]=op.iloc[i,3]





    df['average'] = df[['col1', 'col2', 'col3', 'col0']].mean(axis=1)
    df.drop(['col1', 'col2', 'col3', 'col0'], axis=1, inplace=True)
    df.dropna(inplace=True)

    return df  


#这一段代码用来使用来将csv中的数据读转换为0.5*0.5的格点数据
def target2(yearsd):
    M8hO3_path = "E:/2023/weather/main/tapO3"
    M8hO3_path=M8hO3_path+str(yearsd)
    df1 = pd.DataFrame(columns=["longitude","latitude","average","time"])
    time_var = dataset.variables["time"]
    time_units = time_var.units
    time_calendar = time_var.calendar
    for filename in os.listdir(M8hO3_path):
        file_path = os.path.join(M8hO3_path, filename)

        if os.path.isfile(file_path):
            hours=6
            op=pd.read_csv(file_path)
            df2=get_05(op)
            year = int(filename[0:4])
            month = int(filename[4:6])
            day = int(filename[6:8])
            hour = hours
            time_units = time_var.units
            time_calendar = time_var.calendar
            time_datetime = datetime(year, month, day, hour)
            time_data = nc.date2num(time_datetime, units=time_units, calendar=time_calendar)
            df2["time"]=time_data
            # df4=df2.copy()
            # for i in range(1,4):

            #     hour = hours[i]

            #     time_datetime = datetime(year, month, day, hour)
            #     time_data = nc.date2num(time_datetime, units=time_units, calendar=time_calendar) 
            #     df3=df4
            #     df3["time"]=time_data
            #     df2 = pd.concat([df2, df3])
        df2.rename(columns={'col4': 'longitude'}, inplace=True)
        df2.rename(columns={'col5': 'latitude'}, inplace=True)
        print(filename)
        df1 = pd.concat([df1, df2])
        tup="allO3_"+str(yearsd)+".csv"
    df1.to_csv(tup)






if __name__ == '__main__':
    processes = []

 
    for i in range(2022,2023):
        p = multiprocessing.Process(target=target2, args=(i,))
        processes.append(p)
        p.start()

    # Ensure all of the processes have finished
    for p in processes:
        p.join()