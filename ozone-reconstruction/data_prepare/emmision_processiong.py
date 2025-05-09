import xarray as xr
import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta
import calendar
import multiprocessing
def hours_since_1900_to_day_of_year(hours):
    # 定义起始日期
    start_date = datetime(1900, 1, 1)
    
    # 计算从起始日期到目标日期的差
    delta = timedelta(hours=hours)
    
    # 计算目标日期
    target_date = start_date + delta
    
    # 计算是该年的第几天
    day_of_year = target_date.timetuple().tm_yday
    
    return day_of_year-1

def calculate_days_difference(year, month):
    # 创建当前月15号的日期对象
    current_month_15 = datetime(year, month, 15)
    
    # 计算上个月15号的日期对象
    if month == 1:
        previous_month_15 = datetime(year - 1, 12, 15)
    else:
        previous_month_15 = datetime(year, month - 1, 15)
    
    # 计算下个月15号的日期对象
    if month == 12:
        next_month_15 = datetime(year + 1, 1, 15)
    else:
        next_month_15 = datetime(year, month + 1, 15)
    
    # 计算当前月1号的日期对象
    current_month_1 = datetime(year, month, 1)
    
    # 计算当前月最后一天的日期对象
    if month == 12:
        current_month_last = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        current_month_last = datetime(year, month + 1, 1) - timedelta(days=1)

    start_of_year = datetime(year, 1, 1)
    difference_to_start_of_year = (current_month_15 - start_of_year).days
    
    # 计算天数差
    difference_to_previous_month_15 = (current_month_15 - previous_month_15).days
    difference_to_next_month_15 = (next_month_15 - current_month_15).days
    difference_to_current_month_1 = (current_month_15 - current_month_1).days
    difference_to_current_month_last = (current_month_last - current_month_15).days
    
    return [difference_to_previous_month_15,difference_to_next_month_15,difference_to_current_month_1\
            ,difference_to_current_month_last,difference_to_start_of_year]
O3_path="E:/2023/weather/main/O3finish" 
emmision_name=["BC","CO","NH3","NOx","OC","PM10","PM25","RADM2_ALD","RADM2_CH4","RADM2_CSL","RADM2_ETH",\
   "RADM2_GLY","RADM2_HC3","RADM2_HC5","RADM2_HC8","RADM2_HCHO","RADM2_ISO","RADM2_KET","RADM2_MACR",\
    "RADM2_MGLY","RADM2_MVK","RADM2_NR","RADM2_NVOL","RADM2_OL2","RADM2_OLI","RADM2_OLT",\
    "RADM2_ORA1","RADM2_ORA2","RADM2_TOL","RADM2_XYL","SO2","VOC"]
def generate_emmision(p):
    year=2019
    for i in range(p,p+1):
        file_path3=O3_path+"/allO3_"+str(year)+".csv"
        a_to=pd.read_csv(file_path3,index_col=0)
        a_to = a_to.reset_index(drop=True)
        a_to[emmision_name[i]]=-9999.0

        if calendar.isleap(year):
            three_dimensional_array = np.empty([366,100,160])
        else:
            three_dimensional_array = np.empty([365,100,160])
        for month in range(1,13):
            if month==1:
                file_name_qian="E:\\2023\\weather\\main\\emmision\\"+str(year-1)+"_"+str(12)+"_total_"+emmision_name[i]+".nc"
            else:
                file_name_qian="E:\\2023\\weather\\main\\emmision\\"+str(year)+"_"+str(month-1).zfill(2)+"_total_"+emmision_name[i]+".nc"
            if month==12:
                file_name_hou="E:\\2023\\weather\\main\\emmision\\"+str(year+1)+"_"+str(1).zfill(2)+"_total_"+emmision_name[i]+".nc"
            else:
                file_name_hou="E:\\2023\\weather\\main\\emmision\\"+str(year)+"_"+str(month+1).zfill(2)+"_total_"+emmision_name[i]+".nc"
            file_name="E:\\2023\\weather\\main\\emmision\\"+str(year)+"_"+str(month).zfill(2)+"_total_"+emmision_name[i]+".nc"
            file_qian = xr.open_dataset(file_name_qian);z_values_qian = file_qian['z'].values.reshape(100, 160)
            file = xr.open_dataset(file_name);z_values = file['z'].values.reshape(100, 160)
            file_hou = xr.open_dataset(file_name_hou);z_values_hou = file_hou['z'].values.reshape(100, 160)
            x=calculate_days_difference(year,month)
            gardient_qian=(z_values-z_values_qian)
            gardient_hou=(z_values_hou-z_values)
            for k in range(1,x[2]+x[3]+2):
                if k <=15:
                    qfh=z_values_qian+gardient_qian*((x[0]-15+k)/x[0])
                else:
                    qfh=z_values+gardient_hou*((k-15)/x[1])
                three_dimensional_array[x[4]+k-15,:,:]=qfh

        for temp2 in range(0,len(a_to)):
            longitude2=a_to["longitude"][temp2];latitude2=a_to["latitude"][temp2];time2=int(a_to["time"][temp2])
            lon=int((longitude2-70)*2)
            lat=int((59.5-latitude2)*2)
            tim=hours_since_1900_to_day_of_year(time2)
            a_to.loc[temp2,emmision_name[i]]=three_dimensional_array[tim,lat,lon]
            if temp2%10000==1:
                print(temp2/len(a_to),i)
        
        current_dir = "F:\\ozone_reconstrution"
        filename=emmision_name[i]+'_'+str(year)+'.csv'
        file_path = os.path.join(current_dir, 'forest_prepare_emmision', filename) # 拼接文件路径
        a_to.to_csv(file_path) # 传入文件路径作为参数
    
if __name__ == '__main__':
    processes = []


    for ll in range(0,len(emmision_name)):
        p = multiprocessing.Process(target=generate_emmision, args=(ll,))
        processes.append(p)
        p.start()

