#-- coding:UTF-8 --
from cgi import print_form
from ctypes import sizeof
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os
import math
import random
import csv
import time
import numpy as np

np.set_printoptions(suppress=True)
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
# plt.rcParams['font.sans-serif'] = ['Times New Roman']

styles = ['-', '-.', '--', ':', 'solid',
          'dashed', 'dotted', 'dashdot', 'dashed']
markers = [' ', '>', '8', '*', 'x', '+', 'p', 'D']
colors = ["red", "green", "blue", "c", "cyan",
          "brown", "mediumvioletred", "dodgerblue", "orange"]



def date_time_str_to_long(input_date_time_string):
    if input_date_time_string == 'None':
        return 0
    timeArray = time.strptime(input_date_time_string, "%Y-%m-%d %H:%M:%S")
    
    timeStamp = int(time.mktime(timeArray))
    
    return timeStamp



def load_csv_get_beta(filepath):
    df = pd.read_csv(filepath, header = None)
    df.columns = ['taskidname', 'taskid', 'type', 'value']
    return df


def get_completion_time(df_data, task_num):
    res_list = []
    for i in range(task_num):
        start_time = df_data.loc[(df_data['taskid'] == i) & (df_data['type'] == 'start_time')]['value'].values[0]
        finish_time = df_data.loc[(df_data['taskid'] == i) & (df_data['type'] == 'finish_time')]['value'].values[0]
        res_list.append(finish_time - start_time)
    
    return res_list

def get_finish_time(df_data, task_num):
    res_list = []
    for i in range(task_num):
        arriving_time = df_data.loc[(df_data['taskid'] == i) & (df_data['type'] == 'arriving_time')]['value'].values[0]
        # print("debug i",i)
        finish_time = df_data.loc[(df_data['taskid'] == i) & (df_data['type'] == 'finish_time')]['value'].values[0]
        res_list.append(finish_time - arriving_time)

    return res_list

def get_wait_time(df_data, task_num):
    res_list = []
    for i in range(task_num):
        arriving_time = df_data.loc[(df_data['taskid'] == i) & (df_data['type'] == 'arriving_time')]['value'].values[0]
        start_time = df_data.loc[(df_data['taskid'] == i) & (df_data['type'] == 'start_time')]['value'].values[0]
        res_list.append(start_time - arriving_time)

    return res_list


def draw_cdf_from_dict(data_dict):
    """绘制CDF图
    Input: 接受任意数量的数据，key充当画图的图例，value是画图用的原始数据
    """
    # plt.figure(figsize=(6, 4))
    # 适配曲线数量
    count = 0
    for k, data in data_dict.items():
        data = list(data)
        y = data
        x = [i for i in range(len(y))]
        plt.plot(x, y, label=k,
                 linestyle=styles[count], color=colors[count], linewidth=2.5)

        count += 1

    # plt.ylim(0.8, 1)
    # plt.xlim(0, 500)
    plt.yticks(fontsize=24)
    plt.xticks(fontsize=24)
    # plt.yscale("symlog", linthreshy=0.0001)
    plt.xlabel("Taskid", fontsize=24)
    plt.ylabel("Completion Time", fontsize=24)
    plt.grid()
    plt.legend(bbox_to_anchor=(0, 1, 1, 0), loc="lower center", fontsize=23,
               mode="expand", borderaxespad=0, ncol=2, frameon=False, 
               handletextpad=0.1, handlelength=1)
    return plt


task_nums = 5000

oxc_vclos = load_csv_get_beta('oxc_vclos/task_time.log')

oxc_vclos2 = get_wait_time(oxc_vclos, task_nums)
oxc_vclos3 = get_finish_time(oxc_vclos, task_nums)
oxc_vclos1 = get_completion_time(oxc_vclos, task_nums)

print("JRT",end="&")
print(sum(oxc_vclos1)*1/len(oxc_vclos1),end="&")

print()
print("JWT",end="&")
print(sum(oxc_vclos2)*1/len(oxc_vclos2),end="&")

oxc_waiting_num = 0
for i in oxc_vclos2:
    if i>10:
        oxc_waiting_num +=1
print("oxc waiting ratio: ",oxc_waiting_num/task_nums)
print()
print("JCT",end="&")
print(sum(oxc_vclos3)*1/len(oxc_vclos3),end="&")
# print(oxc_vclos3)
print()
