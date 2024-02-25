from ctypes import sizeof
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

np.set_printoptions(suppress=True)
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
# plt.rcParams['font.sans-serif'] = ['Times New Roman']

styles = ['-', '-.', '--', ':', 'solid',
          'dashed', 'dotted', 'dashdot', 'dashed']
markers = [' ', '>', '8', '*', 'x', '+', 'p', 'D']
colors = ["red", "green", "blue", "c", "cyan",
          "brown", "mediumvioletred", "dodgerblue", "orange"]


plt.rcParams['font.sans-serif'] = ['Times New Roman']

styles = ['-', '-.', '--', ':', 'solid',
          'dashed', 'dotted', 'dashdot', 'dashed']
markers = [' ', '>', '8', '*', 'x', '+', 'p', 'D']
colors = ["red", "orange", "blue", "c", "cyan",
          "brown", "mediumvioletred", "dodgerblue", "green"]



def get_two_res_from_csv(filename):
    with open(filename, 'r') as f:
        row_list = f.read().splitlines()
    
    queue_length_list = []
    time_list = []
    record_flag = False
    for i in range(1, len(row_list)):
        queue_length, time = row_list[i].split(',')
        queue_length = float(queue_length)
        time = float(time)
        if time > 0:
            record_flag = True
        if record_flag == True:
            #if network_fragmentation not in queue_length_list:
            queue_length_list.append(queue_length)
            time_list.append(time)
    return queue_length_list, time_list

if __name__ == '__main__':
    queue_length, time_list = get_two_res_from_csv('oxc_vclos/queue_length.txt')
    # queue_length2, time_list2 = get_two_res_from_csv('vclos/queue_length.txt')
    # queue_length3, time_list3 = get_two_res_from_csv('static_balance/queue_length.txt')
    queue_length4, time_list4 = get_two_res_from_csv('static_routing/queue_length.txt')
    # queue_length6, time_list6 = get_two_res_from_csv('static_ecmp/queue_length.txt')
    # queue_length7, time_list7 = get_two_res_from_csv('static_ecmp_random/queue_length.txt')
    plt.plot(time_list, queue_length, linestyle=styles[0], color=colors[0], label='OXC-vClos', linewidth=2.5)
    # plt.plot(time_list2, queue_length2, linestyle=styles[1], color=colors[1], label='vClos', linewidth=2.5)
    # plt.plot(time_list3, queue_length3, linestyle=styles[2], color=colors[2], label='best', linewidth=2.5)
    plt.plot(time_list4, queue_length4, linestyle=styles[3], color=colors[3], label='Source Routing', linewidth=2.5)
    # plt.plot(time_list6, queue_length6, linestyle=styles[3], color=colors[5], label='Balanced ECMP', linewidth=2.5)
    # plt.plot(time_list7, queue_length7, linestyle=styles[3], color=colors[6], label='ECMP', linewidth=2.5)


    plt.rcParams['font.sans-serif'] = ['SimHei']
    # plt.yticks(fontsize=24)
    # plt.xticks(fontsize=24)
    plt.xlabel("Time(s)", fontsize=24)
    plt.ylabel("queue length", fontsize=24)
    # plt.grid()
    # plt.tight_layout()
    plt.legend()
    plt.show()



