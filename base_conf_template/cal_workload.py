import random
import math
import numpy as np
import csv
import time
import matplotlib.pyplot as plt
import numpy as np
import math
import copy
def is_power_of_2(self, n):
        return n & (n - 1) == 0

def date_time_str_to_long(input_date_time_string):
    if input_date_time_string == 'None':
        return 0
    timeArray = time.strptime(input_date_time_string, "%Y-%m-%d %H:%M:%S")
    
    timeStamp = int(time.mktime(timeArray))
    
    return timeStamp


def _modify_to_2exponent(task_occupied_NIC_num):
    import math
    exponent = math.log2(task_occupied_NIC_num)
    exponent = math.ceil(exponent)
    exponent = max(1, exponent)
    task_occupied_NIC_num =  2 ** exponent

    return task_occupied_NIC_num


def generate_tasks(num):
    request_sequence = eval(open("../../base_conf_template/request_sequence","r").read())
    assert num <= len(request_sequence)
    task_info = request_sequence[:num]
    new_task_info = []
    arriving_time = 0
    for (_, model_size, task_occupied_NIC_num) in task_info:
        new_task_occupied_NIC_num = _modify_to_2exponent(task_occupied_NIC_num)
        new_task_info.append((arriving_time, model_size, new_task_occupied_NIC_num))
        # arriving_time += 0.1
    return new_task_info



def generate_helios_tpuv4_map(helios_distribution = {}, tpuv4_distribution = {}):
    if helios_distribution == {}:
        npu_num_map = {}
        total_num = 0
        with open('/home/gdp/hxc/rapidNetSim/base_conf_template/cluster_log_old.csv', encoding='utf-8-sig') as f:
            for row in csv.reader(f, skipinitialspace=True):
                if (row[6] == 'COMPLETED' or row[6] == 'CANCELLED'or row[6] == 'FAILED' or row[6] == 'NODE_FAIL'or row[6] == 'TIMEOUT' ) and int(row[3])>0 and date_time_str_to_long((row[7])) >= date_time_str_to_long("2020-06-03 00:00:00"): #2020-08-15 00:51:57 # 2020-06-02 08:51:57
                    if pow(2,math.ceil(math.log2(int(row[3])))) not in npu_num_map:
                        npu_num_map[pow(2,math.ceil(math.log2(int(row[3]))))] = 0
                    npu_num_map[pow(2,math.ceil(math.log2(int(row[3]))))] += 1
                    total_num += 1
        for key in npu_num_map:
            helios_distribution[key] = npu_num_map[key]/total_num
    if tpuv4_distribution == {}:
        tmp_list = [4,8,16,32,64,128,192,256,384,512,768,1024,1536,2048]
        dis = [0.05,0.1,0.05,0.09,0.43,0.605,0.612,0.729,0.73,0.849,0.856,0.893,0.895,1]
        dis_copy = [dis[i] for i in range(len(dis))]
        for i in range(1,len(dis)):
            dis[i] = dis[i] - dis_copy[i-1]
        for i in range(len(tmp_list)):
            tpuv4_distribution[tmp_list[i]] = dis[i]
            
    
    helios_distribution_list = list(helios_distribution.items ()) 
    helios_distribution_list.sort(key=lambda x:x[0]) 
    for index in range(len(helios_distribution_list)):
        helios_distribution_list[index] = list(helios_distribution_list[index])
    tpuv4_distribution_list = list(tpuv4_distribution.items ()) 
    tpuv4_distribution_list.sort(key=lambda x:x[0]) 
    for index in range(len(tpuv4_distribution_list)):
        tpuv4_distribution_list[index] = list(tpuv4_distribution_list[index])
    # print()
    # print("helios_distribution_list")
    # print(helios_distribution_list,len(helios_distribution_list))
    # print()
    # print("tpuv4_distribution_list")
    # print(tpuv4_distribution_list,len(tpuv4_distribution_list))
    helios_distribution_list_copy = copy.deepcopy(helios_distribution_list)
    ptr1 = 0
    ptr2 = 0
    heliso_npu_size_map = {}
    while ptr1<len(helios_distribution_list) and ptr2<len(tpuv4_distribution_list):
        if helios_distribution_list[ptr1][1] < tpuv4_distribution_list[ptr2][1]:
            if helios_distribution_list[ptr1][0] not in heliso_npu_size_map:
                heliso_npu_size_map[helios_distribution_list[ptr1][0]] = {}
            assert tpuv4_distribution_list[ptr2][0] not in heliso_npu_size_map[helios_distribution_list[ptr1][0]]
            heliso_npu_size_map[helios_distribution_list[ptr1][0]][tpuv4_distribution_list[ptr2][0]] = helios_distribution_list[ptr1][1]
            tpuv4_distribution_list[ptr2][1] = tpuv4_distribution_list[ptr2][1] - helios_distribution_list[ptr1][1]
            ptr1 += 1
        elif helios_distribution_list[ptr1][1] > tpuv4_distribution_list[ptr2][1]:
            if helios_distribution_list[ptr1][0] not in heliso_npu_size_map:
                heliso_npu_size_map[helios_distribution_list[ptr1][0]] = {}
            assert tpuv4_distribution_list[ptr2][0] not in heliso_npu_size_map[helios_distribution_list[ptr1][0]]
            heliso_npu_size_map[helios_distribution_list[ptr1][0]][tpuv4_distribution_list[ptr2][0]] = tpuv4_distribution_list[ptr2][1]
            helios_distribution_list[ptr1][1] = helios_distribution_list[ptr1][1] - tpuv4_distribution_list[ptr2][1] 
            ptr2 += 1
        else:
            ptr1 += 1
            ptr2 += 1
    # print("debug ptr", ptr1, ptr2, len(helios_distribution_list), len(tpuv4_distribution_list))
    # print()
    # print("result")
    # print(heliso_npu_size_map)
    # print(helios_distribution)
    for helios_index in heliso_npu_size_map:
        for to_map_index in heliso_npu_size_map[helios_index]:
            heliso_npu_size_map[helios_index][to_map_index] = heliso_npu_size_map[helios_index][to_map_index]/helios_distribution[helios_index]
    # print(heliso_npu_size_map)
    return heliso_npu_size_map


def randomly_chosen_accord_to_coff(class_map,random_seed):
    # print("debug randomly_chosen_accord_to_coff", class_map)
    class_map_copy = copy.deepcopy(class_map)
    cur_sum = 0
    for key in class_map_copy:
        class_map_copy[key] = cur_sum + class_map_copy[key]
        cur_sum = class_map_copy[key]
    random.seed(random_seed)
    ran_pro = random.uniform(0,1)
    chosen_res = -1
    for key in class_map_copy:
        if class_map_copy[key]>ran_pro:
            chosen_res = key
            break
    return chosen_res


def cal_load_level(_beta = 210, Cluster_size = 8192):
    map_relation = generate_helios_tpuv4_map()
    beta = _beta
    num = 5000
    waiting_list = []
    running_list = []
    gpu_list = []
    arrive_list = []
    arrive_interval_list = []
    new_task_info = []
    time_slot_gpu_num = {}
    slot_size = 1
    with open('/home/gdp/hxc/rapidNetSim/base_conf_template/cluster_log_old.csv', encoding='utf-8-sig') as f:
        start_time = 0
        index = 0
        for row in csv.reader(f, skipinitialspace=True):
            duration_time = 0
            waiting_time = 0
            if (row[6] == 'COMPLETED' or row[6] == 'CANCELLED'or row[6] == 'FAILED' or row[6] == 'NODE_FAIL'or row[6] == 'TIMEOUT' ) and int(row[3])>0 and date_time_str_to_long((row[7])) >= date_time_str_to_long("2020-06-03 00:00:00"): 
                if(start_time == 0):
                    start_time = date_time_str_to_long((row[7]))
                duration_time = date_time_str_to_long(row[9])-date_time_str_to_long(row[8])
                waiting_time = date_time_str_to_long(row[8])-date_time_str_to_long(row[7])
                running_list.append(duration_time)
                waiting_list.append(waiting_time)
                arrive_list.append(date_time_str_to_long((row[7]))-start_time)
                origional_gpu_size = min(512, math.ceil(math.log2(int(row[3]))))
                to_map_npu_size = randomly_chosen_accord_to_coff(map_relation[pow(2,origional_gpu_size)], index)
                to_map_npu_size =  min(512, to_map_npu_size)
                #to_map_npu_size = pow(2,math.ceil(math.log2(int(to_map_npu_size))))
                gpu_list.append(to_map_npu_size)
            index += 1
    data_set_size = len(gpu_list)
    rng = np.random.default_rng(0)
    exponential_interval_list = rng.exponential(beta, num)
    chosen_gpu_list = []
    job_class_num_map = {}
    job_arrive_rate_map = {}
    job_exec_time_map = {}
    last_arrive_time = 0

    work_load_list = []
    time_slot = num
    start_time = 0
    arriving_time = 0
    temp_work_load = 0
    for i in range(time_slot):
        task_occupied_NIC_num = int(gpu_list[i%data_set_size])
        chosen_gpu_list.append(task_occupied_NIC_num)
        if task_occupied_NIC_num not in job_class_num_map:
            job_class_num_map[task_occupied_NIC_num] = 0
            job_exec_time_map[task_occupied_NIC_num] = 0
        job_class_num_map[task_occupied_NIC_num] += 1
        
        total_val = max(1000,running_list[i%data_set_size]) 
        total_val = min(100000,total_val)
        # total_val = 100*max(1,int(total_val/100))
        total_val = 100*max(1,int(total_val/100))
        
        arriving_time += exponential_interval_list[i]

        job_exec_time_map[task_occupied_NIC_num] += total_val
        exp_finish_time = total_val + arriving_time
        start_slot = int(arriving_time/slot_size)
        end_slot = int(exp_finish_time/slot_size)
        for slot_id in range(start_slot, end_slot, 1):
            if slot_id not in time_slot_gpu_num:
                time_slot_gpu_num[slot_id] = 0
            time_slot_gpu_num[slot_id] += task_occupied_NIC_num
        # print(total_val)
        last_arrive_time = arriving_time
    for task_occupied_NIC_num in job_exec_time_map:
        job_exec_time_map[task_occupied_NIC_num] = job_exec_time_map[task_occupied_NIC_num]/job_class_num_map[task_occupied_NIC_num]
    for task_occupied_NIC_num in job_class_num_map:
        #job_arrive_rate_map[task_occupied_NIC_num] = job_class_num_map[task_occupied_NIC_num]/(last_arrive_time-start_time)
        job_arrive_rate_map[task_occupied_NIC_num] = 1/beta*job_class_num_map[task_occupied_NIC_num]/num
    for task_occupied_NIC_num in job_class_num_map:
        lanmda = 1/beta
        # print(task_occupied_NIC_num,job_exec_time_map[task_occupied_NIC_num])
        temp_work_load+= task_occupied_NIC_num*job_exec_time_map[task_occupied_NIC_num]*job_arrive_rate_map[task_occupied_NIC_num]/Cluster_size
        # temp_work_load+= task_occupied_NIC_num*job_exec_time_map[task_occupied_NIC_num]*job_arrive_rate_map[task_occupied_NIC_num]/Cluster_size
    work_load_list.append(temp_work_load)
    # work_load_list = work_load_list[round(len(work_load_list)*1/4): round(len(work_load_list)*3/4)]
    # work_load_list = work_load_list[round(len(work_load_list)*1/8): round(len(work_load_list)*8/4)]
    start_time = last_arrive_time
    print(np.mean(work_load_list))
    # x_list = []
    # y_list = []
    # for slot_id in time_slot_gpu_num:
    #     x_list.append(slot_id)
    #     y_list.append(time_slot_gpu_num[slot_id]/Cluster_size)
    # time_slot = num
    # start_time = 0
    # arriving_time = 0
    # temp_work_load = 0
    # for i in range(time_slot):
    #     task_occupied_NIC_num = int(gpu_list[i%data_set_size])
    #     chosen_gpu_list.append(task_occupied_NIC_num)
    #     if task_occupied_NIC_num not in job_class_num_map:
    #         job_class_num_map[task_occupied_NIC_num] = 0
    #         job_exec_time_map[task_occupied_NIC_num] = 0
    #     job_class_num_map[task_occupied_NIC_num] += 1
        
    #     total_val = max(1000,running_list[i%data_set_size]) 
    #     total_val = min(100000,total_val)
    #     total_val = 100*max(1,int(total_val/100))
        
    #     arriving_time += exponential_interval_list[i]
    #     f1 = open('job_exc_timeline.txt','a')
    #     f1.write(str(i) )
    #     f1.write(",")
    #     f1.write(str(total_val) )
    #     f1.write(",")
    #     f1.write(str((arriving_time) ))
    #     f1.write(",")
    #     f1.write(str((total_val + arriving_time) ))
    #     f1.write(",")
    #     time_slot_id = int(arriving_time/slot_size)
    #     f1.write(str((time_slot_gpu_num[time_slot_id]/Cluster_size)))
    #     f1.write(",")
    #     f1.write("\n" )
    #     f1.close()
    #     job_exec_time_map[task_occupied_NIC_num] += total_val
    #     exp_finish_time = total_val + arriving_time
    #     # print(total_val)
    #     last_arrive_time = arriving_time
    # plt.plot(x_list, y_list)
    # plt.rcParams['font.sans-serif'] = ['SimHei']
    # plt.xlabel("Time(s)", fontsize=24)
    # plt.ylabel("queue length", fontsize=24)
    # plt.legend()
    # plt.show()
cal_load_level(241, 8192)
cal_load_level(285, 8192)
cal_load_level(360, 8192)
cal_load_level(375, 8192)
cal_load_level(390, 8192)
cal_load_level(421, 8192)
cal_load_level(555, 8192)
