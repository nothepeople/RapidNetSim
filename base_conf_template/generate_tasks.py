import random
import math
import numpy as np
import csv
import time
import matplotlib.pyplot as plt
import numpy as np
import math
import copy

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
    
            


Large_model_list = ["Pangu"]
Small_model_list = ["VGG16_4","VGG16_8","VGG16_16","VGG16_32","VGG16_64","VGG16_96",
                    "ResNet50_16","ResNet50_32","ResNet50_64","ResNet50_96","ResNet50_128",
                    "ResNet101_16","ResNet101_32","ResNet101_64","ResNet101_96",
                    "Bert_16","Bert_32"
                    ]

ALL_REDUCE_COST = {"VGG16_4":0.25,"VGG16_8":0.23,"VGG16_16":0.16,"VGG16_32":0.1,"VGG16_64":0.07,"VGG16_96":0.04,
                    "ResNet50_4":0.04,"ResNet50_8":0.04,"ResNet50_16":0.04,"ResNet50_32":0.05,"ResNet50_64":0.04,"ResNet50_96":0.03,"ResNet50_128":0.02,
                    "ResNet101_4":0.05,"ResNet101_8":0.05,"ResNet101_16":0.05,"ResNet101_32":0.05,"ResNet101_64":0.04,"ResNet101_96":0.02,
                    "Bert_4":0.12,"Bert_8":0.12,"Bert_16":0.12,"Bert_32":0.08,
                    "Pangu":0.0737
                    }
ALL2ALL = {
                    "Pangu":0.258
                    }
ALLGATHER = {
                    "Pangu":0.0158
                    }
ReduceScatter = {
                    "Pangu":0.0002
                    }
Algo_Type = {
    "ALL2ALL":[0],
    "ALLREDUCE":[3,4]
}

def is_power_of_2(n):
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


def generate_big_custom_tasks(num, exponential_beta = 0.1, fixed_model_size = None):
    new_task_info = []
    arriving_time = 0

    base_NIC_num_list = [1, 512, 1024, 256, 512]
    num_times_list = [50, 20, 50, 60, 500]
    
    task_occupied_NIC_num_list = []
    for i in range(len(num_times_list)):
        for _ in range(num_times_list[i]):
            task_occupied_NIC_num_list.append(base_NIC_num_list[i])
    random.seed(0)
    random.shuffle(task_occupied_NIC_num_list)

    if exponential_beta != False:
        rng = np.random.default_rng(0)
        exponential_interval_list = rng.exponential(exponential_beta, num)

    j = 0
    last_arriving_time = arriving_time
    interval_list = []
    for _ in range(num):
        if fixed_model_size is None:
            model_size = random.random()
        else:
            model_size = fixed_model_size
        if exponential_beta != False:
            arriving_time += exponential_interval_list[i]
        new_task_info.append((arriving_time, model_size, task_occupied_NIC_num_list[j]))
        interval_list.append(arriving_time - last_arriving_time)
        j += 1
    print('new_task_info:', new_task_info)
    print_workload(new_task_info)
    print('average interval:', np.mean(interval_list))
    return new_task_info


def generate_custom_tasks(num, max_NIC_num = 256, exponential_beta = 0.1, fixed_model_size = None):
    new_task_info = []
    arriving_time = 0

    base_NIC_num_list = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    num_times_list = [50, 20, 20, 100, 100, 100, 500, 50, 60, 30]
    
    task_occupied_NIC_num_list = []
    for i in range(len(num_times_list)):
        for _ in range(num_times_list[i]):
            if base_NIC_num_list[i] > max_NIC_num:
                base_NIC_num_list[i] = max_NIC_num
            task_occupied_NIC_num_list.append(base_NIC_num_list[i])
    random.seed(0)
    random.shuffle(task_occupied_NIC_num_list)

    if exponential_beta != False:
        rng = np.random.default_rng(0)
        exponential_interval_list = rng.exponential(exponential_beta, num)

    j = 0
    last_arriving_time = arriving_time
    interval_list = []
    for _ in range(num):
        if fixed_model_size is None:
            model_size = random.random()
        else:
            model_size = fixed_model_size
        if exponential_beta != False:
            arriving_time += exponential_interval_list[i]
        new_task_info.append((arriving_time, model_size, task_occupied_NIC_num_list[j]))
        interval_list.append(arriving_time - last_arriving_time)
        j += 1
    print('new_task_info:', new_task_info)
    print_workload(new_task_info)
    print('average interval:', np.mean(interval_list))
    return new_task_info


def get_fixed_requests_256_part_tasks(num, exponential_interval = True, beta = 1, filename = '../../base_conf_template/fixed_requests_256_part.txt', modify = False, random_beta = False, beta_list = []):
    with open(filename, 'r') as f:
        row_list = f.read().splitlines()
        
    base_NIC_num_list = [1, 2, 4, 8, 16, 32, 64, 128, 256]
    temp_sum = 0
    for job_class_id in base_NIC_num_list:
        temp_sum += math.log(job_class_id)
    job_weight_map = {}
    for job_class_id in base_NIC_num_list:
        job_weight_map[job_class_id] = math.log(job_class_id)/temp_sum

    random.seed(0)

    temp_list = []
    temp_time = 0
    if exponential_interval == True and random_beta == False:
        rng = np.random.default_rng(0)
        exponential_interval_list = rng.exponential(beta, num)
    elif random_beta:
        exponential_interval_list_list = []
        for temp_beta in beta_list:
            rng = np.random.default_rng(0)
            exponential_interval_list = rng.exponential(temp_beta, num)
            exponential_interval_list_list.append(exponential_interval_list)
    new_task_info = []
    arriving_time = 0
    np.random.seed(0)
    model_size_list = np.random.normal(800,700,(num,)).tolist()
    for i in range(num):
        task_occupied_NIC_num = int(float(row_list[i].split(' ')[0]))
        temp_list.append(task_occupied_NIC_num)

        if modify == True:
            task_occupied_NIC_num = tmp_modify_occupied_NIC_num(task_occupied_NIC_num)

        #model_size = random.randint(100, 1000)
        model_size = max(100,model_size_list[i])
        #print("fuck", model_size)

        if exponential_interval == True:
            if not random_beta:
                arriving_time += exponential_interval_list[i]
            else:
                arriving_time += exponential_interval_list_list[(100*i)%(len(beta_list))][i]
            # print((arriving_time))
            # arriving_time += random_exponential(0.5)

        new_task_info.append((arriving_time, model_size, task_occupied_NIC_num))
    print('new_task_info:', new_task_info)
    return new_task_info

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


def get_exp_comm_time_HD(task_occupied_NIC_num, task_iteration_num = 1, NIC_num_in_server = 4 ):
    model_size = 1
    if task_occupied_NIC_num>NIC_num_in_server:
        comm_time = 0
        communication_size = model_size/2
        for t in range(int(math.log2(task_occupied_NIC_num))):
            if t<int(math.log2(NIC_num_in_server)):
                comm_time+=communication_size/1000
            else:
                comm_time+=communication_size/100
            communication_size/=2
        exp_communication_time = task_iteration_num*(comm_time*2)     
    else:
        comm_time = 0
        communication_size = model_size/2
        for t in range(int(math.log2(task_occupied_NIC_num))):
            comm_time+=communication_size/1000
            communication_size/=2
        if comm_time == 0:
            comm_time = model_size/100/2
        exp_communication_time = task_iteration_num*(comm_time*2)
    return exp_communication_time

def get_exp_comm_time_Ring(task_occupied_NIC_num, task_iteration_num = 1, NIC_num_in_a_server = 4 ):
    model_size = 1
    node_num = int(task_occupied_NIC_num / NIC_num_in_a_server)
    comm_time = 0
    
    if task_occupied_NIC_num > NIC_num_in_a_server:
        # ring allreduce in the intra-server
        round_num = NIC_num_in_a_server - 1 
        communication_size = model_size / NIC_num_in_a_server
        for _ in range(round_num):
            comm_time += communication_size/1000
        
        # ring allreduce inter servers
        communication_size = model_size / NIC_num_in_a_server / node_num 
        round_num = node_num 
        for _ in range(round_num):
            comm_time += communication_size/100
            
        # ring allreduce in the intra-server
        communication_size = model_size / NIC_num_in_a_server
        round_num = NIC_num_in_a_server - 1
        for _ in range(round_num):
            comm_time += communication_size/1000
    else:
        round_num = task_occupied_NIC_num - 1 
        communication_size = model_size / task_occupied_NIC_num
        for _ in range(round_num):
            comm_time += communication_size/1000
            
    return comm_time

def get_exp_comm_time_all2all(task_occupied_NIC_num, task_iteration_num = 1, NIC_num_in_a_server = 4 ):
    model_size = 1
    comm_time = 0
    
    if task_occupied_NIC_num > NIC_num_in_a_server:
        round_num = task_occupied_NIC_num - 1 
        communication_size = model_size
        for _ in range(round_num):
            comm_time += communication_size/100
    else:
        round_num = task_occupied_NIC_num - 1 
        communication_size = model_size
        for _ in range(round_num):
            comm_time += communication_size/1000
    return comm_time


def random_choose_model_name(taskid, task_occupied_NIC_num):
    random.seed(taskid)
    random_value = random.uniform(0, 1)
    if random_value < 0.5 and is_power_of_2(task_occupied_NIC_num):
        model_name = random.choice(Small_model_list)
        exp_comm_time = get_exp_comm_time_HD(task_occupied_NIC_num)
    elif random_value < 0.7 or not(is_power_of_2(task_occupied_NIC_num)):
        model_name = random.choice(Small_model_list)
        exp_comm_time = get_exp_comm_time_Ring(task_occupied_NIC_num)
    else:
        tmp_list = copy.deepcopy(Large_model_list)
        # tmp_list.extend(Small_model_list)
        model_name = random.choice(tmp_list)
        exp_comm_time = get_exp_comm_time_all2all(task_occupied_NIC_num)
    # print("debug random_value" ,taskid ,random_value, model_name)
    return model_name, exp_comm_time

def get_fixed_requests_256_part_tasks_TPUv4_512(num, exponential_interval = True, beta = 1, filename = '../../../base_conf_template/fixed_requests_256_part.txt', modify = False, random_beta = False, beta_list = [], ave_comm_ratio = 0.2):
    map_relation = generate_helios_tpuv4_map()
    f3 = open('task_detail.txt','w')
    waiting_list = []
    running_list = []
    gpu_list = []
    arrive_list = []
    new_task_info = []
    random.seed(0)
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
    # print("debug running_list", running_list)
    print(gpu_list[:num])
    arriving_time = 0
    data_set_size = len(gpu_list)
    if exponential_interval == True and random_beta == False:
        rng = np.random.default_rng(0)
        exponential_interval_list = rng.exponential(beta, num)
    elif random_beta:
        exponential_interval_list_list = []
        for temp_beta in beta_list:
            # print("debug temp_beta", temp_beta)
            rng = np.random.default_rng(0)
            exponential_interval_list = rng.exponential(temp_beta, num)
            exponential_interval_list_list.append(exponential_interval_list)
    new_task_info = []
    arriving_time = 0
    chosen_gpu_list = []
    task_class = {}
    model_size_list = []
    for i in range(num):
        task_occupied_NIC_num = int(gpu_list[i%data_set_size])
        chosen_gpu_list.append(task_occupied_NIC_num)
        comm_all_ration = 0
        
        model_name, exp_communication_time = random_choose_model_name(i, task_occupied_NIC_num)
        if model_name in ALL_REDUCE_COST:
            comm_all_ration += ALL_REDUCE_COST[model_name]
        if model_name in ALL2ALL:
            comm_all_ration += ALL2ALL[model_name]
        if model_name in ALLGATHER:
            comm_all_ration += ALLGATHER[model_name]
        if model_name in ReduceScatter:
            comm_all_ration += ReduceScatter[model_name]

        total_val = max(1000,running_list[i%data_set_size]) #140 #m
        total_val = min(100000,total_val)
        total_val = 100*max(1,int(total_val/100))
        
        task_iteration_num = 1
        model_size = 0
        if task_occupied_NIC_num>1:
            model_size = total_val*comm_all_ration/exp_communication_time
        computation_time = total_val*(1-comm_all_ration)/task_iteration_num
            
        if not random_beta:
            arriving_time += exponential_interval_list[i]
        else:
            arriving_time += exponential_interval_list_list[int(i/100)%(len(beta_list))][i]
        # if i == 2:
        #     model_size = 0.0001
        #     computation_time = 0.0001
        new_task_info.append((arriving_time, model_size, task_occupied_NIC_num, computation_time, task_iteration_num))
        model_size_list.append(model_size)
        # print("debug new_task_info",(total_val, arriving_time, model_size, task_occupied_NIC_num, computation_time, task_iteration_num))
    return new_task_info


def tmp_modify_occupied_NIC_num(task_occupied_NIC_num):
    if task_occupied_NIC_num >= 64:
        return 64
    else:
        return task_occupied_NIC_num


def random_exponential(lam):
    # x = np.arange(0, 15, 0.1)
    # y = lam * np.exp(-lam * x)
    pv = 0.0
    pv = (random.random() % 100) / 100
    while pv == 0:
        pv = (random.random()() % 100) / 100

    pv = (-1  / lam) * math.log(1 - pv)
    print(pv)
    return pv


def date_time_str_to_long(input_date_time_string):
    if input_date_time_string == 'None':
        return 0
    timeArray = time.strptime(input_date_time_string, "%Y-%m-%d %H:%M:%S")
    
    timeStamp = int(time.mktime(timeArray))
    
    return timeStamp


def get_shangtang_tasks_old(task_num, filename = '../../base_conf_template/cluster_log.csv', exponential_beta = 0.000003):
    waiting_list = []
    running_list = []
    gpu_list = []
    new_task_info = []

    rng = np.random.default_rng(0)
    exponential_interval_list = rng.exponential(exponential_beta, task_num)
    i = 0
    with open(filename, encoding='utf-8-sig') as f:
        start_time = 0
        for row in csv.reader(f, skipinitialspace=True):
            duration_time = 0
            waiting_time = 0
            if (row[6] == 'COMPLETED') and int(row[3])>0 and date_time_str_to_long((row[7])) >= date_time_str_to_long("2020-08-20 00:51:57"): #2020-08-15 00:51:57
                if(start_time == 0):
                    start_time = date_time_str_to_long((row[7]))
                duration_time = date_time_str_to_long(row[9]) - date_time_str_to_long(row[8])
                waiting_time = date_time_str_to_long(row[8]) - date_time_str_to_long(row[7])
                running_list.append(duration_time)
                waiting_list.append(waiting_time)
                task_occupied_NIC_num = pow(2,math.ceil(math.log2(int(row[3]))))
                gpu_list.append(task_occupied_NIC_num)

                new_task_info.append((exponential_interval_list[i], 1, task_occupied_NIC_num))
                i += 1
                if i == task_num:
                    break
    
    print('debug new_task_info', new_task_info)
    return new_task_info


def get_shangtang_tasks(num, exponential_beta = 0.0001):
    new_task_info = []
    arriving_time = 0

    base_NIC_num_list = [1, 2, 4, 8, 16, 32, 64, 128, 256]
    num_times_list = [11136, 829, 1109, 4257, 400, 925, 76, 80, 13]
    
    task_occupied_NIC_num_list = []
    for i in range(len(num_times_list)):
        for _ in range(num_times_list[i]):
            task_occupied_NIC_num_list.append(base_NIC_num_list[i])
    random.seed(0)
    random.shuffle(task_occupied_NIC_num_list)

    if exponential_beta != False:
        rng = np.random.default_rng(0)
        exponential_interval_list = rng.exponential(exponential_beta, num)

    j = 0
    for _ in range(num):
        # model_size = random.random()
        model_size = 100
        if exponential_beta != False:
            arriving_time += exponential_interval_list[i]
        new_task_info.append((arriving_time, model_size, task_occupied_NIC_num_list[j]))

        j += 1

    return new_task_info


def print_workload(new_task_info):
    GPU_num_sum = 0
    simultaneous_GPU_time = 0
    full_GPU_time = 0
    for (submit_time, model_size, GPU_num) in new_task_info:
        GPU_num_sum += GPU_num
        # submit_time_sum += submit_time
        simultaneous_GPU_time += model_size * GPU_num
        full_GPU_time += model_size * GPU_num + submit_time

    print('GPU_num_sum', GPU_num_sum)
    print('simultaneous_GPU_time', simultaneous_GPU_time)
    print('full_GPU_time', full_GPU_time)
    print('workload', simultaneous_GPU_time / full_GPU_time)


def generate_16384_custom_tasks(exponential_beta = 1, fixed_model_size = None):
    new_task_info = []
    arriving_time = 0

    base_NIC_num_list = [4096, 512, 1024, 256]
    num_times_list = [5, 30, 15, 50]
    num = sum(num_times_list)
    fixed_model_size = 1

    task_occupied_NIC_num_list = []
    for i in range(len(num_times_list)):
        for _ in range(num_times_list[i]):
            task_occupied_NIC_num_list.append(base_NIC_num_list[i])
    random.seed(0)
    random.shuffle(task_occupied_NIC_num_list)

    if exponential_beta != False:
        rng = np.random.default_rng(0)
        exponential_interval_list = rng.exponential(exponential_beta, num)

    j = 0
    last_arriving_time = arriving_time
    interval_list = []
    for _ in range(num):
        if fixed_model_size is None:
            model_size = random.random()
        else:
            model_size = fixed_model_size
        if exponential_beta != False:
            arriving_time += exponential_interval_list[i]
        new_task_info.append((arriving_time, model_size, task_occupied_NIC_num_list[j]))
        interval_list.append(arriving_time - last_arriving_time)
        j += 1
    print('new_task_info:', new_task_info)
    print_workload(new_task_info)
    print('average interval:', np.mean(interval_list))
    return new_task_info

def generate_16384_custom_tasks2(num_tmp = 100, exponential_beta = 1, fixed_model_size = None):
    new_task_info = []
    arriving_time = 0

    base_NIC_num_list = [4096, 512, 1024, 256, 8192]
    if num_tmp == 100:
        num_times_list = [35, 10, 5, 10, 40]
    else:
        num_times_list = np.array([35, 10, 5, 10, 40]) * num_tmp / 100
    num = int(sum(num_times_list))
    fixed_model_size = 1

    task_occupied_NIC_num_list = []
    for i in range(len(num_times_list)):
        for _ in range(int(num_times_list[i])):
            task_occupied_NIC_num_list.append(base_NIC_num_list[i])
    random.seed(0)
    random.shuffle(task_occupied_NIC_num_list)

    if exponential_beta != False:
        rng = np.random.default_rng(0)
        exponential_interval_list = rng.exponential(exponential_beta, num)

    j = 0
    last_arriving_time = arriving_time
    interval_list = []
    for _ in range(num):
        if fixed_model_size is None:
            model_size = random.random()
        else:
            model_size = fixed_model_size
        if exponential_beta != False:
            arriving_time += exponential_interval_list[i]
        new_task_info.append((arriving_time, model_size, task_occupied_NIC_num_list[j]))
        interval_list.append(arriving_time - last_arriving_time)
        j += 1
    print('new_task_info:', new_task_info)
    print_workload(new_task_info)
    print('average interval:', np.mean(interval_list))
    return new_task_info


# if __name__ == "__main__":
#     print(generate_16384_custom_tasks())
# print(get_fixed_requests_256_part_tasks_TPUv4_512(5000))