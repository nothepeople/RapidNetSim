
import gurobipy
import math
import numpy as np

def power_of_2(n):
    start = 1
    flag = False
    while start <= n:
        if start == n: flag = True
        start *= 2
    return flag

def power2_zero(n):
    if n == 0: return True
    start = 1
    flag = False
    while start <= n:
        if start == n: flag = True
        start *= 2
    return flag

def chose_server_map(gpu_index_list, server_size = 8):
    server_num_map = {}
    for gpu_id in gpu_index_list:
        server_id = int(gpu_id/8)
        if server_id not in server_num_map:
            server_num_map[server_id] = 0
        server_num_map[server_id] += 1
    return server_num_map

def find_first_element_list(pair_list, second_value):
    result = []
    for temp_pair in pair_list:
        if temp_pair[1] == second_value:
            result.append(temp_pair[0])
    return result

def find_closest_first_list_pair(pair_list, second_value):
    result_index = -1
    cur_gap = 10000000
    for temp_pair in pair_list:
        if temp_pair[1]-second_value>=0 and temp_pair[1]-second_value<cur_gap:
            cur_gap = temp_pair[1]-second_value
            result_index = temp_pair[0]
    assert result_index != -1
    assert cur_gap != 0
    return [result_index, second_value+cur_gap]

def get_leaf_module_id(leaf_id, gpu_num):
        return leaf_id+gpu_num

def get_spine_module_id(spine_id, gpu_num, leaf_num):
    return spine_id+gpu_num+leaf_num


def new_make_spine_migration_strategy():
    max_weight_spine = int(math.log2(32))+1
    print("start migration")
    job_running_group_require = [0, 0, 0, 0, 3, 10]
    job_group_require = [0, 0, 0, 0, 3, 10]
    init_spine_group_list = [[0, 0, 0, 0, 1, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 1, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 1], [0, 0, 0, 0, 0, 1], [0, 0, 0, 0, 0, 1], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 1]]
 

    spine_state_vec = []
    for temp_spine_state_vec in init_spine_group_list:
        spine_state_vec.extend(temp_spine_state_vec)
    running_group_list = []
    for temp_spine_group_id in range(len(init_spine_group_list)):
        temp_spine_group = init_spine_group_list[temp_spine_group_id]
        temp_running_group = [0 for i in range(max_weight_spine)]
        remain_gpu = 32
        for temp_element in range(len(temp_spine_group)):
            if(temp_spine_group[temp_element]):
                remain_gpu -= int(pow(2,temp_element))
        if remain_gpu>0:
            have_chosen_group_size = int (math.pow( 2, int( math.log2(remain_gpu) ) ))
            temp_running_group[int(math.log2(have_chosen_group_size))] = 1
            temp_potentional_group_size = have_chosen_group_size
            while(have_chosen_group_size<remain_gpu):
                if(have_chosen_group_size+int(temp_potentional_group_size) <= remain_gpu):
                    temp_running_group[int( math.log2(temp_potentional_group_size))] = 1
                    have_chosen_group_size += temp_potentional_group_size
                    temp_potentional_group_size = int(temp_potentional_group_size/2)
                else:
                    temp_potentional_group_size = int(temp_potentional_group_size/2)
        else:
            assert remain_gpu == 0
        running_group_list.extend(temp_running_group)

        
    m = gurobipy.Model("Clos solution")
    m.setParam('OutputFlag', 0)
    m.setParam('TimeLimit', 120)

    x_i = {}
    for it in range(len(spine_state_vec)):
        x_i[it] = m.addVar(vtype=gurobipy.GRB.INTEGER, lb=0, ub=1,name='x_i')
    r = {}
    for it in range(len(spine_state_vec)):
        r[it] = m.addVar(vtype=gurobipy.GRB.INTEGER, lb=0, ub=32,name='r_i')

    obj_val = m.addVar(vtype=gurobipy.GRB.INTEGER, lb=0, ub=4096,name='obj')
    obj_x_i = {}
    for it in range(len(spine_state_vec)):
        obj_x_i[it] = m.addVar(vtype=gurobipy.GRB.INTEGER, lb=0, ub=32,name='obj_x_i')
    obj_r_i = {}
    for it in range(len(spine_state_vec)):
        obj_r_i[it] = m.addVar(vtype=gurobipy.GRB.INTEGER, lb=0, ub=1,name='obj_r_i')
    m.setObjective(obj_val, gurobipy.GRB.MINIMIZE)
    m.update()

    # 设置条件
    for it in range(len(spine_state_vec)):
        m.addConstr(gurobipy.quicksum( (x_i[j] + r[j])*math.pow(2,int(j%max_weight_spine)) for j in range(len(spine_state_vec)) if int(it/max_weight_spine) == int(j/max_weight_spine)) == 32)

    for k in range(max_weight_spine):
        temp_class_list = []
        for temp_k in range(k,max_weight_spine,1):
            temp_class_list.append(temp_k)
        temp_value = 0
        for temp_k in range(k,max_weight_spine,1):
            temp_value += int(job_group_require[k]*pow(2,temp_k))
        m.addConstr(gurobipy.quicksum( (x_i[j] + r[j])*math.pow(2,int(j%max_weight_spine)) for j in range(len(spine_state_vec)) if int(j%max_weight_spine) in temp_class_list) >= temp_value)

    for k in range(max_weight_spine-1):
        m.addConstr(gurobipy.quicksum( (x_i[j] + r[j]) for j in range(len(spine_state_vec)) if int(k) == int(j%max_weight_spine)) <= job_group_require[k]+1)

    for k in range(max_weight_spine):
        m.addConstr(gurobipy.quicksum( r[j] for j in range(len(spine_state_vec)) if int(k) == int(j%max_weight_spine)) == job_running_group_require[k])

    for it in range(len(spine_state_vec)):
        m.addConstr(obj_x_i[it]>=(x_i[it]-spine_state_vec[it])*math.pow(2,it%max_weight_spine))
        m.addConstr(obj_x_i[it]>=(spine_state_vec[it]-x_i[it])*math.pow(2,it%max_weight_spine))
        m.addConstr(obj_x_i[it]>=(r[it] - running_group_list[it])*math.pow(2,it%max_weight_spine))
        m.addConstr(obj_x_i[it]>=(running_group_list[it]-r[it])*math.pow(2,it%max_weight_spine))
    m.addConstr(gurobipy.quicksum( obj_x_i[j] for j in range(len(spine_state_vec))) == obj_val)
    m.addConstr(gurobipy.quicksum((r[j] - running_group_list[j])*math.pow(2,j%max_weight_spine) for j in range(len(spine_state_vec)) )==0)
    # 开始执行
    m.update()
    m.optimize()
    # 记录运行结果
    if m.status == gurobipy.GRB.Status.OPTIMAL:
        print(int(obj_val.X))
        x_i_solution = m.getAttr('X',x_i)
        spine_total_group_map = {}
        temp_spine_ptr = 0
        temp_total_group_list = []
        for x_index in x_i_solution:
            if x_index%max_weight_spine == 0 and x_index>0:
                spine_total_group_map[temp_spine_ptr] = temp_total_group_list
                temp_total_group_list = []
                temp_spine_ptr+=1
            temp_total_group_list.append(int(x_i_solution[x_index]*math.pow(2,int(x_index%max_weight_spine))))
        spine_total_group_map[temp_spine_ptr] = temp_total_group_list
        print(spine_total_group_map)

        r_solution = m.getAttr('X',r)
        spine_total_running_group_map = {}
        temp_spine_ptr = 0
        temp_total_group_list = []
        for x_index in r_solution:
            if x_index%max_weight_spine == 0 and x_index>0:
                spine_total_running_group_map[temp_spine_ptr] = temp_total_group_list
                temp_total_group_list = []
                temp_spine_ptr+=1
            temp_total_group_list.append(int(r_solution[x_index]*math.pow(2,int(x_index%max_weight_spine))))
        spine_total_running_group_map[temp_spine_ptr] = temp_total_group_list
        print(spine_total_running_group_map)

        print("spine group migration map")
        spine_task_migration_out_pair = []
        spine_task_migration_in_pair = []
        for it in range(len(running_group_list)):
            if int(r_solution[it])!=running_group_list[it]:
                temp_spine_id = int(it/max_weight_spine)
                temp_class_id = int(it%max_weight_spine)
                if(int( pow(2,temp_class_id)*(r_solution[it] - running_group_list[it]))<0):
                    spine_task_migration_out_pair.append([temp_spine_id,int( -1*pow(2,temp_class_id)*(r_solution[it] - running_group_list[it])) ])
                else:
                    spine_task_migration_in_pair.append([temp_spine_id,int( pow(2,temp_class_id)*(r_solution[it] - running_group_list[it])) ])

        group_migration_map = {}
        spine_task_migration_out_pair.sort(key=lambda x: x[1])
        spine_task_migration_in_pair.sort(key=lambda x: x[1])
        print(spine_task_migration_out_pair)
        print(spine_task_migration_in_pair)
        out_ptr_left = 0
        out_ptr_right = 0
        in_ptr = 0
        while(out_ptr_right<len(spine_task_migration_out_pair) and in_ptr<len(spine_task_migration_in_pair)):
            temp_match_flag = False
            temp_sum_group_num = 0
            for i in range(out_ptr_left, out_ptr_right+1, 1):
                temp_sum_group_num += spine_task_migration_out_pair[i][1]
            assert temp_sum_group_num<= spine_task_migration_in_pair[in_ptr][1]
            if(temp_sum_group_num == spine_task_migration_in_pair[in_ptr][1]):
                temp_match_flag = True
            if(temp_match_flag):
                for i in range(out_ptr_left, out_ptr_right+1, 1):
                    target_spine = spine_task_migration_out_pair[i][0]
                    start_spine = spine_task_migration_in_pair[in_ptr][0]
                    group_size = spine_task_migration_out_pair[i][1]
                    if start_spine not in group_migration_map:
                        group_migration_map[start_spine] = {}
                    if target_spine not in group_migration_map[start_spine]:
                        group_migration_map[start_spine][target_spine] = []
                    group_migration_map[start_spine][target_spine].append(group_size)
                    

                out_ptr_left = out_ptr_right+1
                in_ptr += 1
            out_ptr_right+=1

        print(group_migration_map)
        #TODO 是否更新group
        return group_migration_map
    else:
        raise Exception("something wrong5 in gurobi solver")


