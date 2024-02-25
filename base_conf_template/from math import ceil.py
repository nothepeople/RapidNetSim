from math import ceil
import queue
import math
import rapidnetsim.scheduler.static_scheduler_multi_cluster.job as job
import numpy as np
import rapidnetsim.scheduler.static_scheduler_multi_cluster.switch_resource as switch
import time
from rapidnetsim.scheduler.static_scheduler_multi_cluster.utils import *
import logging
import rapidnetsim.scheduler.static_scheduler_multi_cluster.job_request as job_request
import rapidnetsim.scheduler.static_scheduler_multi_cluster.gurobi_solver as gurobi_solver
# import rapidnetsim.communication_strategy.detect_butterfly2_conflict as detect_butterfly2_conflict
import operator
import gurobipy
import numpy as np
import copy


logging.basicConfig(level= logging.ERROR)

class static_scheduler:
    def __init__(self, clos_n, clos_m, opt_segmentation = False, server_num = 4, sub_cluster_num = 4):#spine_switch_num, leaf_switch_num, spine_switch_port_num, leaf_switch_port_num):
        spine_switch_num = clos_n
        leaf_switch_num = clos_m
        spine_switch_port_num = clos_m
        leaf_switch_port_num = clos_n * 2

        gurobi_solver.config_cluster_size(clos_n, clos_m)

        self.sub_cluster_num = sub_cluster_num
        self.spine_num = spine_switch_num
        self.leaf_num = leaf_switch_num
        self.gpu_per_switch = int(leaf_switch_port_num / 2)
        self.gpu_num = self.gpu_per_switch * leaf_switch_num
        self.spine_switch_port_num = spine_switch_port_num
        self.leaf_switch_port_num = leaf_switch_port_num
        self.gpu_per_server = int(self.gpu_num/server_num)
        self.gpu_per_leaf = int(self.gpu_num/self.leaf_num)
        self.server_per_leaf = int(self.gpu_per_leaf/self.gpu_per_server)
        self.sub_cluster_num = sub_cluster_num
        # self.gpu_per_server = 8
        # self.allocated_gpu_num = 0
        self.gpus = []
        for i in range(self.leaf_num):
            self.gpus.append(np.zeros(int(leaf_switch_port_num / 2)))

        logging.debug("cluster size: %d gpus" % self.gpu_num)
        print(spine_switch_port_num, self.leaf_num, int(leaf_switch_port_num/2), self.spine_num, self.gpu_num, self.spine_num * spine_switch_port_num)
        assert spine_switch_port_num == self.leaf_num and int(leaf_switch_port_num/2) == self.spine_num and self.gpu_num == int(self.spine_num * spine_switch_port_num)


        # job queue
        self.reqQueue = queue.Queue()
        self.next_req = None

        # allocation method
        self.allocate_methods = ["static_based"]
        self.allocate_method = self.allocate_methods[0]
        self.allocated_jobs = []

        # statistics
        self.utilization_rate = []
        self.utilization_time = []
        self.ideal_utilization = []

        self.utilization_rate_aggregated = []
        self.served_job_count = 0
        self.allocation_failure_job = []
        self.allocation_failure_job_details = []
        self.failure_logged = False
        self.job_info = {}
        self.free_gpu_before_allocation = 0
        self.l_error = 0
        self.l_retry = 0

        self.allocation_chain = {}
        self.chain_index = 0
        self.larger_allocation = []

        self.time_slot = 0
        self.come_in_time = 0

        self.state_changed = True

        self.allocation_times = 0
        self.total_allocation_sec = 0

        # debug
        self.last_update_time = 0
        self.wasted_time = 0
        self.should_schedule = False

        #opt segmentation
        self.opt_segmentation = opt_segmentation

        # thresh-based policy adjustment
        self.thresh_ratio = 1.0
        self.thresh_line = int(self.thresh_ratio * spine_switch_port_num)
        self.even_allocation_num = 0  # the number of even allocation to all spine servers
        self.to_decrease_thresh = False

        # optimization time limit
        self.time_limit = -1  # reserved

        # conflict detection
        self.conflict_detector = None
        self.failed_by_conflict = False
        self.conflict_time_point = -1
        self.current_running_task = 0
        self.current_gpu_num = 0
        self.gpu_class_num_map = {0 for i in range(int(math.log2(spine_switch_num*spine_switch_port_num)))}
        self.cluster_remain_gpu_pair_list = [(i,int(self.gpu_num)) for i in range(sub_cluster_num)]


    def set_allocate_method(self, method):
        logging.debug("Setting algorithm:", method)
        assert method in self.allocate_methods
        self.allocate_method = method


    def _translate_gpu_index(self, algo_gpu_allocation):
        output_gpu_indexes = []
        for gpu_entry in algo_gpu_allocation:
            # gpu results
            gpu_index = gpu_entry[0] * self.gpu_per_switch + gpu_entry[1]  # translate to output index
            output_gpu_indexes.append(gpu_index)
        return output_gpu_indexes

    def _translate_allocation(self, algo_gpu_allocation, algo_spine_allocation):
        #output
        allocation_link_mapping = []

        # gpu - leaf links
        for gpu_entry in algo_gpu_allocation:
            # gpu results
            output_gpu_index = gpu_entry[0] * self.gpu_per_switch + gpu_entry[1]  # translate to output index
            leaf_index = gpu_entry[0]  # the leaf switch the gpu locates on
            output_leaf_index = leaf_index + self.gpu_num
            allocation_link_mapping.append((output_gpu_index, output_leaf_index, 1))
            allocation_link_mapping.append((output_leaf_index, output_gpu_index, 1))

        # leaf - spine links
        leaf_to_spine_link_num = {}
        for spine_port_entry in algo_spine_allocation:
            leaf_index = spine_port_entry[1]
            spine_index = spine_port_entry[0]
            if leaf_index not in leaf_to_spine_link_num.keys(): leaf_to_spine_link_num[leaf_index] = {}
            if spine_index not in leaf_to_spine_link_num[leaf_index].keys(): leaf_to_spine_link_num[leaf_index][
                spine_index] = 0
            leaf_to_spine_link_num[leaf_index][spine_index] += 1
        for leaf_index in leaf_to_spine_link_num.keys():
            output_leaf_index = leaf_index + self.gpu_num
            for spine_index in leaf_to_spine_link_num[leaf_index].keys():
                output_spine_index = self.leaf_num + spine_index + self.gpu_num
                allocation_link_mapping.append((output_leaf_index, output_spine_index, leaf_to_spine_link_num[leaf_index][spine_index]))
                allocation_link_mapping.append((output_spine_index, output_leaf_index, leaf_to_spine_link_num[leaf_index][spine_index]))

        return allocation_link_mapping

    

    def schedule(self, gpu_num, job_id, sim_time, queued_jobs):
        strict_clos = False
        check_conflict = False
        print("some job arrive: "+str(job_id)+","+str(gpu_num))
        from rapidnetsim.core.simulator import Simulator
        time_start = time.perf_counter()
        next_req = job_request.job_request(gpu_num, job_id)

        # debug
        self.should_schedule = False
        self.wasted_time += (sim_time - self.last_update_time)

        # current gpu use rate
        self.used_gpu_num = np.sum(self.gpus)
        

        allocate_success, cause_of_failure, algo_gpu_allocation, algo_spine_allocation = self.allocate_GPU(next_req, strict_clos, check_conflict, sim_time, [], self.sub_cluster_num)
        if allocate_success:
            all_gpu_index,link_mapping = None, None#self._translate_cluster_status()#self._translate_allocation_results(algo_gpu_allocation, algo_spine_allocation)
            gpu_indexes = self._translate_gpu_index(algo_gpu_allocation)

            if strict_clos:
                assert algo_spine_allocation != None
                allocated_link_mapping = self._translate_allocation(algo_gpu_allocation, algo_spine_allocation)
            else:
                if algo_spine_allocation == None: allocated_link_mapping = None
                else: allocated_link_mapping = self._translate_allocation(algo_gpu_allocation, algo_spine_allocation)

            self.utilization_rate.append(self.used_gpu_num / float(self.gpu_num))
            self.utilization_time.append(sim_time - self.time_slot)
            self.time_slot = sim_time
            if gpu_num>=1:
                self.current_running_task += 1
                self.current_gpu_num 
            self.current_gpu_num += gpu_num
        else:
            gpu_indexes = None
            link_mapping = None
            all_gpu_index = None
            allocated_link_mapping = None
        time_end = time.perf_counter()
        time_sum = time_end-time_start
        Simulator.SCHEDULER_TIME_COST[job_id] = 0
        f3 = open('schedule_time_cost.txt','a')
        f3.write(str(job_id))
        f3.write(",")
        f3.write(str(time_sum) )
        f3.write("\n" )
        f3.close()  
        comm_pair = None
        if gpu_indexes != None:
            pow_2_gpu_list = gpu_indexes
            remain_gpu_list = []
            if not power_of_2(len(gpu_indexes)):
                comm_pair = self.fusion_gpu_list(pow_2_gpu_list, remain_gpu_list)
            assert len(gpu_indexes) == gpu_num
        f2 = open('queue_length.txt','a')
        f2.write(str(len(queued_jobs)))
        f2.write(",")
        f2.write(str(sim_time) )
        f2.write("\n" )
        f2.close()
        return allocate_success, gpu_indexes, allocated_link_mapping, None, comm_pair, link_mapping#allocate_success, gpu_indexes, link_mapping


    def update_finished_job(self, job_id, sim_time, queued_jobs):
        move_flag = False

        if not self.should_schedule:
            self.last_update_time = sim_time
            self.should_schedule = True

        # current gpu use rate
        self.used_gpu_num = np.sum(self.gpus)
        job_gpu_num = 0
        for i, ongoing_job in enumerate(self.allocated_jobs):
            if ongoing_job.id == job_id:
                # release resources
                # print(ongoing_job.allocated_gpus)
                leaf_switch_indexs = set()
                for gpu_index in ongoing_job.allocated_gpus:
                    # self.allocated_gpu_num -= 1
                    # release gpu
                    assert self.gpus[gpu_index[0]][gpu_index[1]] == 1
                    self.gpus[gpu_index[0]][gpu_index[1]] = 0
                    leaf_switch_indexs.add(gpu_index[0])
                job_gpu_num = len(ongoing_job.allocated_gpus)


                self.allocated_jobs.pop(i)
                move_flag = True

                self.utilization_rate.append(self.used_gpu_num / float(self.gpu_num))
                self.utilization_time.append(sim_time - self.time_slot)
                self.time_slot = sim_time
                break
        if job_gpu_num>=1:
            self.current_running_task -= 1
        self.current_gpu_num -= job_gpu_num
        if not move_flag:
            logging.error("Update status failed! : invalid job_id")
            exit()
        f2 = open('queue_length.txt','a')
        f2.write(str(len(queued_jobs)))
        f2.write(",")
        f2.write(str(sim_time) )
        f2.write("\n" )
        f2.close()





    # check if allocation leads to link contention
    def check_conflict(self, gpu_indexes):
        global_gpu_indexes = self._translate_gpu_index(gpu_indexes)
        return self.conflict_detector.whether_conflict(100, len(global_gpu_indexes), global_gpu_indexes)
    
    
    def fusion_gpu_list(self, pow_2_gpu_list, remain_gpu_list):
        communication_pair_list = []
        fus_gpu_list = []
        fus_gpu_list.extend(pow_2_gpu_list)
        fus_gpu_list.extend(remain_gpu_list)
        gpu_global_local_index_map = {}
        for i in range(len(fus_gpu_list)):
            gpu_global_local_index_map[fus_gpu_list[i]] = i
        # print(gpu_global_local_index_map)
        # print(pow_2_gpu_list, len(pow_2_gpu_list))
        # print(remain_gpu_list, len(remain_gpu_list))
        leaf_pow_2_gpu_list_map = {}
        for pow_2_gpu in pow_2_gpu_list:
            leaf_id = int(pow_2_gpu/self.gpu_per_leaf)
            if leaf_id not in leaf_pow_2_gpu_list_map:
                leaf_pow_2_gpu_list_map[leaf_id] = []
            leaf_pow_2_gpu_list_map[leaf_id].append(pow_2_gpu)
        leaf_remain_gpu_list_map = {}
        temp_index = 0
        for remain_gpu in remain_gpu_list:
            leaf_id = int(remain_gpu/self.gpu_per_leaf)
            if leaf_id not in leaf_remain_gpu_list_map:
                leaf_remain_gpu_list_map[leaf_id] = []
            to_comm_gpu = pow_2_gpu_list[temp_index]
            leaf_remain_gpu_list_map[leaf_id].append(remain_gpu)
            communication_pair_list.append((gpu_global_local_index_map[remain_gpu], gpu_global_local_index_map[to_comm_gpu]))
            temp_index+=1
        # for leaf_id in leaf_remain_gpu_list_map:
        #     for remain_gpu in leaf_remain_gpu_list_map[leaf_id]:
        #         to_comm_gpu = leaf_pow_2_gpu_list_map[leaf_id][0]
        #         communication_pair_list.append((gpu_global_local_index_map[remain_gpu], gpu_global_local_index_map[to_comm_gpu]))
        #         del(leaf_pow_2_gpu_list_map[leaf_id][0])
        return communication_pair_list

    def allocate_GPU(self, next_req: job_request.job_request, strict_clos, check_conflict, sim_time, banned_server_list=[], sub_cluster_num = 1):
        #print("start allo")
        #### cause of allocation failure
        failure_cause = ""
        self.failed_by_conflict = False
        assert sub_cluster_num == 4
        gpu_each_cluster = int(self.gpu_num/sub_cluster_num)

        #self.allocate_method = "arbitrary"

        if self.allocate_method == "static_based":
            # print("Time:", self.time_slot)  # next_req.exec_time)
            # a naive way for allocating GPUs as split clos
            if int((self.gpu_num - np.sum(self.gpus))) < next_req.gpu_num:
                print("no resource0", self.gpu_num - np.sum(self.gpus), next_req.gpu_num)
                return False, failure_cause,None, None

            # decide gpu allocation
            request_num = next_req.gpu_num
            allocation_success = False
            gpu_indexes = []


            # simply see if there are enough remaining GPU
            if (self.gpu_num - np.sum(self.gpus)) < next_req.gpu_num:
                print("no resource0")
                return False, failure_cause, None, None
            else:
                potention_cluster_pair_list = copy.deepcopy(self.cluster_remain_gpu_pair_list)
                potention_cluster_pair_list.sort(key=lambda x:x[1]) 
                while not allocation_success and len(potention_cluster_pair_list)>0:
                    tmp_cluster_id = potention_cluster_pair_list[0][0]
                    leaf_num_per_cluster = int(self.leaf_num/self.sub_cluster_num)
                    valid_leaf_id_list = [tmp_leaf_id + leaf_num_per_cluster*tmp_cluster_id for tmp_leaf_id in range(leaf_num_per_cluster)]
                    distrib_count = 0
                    new_job = job.job(next_req.request_id)  # next_req.exec_time)
                    
                    
                    require_server_num = ceil(request_num/self.gpu_per_server)
                    temp_server_remain_gpu_map = {}
                    for li in range(self.leaf_num):
                        if li in valid_leaf_id_list:
                            for j in range(self.gpu_per_switch):  # we do not care about the gpu
                                temp_server_id = int(j/self.gpu_per_server)+self.server_per_leaf*li
                                temp_gpu_id = j+self.gpu_per_switch*li
                                if temp_server_id not in banned_server_list:
                                    temp_gpu_id = j+self.gpu_per_switch*li
                                    if self.gpus[li][j] == 0:
                                        if temp_server_id not in temp_server_remain_gpu_map:
                                            temp_server_remain_gpu_map[temp_server_id] = []
                                        temp_server_remain_gpu_map[temp_server_id].append(temp_gpu_id)
                    temp_server_remain_gpu_map = dict( sorted(temp_server_remain_gpu_map.items(),key = lambda x:len(x[1]),reverse = False))
                    temp_server_remain_gpu_list = []
                    for server_id in temp_server_remain_gpu_map:
                        temp_remain_gpu_list = temp_server_remain_gpu_map[server_id]
                        if len(temp_remain_gpu_list)==self.gpu_per_server or len(temp_remain_gpu_list)>=next_req.gpu_num:
                            temp_server_remain_gpu_list.append([server_id, temp_remain_gpu_list])
                    if len(temp_server_remain_gpu_list)>=require_server_num:
                        allocation_success = True
                        for i in range(require_server_num):
                            for gpu_id in temp_server_remain_gpu_list[i][1]:
                                if distrib_count< next_req.gpu_num:
                                    distrib_count+=1
                                    gpu_indexes.append((int(gpu_id/self.gpu_per_switch), gpu_id%self.gpu_per_switch))
                                    new_job.add_gpu((int(gpu_id/self.gpu_per_switch), gpu_id%self.gpu_per_switch))

                        self.allocated_jobs.append(new_job)
                        for pair in gpu_indexes:
                            self.gpus[pair[0]][pair[1]] = 1
                        #print("finish allo", self.gpu_num, self.gpu_num - np.sum(self.gpus), next_req.gpu_num)
                        # print("gpu_indexes", gpu_indexes)
                        assert len(gpu_indexes) == request_num
                        return True, failure_cause, gpu_indexes, None
                    else:
                        del potention_cluster_pair_list[0]
                    
                print("no resource1",  (self.gpu_num - np.sum(self.gpus)) , next_req.gpu_num, len(temp_server_remain_gpu_list), require_server_num)
                return False, failure_cause, None, None
