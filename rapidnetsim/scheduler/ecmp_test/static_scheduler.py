from math import ceil
import queue
import math
import rapidnetsim.scheduler.static_scheduler.job as job
import numpy as np
import rapidnetsim.scheduler.static_scheduler.switch_resource as switch
import time
from rapidnetsim.scheduler.static_scheduler.utils import *
import logging
import rapidnetsim.scheduler.static_scheduler.job_request as job_request
import rapidnetsim.scheduler.static_scheduler.gurobi_solver as gurobi_solver
import operator
import gurobipy
import numpy as np


logging.basicConfig(level= logging.ERROR)

class static_scheduler:
    def __init__(self, clos_n, clos_m, _spine_switch_port_num, _leaf_switch_port_num):#spine_switch_num, leaf_switch_num, spine_switch_port_num, leaf_switch_port_num):
        print("debug clos_n",clos_n, clos_m)
        spine_switch_num = clos_n
        leaf_switch_num = clos_m
        
        spine_switch_port_num = _spine_switch_port_num
        leaf_switch_port_num = _leaf_switch_port_num
        self.spine_num = spine_switch_num
        self.leaf_num = leaf_switch_num
        self.gpu_per_switch = int(leaf_switch_port_num / 2)
        self.gpu_num = self.gpu_per_switch * leaf_switch_num
        self.spine_switch_port_num = spine_switch_port_num
        self.leaf_switch_port_num = leaf_switch_port_num
        self.gpu_per_server = 4
        self.gpu_per_leaf = int(self.gpu_num/self.leaf_num)
        self.server_per_leaf = int(self.gpu_per_leaf/self.gpu_per_server)
        # self.gpu_per_server = 8
        # self.allocated_gpu_num = 0
        self.gpus = []
        for i in range(self.leaf_num):
            self.gpus.append(np.zeros(int(leaf_switch_port_num / 2)))

        logging.debug("cluster size: %d gpus" % self.gpu_num)
        

        
    def _translate_hull_cluster_allocation(self):
        #output
        allocation_link_mapping = []
        for tmp_gpu_id in range(self.gpu_num):
            tmp_leaf_id = int(tmp_gpu_id/self.gpu_per_leaf) + self.gpu_num
            allocation_link_mapping.append((tmp_gpu_id, tmp_leaf_id, 1))
            allocation_link_mapping.append((tmp_leaf_id, tmp_gpu_id, 1))
        for tmp_leaf_id in range(self.gpu_num, self.gpu_num+self.leaf_num):
            leaf_spine_link = int(self.gpu_per_leaf/self.spine_num)
            for tmp_spine_id in range(self.gpu_num+self.leaf_num, self.gpu_num+self.leaf_num+self.spine_num):
                allocation_link_mapping.append((tmp_leaf_id, tmp_spine_id, leaf_spine_link))
                allocation_link_mapping.append((tmp_spine_id, tmp_leaf_id, leaf_spine_link))
        # print(allocation_link_mapping)
        return allocation_link_mapping

        
    def schedule(self, gpu_num, job_id, sim_time, queued_jobs):
        allocate_success = True
        gpu_indexes = [i for i in range(gpu_num)]
        allocated_link_mapping = self._translate_hull_cluster_allocation()
        # print(gpu_indexes)
        # print(allocated_link_mapping)
        return allocate_success, gpu_indexes, allocated_link_mapping, None, None #allocate_success, gpu_indexes, link_mapping


    def update_finished_job(self, job_id, sim_time, queued_jobs):
        print("hash_finish")