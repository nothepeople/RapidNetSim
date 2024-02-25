
from jinja2 import Environment, PackageLoader
from base_conf_template.create_clos import create_clos_connect_list
from base_conf_template.generate_tasks import get_fixed_requests_256_part_tasks
from base_conf_template.generate_tasks import generate_tasks, get_fixed_requests_256_part_tasks_TPUv4_512


if __name__ == '__main__':
    NIC_num = 8192
    leaf_switch_num = 128
    leaf_switch_port_num = 128
    spine_switch_num = 64
    spine_switch_port_num = 128
    NIC_num_in_a_server = 4

    # joint_scheduler = 'oxc_scheduler'
    joint_scheduler = 'static_scheduler'

    # [(arriving_time, model_size at every NIC, task_occupied_NIC_num), ...]
    task_list = get_fixed_requests_256_part_tasks_TPUv4_512(5000, True,  0.22, modify = False,  random_beta = True, beta_list = [241]  )

    

    if joint_scheduler == 'static_scheduler':
        connect_info_list = create_clos_connect_list(NIC_num, leaf_switch_num, spine_switch_num, leaf_switch_port_num, spine_switch_port_num, NIC_num_in_a_server)
        connect_info_str = str(connect_info_list)
    else:
        connect_info_str = []

    env = Environment(loader = PackageLoader('base_conf_template', './'))
    template = env.get_template('base_ini_template.j2')
    content = template.render(
        connect_info_str = connect_info_str,
        topo_type = 'clos',
        find_path_method = 'shortest',
        joint_scheduler = joint_scheduler,
        measure_sampling_interval = 10,
        
    
        find_next_hop_method = 'static_routing',
        greedy = 'yes',
        reconfiguration = 'no',
        waiting_task_order_mode = 'FIFO',
        
        


        NIC_num = NIC_num,
        NIC_num_in_a_server = NIC_num_in_a_server,

        leaf_switch_num = leaf_switch_num,

        
        leaf_switch_port_num = leaf_switch_port_num,
        spine_switch_port_num = spine_switch_port_num,

        spine_switch_num = spine_switch_num,

        inner_server_bandwidth = 1000,
        # 100Gbps = 100 * 1024 * 1024 * 1024 bit / 1000000000 ns ~= 100 bit / ns
        switch_port_bandwidth = 100,

        computation_time = 1,

        #task_type = 'ring',
        task_type = 'randomly',
        task_list = task_list,
        task_iteration_num = 1,
    )
    with open('exp.ini', 'w') as f:
        f.write(content)
