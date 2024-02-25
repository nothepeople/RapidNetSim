

from rapidnetsim.core.infrastructure.flow import Flow


def refresh_taskstep_finish_event():
    from rapidnetsim.core.simulator import Simulator
    from rapidnetsim.core.event.taskstep_finish_event import TaskStepFinishEvent

    inflight_taskstep_info = Simulator.get_inflight_taskstep_info()
    earliest_finish_obj = None
    earliest_finish_time = float('inf')
    
    # Fix the bug of repeating TaskStepFinishEvent
    if Simulator.LAST_TASKSTEPFINISHEVENT != None:
        Simulator.LAST_TASKSTEPFINISHEVENT.change_to_inactive()

    for taskid, taskstep_obj in inflight_taskstep_info.items():
        taskstep_obj.has_cal_time += (Simulator.get_current_time() - taskstep_obj.get_last_step_calculated_time())
        expected_finish_time = max(0.0000001,taskstep_obj.get_expected_finish_time() - taskstep_obj.has_cal_time)
        #print("debug expected_finish_time",taskstep_obj._taskid, taskstep_obj._stepid, taskstep_obj.get_expected_finish_time(), taskstep_obj.has_cal_time, Simulator.get_current_time())
        if expected_finish_time <= earliest_finish_time:
            earliest_finish_obj = taskstep_obj 
            earliest_finish_time = expected_finish_time
        taskstep_obj.set_last_step_calculated_time(Simulator.get_current_time())

    if earliest_finish_obj != None:
        earliest_finish_event = TaskStepFinishEvent(earliest_finish_time, earliest_finish_obj)
        Simulator.register_event(earliest_finish_event)
        # Fix the bug of repeating TaskStepFinishEvent
        Simulator.LAST_TASKSTEPFINISHEVENT = earliest_finish_event
