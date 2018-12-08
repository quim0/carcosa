GOOD_STATES = ['complete', 'completed', 'special_exit']
ACTIVE_STATES = ['configuring', 'completing', 'pending',
                 'held', 'running', 'submitted']
BAD_STATES = ['boot_fail', 'cancelled', 'failed', 'killed',
              'node_fail', 'timeout', 'disappeared']
UNCERTAIN_STATES = ['preempted', 'stopped',
                    'suspended']
ALL_STATES = GOOD_STATES + ACTIVE_STATES + BAD_STATES + UNCERTAIN_STATES
DONE_STATES = GOOD_STATES + BAD_STATES
