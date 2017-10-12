#
#
# Run locally with flow control
#
#


import DataFuser
import DataBoss
import DataAggregator
import DataGenerator
import time
import sys

if __name__ == '__main__':

    
    # create the Boss
    boss = DataBoss.PubDataBoss(debug_time=1)

    
    # create the DataAggregators
    n = 4
    nr = 5556
    for a in range(n):
        agg = DataAggregator.ZmqDataAggregator('agg%d'%a, boss.q, boss.context, socket_nr=nr, debug_time=boss.debug_time)
        gen = DataGenerator.LocalZmqSubDataGenerator('gen%d'%a, boss.context, socket_sub_nr=boss.publisher.socket_nr, socket_nr=nr, debug_time=boss.debug_time, sleep = -0.1)
        boss.add_agg(agg)
        boss.add_gen(gen)
        nr += 1
    
    
    print('start the boss')
    boss.start()

    # let it run for some time and print status at regular intervals
    i = 0
    while i < 10:
        print(boss.get_status())
        if i == 1:
            boss.publisher.publish('Running')
        if i ==4:
            boss.publisher.publish('Stopped')
        i += 1
        time.sleep(1.0)
    
    boss.end()

    print(boss.fuser.get_summary_str())
