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
    boss = DataBoss.UberDataBoss(socket_pub_nr=5555, debug_time=1)

    # create DataFuser object
    fuser = DataFuser.SimpleFileDataFuser(boss.q, debug_time = boss.debug_time)

    # assign fuser to boss
    boss.fuser = fuser
    
    # create the DataAggregators
    agg1 = DataAggregator.ZmqDataAggregator('agg1', boss.q, boss.context, socket_nr=5556, debug_time=boss.debug_time)
    agg2 = DataAggregator.ZmqDataAggregator('agg2', boss.q, boss.context, socket_nr=5557, debug_time=boss.debug_time)
    agg3 = DataAggregator.ZmqDataAggregator('agg3', boss.q, boss.context, socket_nr=5558, debug_time=boss.debug_time)
    agg4 = DataAggregator.ZmqDataAggregator('agg4', boss.q, boss.context, socket_nr=5559, debug_time=boss.debug_time)

    # create the DataGenerators
    gen1 = DataGenerator.LocalZmqSubDataGenerator('gen1', boss.context, socket_sub_nr=boss.socket_pub_nr, socket_nr=5556, debug_time=boss.debug_time, sleep = -0.1)
    gen2 = DataGenerator.LocalZmqSubDataGenerator('gen2', boss.context, socket_sub_nr=boss.socket_pub_nr, socket_nr=5557, debug_time=boss.debug_time, sleep = -0.1)
    gen3 = DataGenerator.LocalZmqSubDataGenerator('gen3', boss.context, socket_sub_nr=boss.socket_pub_nr, socket_nr=5558, debug_time=boss.debug_time, sleep = -0.1)
    gen4 = DataGenerator.LocalZmqSubDataGenerator('gen4', boss.context, socket_sub_nr=boss.socket_pub_nr, socket_nr=5559, debug_time=boss.debug_time, sleep = -0.1)
    
    # assign aggregators to boss
    boss.add_agg(agg1)
    boss.add_agg(agg2)
    boss.add_agg(agg3)
    boss.add_agg(agg4)

    # assign generators to boss
    boss.add_gen(gen1)
    boss.add_gen(gen2)
    boss.add_gen(gen3)
    boss.add_gen(gen4)
    
    print('start the boss')
    boss.start()

    # let it run for some time and print status at regular intervals
    i = 0
    while i < 10:
        print(boss.get_status())
        if i >-1:
            boss.publish('Boss Running')
        if i >=9:
            boss.publish('Boss Stopped')
        i += 1
        time.sleep(1.0)
    
    
    boss.end()

    print(fuser.get_summary_str())
