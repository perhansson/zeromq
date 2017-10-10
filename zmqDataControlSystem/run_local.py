#
#
# Run locally
#
#


import DataFuser
import DataBoss
import DataAggregator
import DataGenerator
import time

if __name__ == '__main__':


    # create the Boss
    boss = DataBoss.DataBoss(debug_time=1)

    # create DataFuser object
    fuser = DataFuser.SimpleFileDataFuser(boss.q, debug_time = boss.debug_time)
    #fuser = DataFuser.SimpleDumpDataFuser(boss.q, debug_time = boss.debug_time)

    # assign fuser to boss
    boss.fuser = fuser


    # create the DataAggregators
    agg1 = DataAggregator.ZmqDataAggregator('agg1', boss.q, socket_nr=5556, debug_time=boss.debug_time)
    agg2 = DataAggregator.ZmqDataAggregator('agg2', boss.q, socket_nr=5557, debug_time=boss.debug_time)
    agg3 = DataAggregator.ZmqDataAggregator('agg3', boss.q, socket_nr=5558, debug_time=boss.debug_time)
    agg4 = DataAggregator.ZmqDataAggregator('agg4', boss.q, socket_nr=5559, debug_time=boss.debug_time)

    # create the DataGenerators
    gen1 = DataGenerator.LocalZmqDataGenerator('gen1', socket_nr=5556, debug_time=boss.debug_time, sleep = -1.0)
    gen2 = DataGenerator.LocalZmqDataGenerator('gen2', socket_nr=5557, debug_time=boss.debug_time, sleep = -1.0)
    gen3 = DataGenerator.LocalZmqDataGenerator('gen3', socket_nr=5558, debug_time=boss.debug_time, sleep = -1.0)
    gen4 = DataGenerator.LocalZmqDataGenerator('gen4', socket_nr=5559, debug_time=boss.debug_time, sleep = -1.0)

    
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
    
    print(boss.get_status())

    print('start the boss')
    boss.start()

    # let it run for some time and print status at regular intervals
    i = 0
    while i < 5:
        print(boss.get_status())
        time.sleep(1.0)
        i += 1

    boss.end()

    print(fuser.get_summary_str())
