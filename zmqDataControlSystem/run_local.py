#
#
# Run locally
#
#


import DataFuser
import DataBoss
import DataAggregator
import time

if __name__ == '__main__':


    # create the Boss
    boss = DataBoss.DataBoss(debug_time=1)

    # create DataFuser object
    fuser = DataFuser.SimpleFileDataFuser(boss.q, debug_time = boss.debug_time)

    # assign fuser to boss
    boss.fuser = fuser


    # create the DataAggregators
    agg = DataAggregator.LocalDataAggregator(boss.q, debug_time=boss.debug_time, sleep = 0.2)
    agg2 = DataAggregator.LocalDataAggregator(boss.q, debug_time=boss.debug_time, sleep = 0.2)

    # assign to boss
    boss.add_agg(agg)
    boss.add_agg(agg2)
    
    print(boss.get_status())

    print('start the boss')
    boss.start()

    # let it run for some time
    i = 0
    while i < 5:
        print(boss.get_status())
        time.sleep(1.0)
        i += 1

    boss.end()

