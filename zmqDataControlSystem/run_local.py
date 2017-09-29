#
#
# Run locally
#
#


import DataBoss
import time

if __name__ == '__main__':

    boss = DataBoss.DataBoss()

    print('start')

    boss.start()


    i = 0
    while i < 10:
        boss.print_status()
        print(' sleep ')
        time.sleep(1)
        i += 1

    boss.end()

