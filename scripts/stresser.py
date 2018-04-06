#!/usr/bin/env python

import time
import math
import sys

if __name__ == '__main__':
    percent_cpu = sys.argv[1]

    time_of_run = 0.1
    cpu_time_utilisation = float(percent_cpu) / 100
    on_time = time_of_run * cpu_time_utilisation
    off_time = time_of_run * (1-cpu_time_utilisation)

    while True:
        start_time = time.clock()
        while time.clock() - start_time < on_time:
            math.factorial(100)
        time.sleep(off_time)
