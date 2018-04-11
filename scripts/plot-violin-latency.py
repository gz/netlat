import os, sys
import glob
import numpy
import matplotlib
matplotlib.use('Agg')

from matplotlib import pyplot as plt, font_manager

import numpy as np
import pandas as pd
import math

if __name__ == '__main__':

    fig, axes = plt.subplots(nrows=math.floor(len(sys.argv) / 3)+1, ncols=3, figsize=(14,14))
    min_all = 10**10
    max_all = 0

    for i, f in enumerate(sys.argv[1:]):

        filename, file_extension = os.path.splitext(os.path.basename(f))
        raw_data = pd.read_csv(f, skipinitialspace=True)
        series = raw_data['latency_ns']
        rseries = pd.Series(series)
        if min_all > rseries.min():
            min_all = rseries.min()
        if max_all < rseries.max():
            max_all = rseries.max()

        axes[int(i/3), i%3].set_ylabel('Latency [ns]')
        axes[int(i/3), i%3].set_xlabel('')

        axes[int(i/3), i%3].spines['top'].set_visible(False)
        axes[int(i/3), i%3].spines['right'].set_visible(False)
        axes[int(i/3), i%3].get_xaxis().tick_bottom()
        axes[int(i/3), i%3].get_yaxis().tick_left()

        axes[int(i/3), i%3].violinplot(rseries, points=20, widths=0.1, showmeans=True, showextrema=True, showmedians=True)
        axes[int(i/3), i%3].set_title(filename.split("-")[1])
        axes[int(i/3), i%3].set_yscale("log", nonposy='clip')


    for (m,n), subplot in numpy.ndenumerate(axes):
        subplot.set_ylim(min_all,max_all)

    plt.legend()
    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.40, hspace=0.2)

    plt.savefig("plot-violin-latency.png", format='png', pad_inches=0.0)
    plt.savefig("plot-violin-latency.pdf", format='pdf', pad_inches=0.0)