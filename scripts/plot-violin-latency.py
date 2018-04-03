import os, sys
import glob

import matplotlib
matplotlib.use('Agg')

from matplotlib import pyplot as plt, font_manager

import numpy as np
import pandas as pd

if __name__ == '__main__':

    fig, axes = plt.subplots(nrows=int(len(sys.argv)-1/3), ncols=3, figsize=(10,10))

    for i, f in enumerate(sys.argv[1:]):
        filename, file_extension = os.path.splitext(os.path.basename(f))

        raw_data = pd.read_csv(f, skipinitialspace=True)
        series = raw_data['latency_ns']
        rseries = pd.Series(series)


        axes[int(i/3), i%3].set_ylabel('Latency [ns]')
        axes[int(i/3), i%3].set_xlabel('')

        axes[int(i/3), i%3].spines['top'].set_visible(False)
        axes[int(i/3), i%3].spines['right'].set_visible(False)
        axes[int(i/3), i%3].get_xaxis().tick_bottom()
        axes[int(i/3), i%3].get_yaxis().tick_left()

        axes[int(i/3), i%3].violinplot(rseries, points=20, widths=0.1, showmeans=True, showextrema=True, showmedians=True)
        axes[int(i/3), i%3].set_title(filename.split("-")[1])

    plt.legend()
    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.4, hspace=None)

    plt.savefig("plot-violin-latency.png", format='png')
    plt.savefig("plot-violin-latency.pdf", format='pdf')