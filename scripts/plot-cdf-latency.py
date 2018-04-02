import os, sys
import glob

import matplotlib
matplotlib.use('Agg')

from matplotlib import pyplot as plt, font_manager

import numpy as np
import pandas as pd

if __name__ == '__main__':

    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    ax1.set_ylabel('CDF [%]')
    ax1.set_xlabel('Latency [ns]')

    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.get_xaxis().tick_bottom()
    ax1.get_yaxis().tick_left()

    for f in sys.argv:
        filename, file_extension = os.path.splitext(os.path.basename(f))

        raw_data = pd.read_csv(f, skipinitialspace=True)
        series = raw_data['latency_ns']
        series = series.sort_values()
        series[len(series)] = series.iloc[-1]
        cum_dist = np.linspace(0.,1.,len(series))
        cdf = pd.Series(cum_dist, index=series)

        print (f, "Samples", len(series))
        print ("50 percentile", np.percentile(series, 50))
        print ("95 percentile", np.percentile(series, 95))
        print ("99 percentile", np.percentile(series, 99))
        print ("99.9 percentile", np.percentile(series, 99.9))
        print ("100 percentile", np.percentile(series, 100))

        p = ax1.plot(cdf, label=f)

        ax1.get_xaxis().get_major_formatter().set_useOffset(False)
        #plt.setp(ax1.get_xticklabels(), fontproperties=ticks_font)
        #plt.setp(ax1.get_yticklabels(), fontproperties=ticks_font)

        plt.savefig(os.path.join(filename + ".png"), format='png', pad_inches=0.0)
        plt.savefig(os.path.join(filename + ".pdf"), format='pdf', pad_inches=0.0)