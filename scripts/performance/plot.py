import matplotlib.pyplot as plt
import numpy as np
import os

data = [1000, 2000, 4000, 8000, 16000, 32000]
data = [1000, 2000, 4000, 8000, 16000, 32000]

x_small = [16, 32, 64, 128, 256, 512]
xlabels_large = [16, 32, 64, 128, 256, 512]
xlabels_small = [16, 32, 64, 128, 256, 512]

markers=["o", "x", "^", "v", "D", "*"]
cls=["red", "blue", "green", "yellow", "black", "cyan", "magenta"]

def plot_line(y=None, x=None, xlabel=None, ylabel=None, title=None, col=None, legend=None, plot=None, logy=False, ylim=None, legendloc=None, ticks=None) :
    if not plot:
        p = plt
    else:
        p = plot
    if not col:
        col = cls
    for i in range(len(y)):
        if logy:
            p.semilogy(x, y[i], color=col[i], marker=markers[i])
        else:
            p.plot(x, y[i], color=col[i], marker=markers[i])

    if ylim:
        p.ylim(ylim)
    if not xlabel:
        xlabel = 'message size (KB)'
    p.xlabel(xlabel)
    if not ylabel:
        ylabel = 'time (ms)'
    p.ylabel(ylabel)
    if ticks:
        plt.xticks(np.array([0, 64, 128,256,512]))

    if title:
        p.title(title)

    for l in y:
        print title, l
    p.grid(True)
    if legend:
        if legendloc:
            p.legend(legend, loc=legendloc, fancybox=True, framealpha=0.25)
        else:
            p.legend(legend, loc="upper left", fancybox=True, framealpha=0.25)
    p.minorticks_on()
    p.grid(b=True, which='major', color='k', linestyle='-')
    p.grid(b=True, which='minor', color='grey', linestyle='-', alpha=0.2)
    p.tight_layout()
    if not plot:
        p.show()
    return plt

def plot_bar(y=None, x=None, xlabel=None, ylabel=None, title=None, col=None, legend=None, plot=None, logy=False, ylim=None, legendloc=None, y_std=None) :
    N = 3
    width = .15
    ind = np.arange(0, .5*N, .5)

    if not plot:
        p = plt
    else:
        p = plot
    if not col:
        col = cls

    l = []
    current_width = 0
    for i in range(len(y)):
        temp = None
        if logy:
            temp = p.bar(ind + current_width, y[i], width, color=col[i], yerr=y_std[i])
        else:
            temp = p.bar(ind + current_width, y[i], width, color=col[i], yerr=y_std[i])
        l.append(temp[0])
        current_width = current_width + width

    p.xticks(ind + width / 2, x)

    if ylim:
        p.ylim(ylim)
    if not xlabel:
        xlabel = 'message size (KB)'
    p.xlabel(xlabel)
    if not ylabel:
        ylabel = 'time (ms)'
    p.ylabel(ylabel)

    if title:
        p.title(title)

    for l in y:
        print title, l
    p.grid(True)
    if legend:
        if legendloc:
            p.legend(legend, loc=legendloc, fancybox=True, framealpha=0.25)
        else:
            p.legend(legend, loc="upper left", fancybox=True, framealpha=0.25)
    # p.minorticks_on()
    # p.grid(b=True, which='major', color='k', linestyle='-')
    # p.grid(b=True, which='minor', color='grey', linestyle='-', alpha=0.2)
    p.tight_layout()
    if not plot:
        p.show()
    return plt

def plot_latency_ib():
    # plot_line([[1,2,3]], [3,4,5], [[.1,.1,.1]])
    y_long_large = [[14.2,	14.8,	18.9,	29.7,	55.8,	112],
                    [14.1,	14.6,	14.74,	14.95,	21,	45],
                    [8.5,	8.5,	10.04,	10.2,	14.5,	29.59]]
    y_long_small = [[29.99,29.3,29.67,30.4,29.64,28.9],
                    [28,	27,	28,	28,	29,	29],
                    [17,	17.2,	17.6,	17.4,	17.8,17]]
    y_short_large = [[3.94,	5.46,	10.9,	21.5,	43.53,	88.25],
                     [3.41,	3.56,	5.13,	8.32,	15.38,	41],
                     [2.64,	2.1,	2.73,	4.8,	8,	20.92]]
    y_short_small = [[5,	5.1,	5,	5.2,	5.3,	5.5],
                     [5,	5.1,	5.2,	5.1,	5.3,	5.4],
                     [2.8,	2.8,	2.8,	2.8,	2.9,	2.9]]
    # y_short_small = [[29.7,	45.2,	71.68],
    #                     [14.9,	17.6,	27.33],
    #                     [10.2,	10.59,	13.8]]

    fig = plt.figure(figsize=(14, 4), dpi=100)

    plt.subplot2grid((1,27), (0, 0), colspan=6)
    plot_line(y_long_large, x=x_small, legend=["TCP", "IPoIB", "IB"], title="Top. A Large Messages", plot=plt, ticks=xlabels_large)

    plt.subplot2grid((1,27), (0, 7), colspan=6)
    plot_line(y_long_small, x=x_small, xlabel="Message size bytes", legend=["TCP", "IPoIB", "IB"], title="Top. A Small Messages", plot=plt, ticks=xlabels_small)

    plt.subplot2grid((1,27), (0, 14), colspan=6)
    plot_line(y_short_large, x=x_small, legend=["TCP", "IPoIB", "IB"], title="Top. B Large Messages", plot=plt, ticks=xlabels_large)

    plt.subplot2grid((1,27), (0, 21), colspan=6)
    plot_line(y_short_small, x=x_small, xlabel="Message size bytes", legend=["TCP", "IPoIB", "IB"], title="Top. B Small Messages", plot=plt, ticks=xlabels_small)
    plt.show()

    plt.show()

def plot_latency_parallel_ib():
    # plot_line([[1,2,3]], [3,4,5], [[.1,.1,.1]])
    y_long_large = [[29.7,	45.2,	71.68],
                        [14.9,	17.6,	27.33],
                        [10.2,	10.59,	13.8]]
    y_long_large_std = [[1.6,	5.15,	14.8],
                        [1,	2.4,	2.7],
                        [0.85,	1.1,	1.4]]
    y_short_large = [[11.5,	21.5,	39.81],
                     [5.65,	8.32,	15.54],
                     [3.55,	4.8,	8.34]]

    y_long_small = [[30.4,	30.8,	40.14],
                    [28,	29,	40],
                    [17.4,	17.2,	22.2]]
    y_short_small = [[3.65,	5.2,	8.8],
                     [3.7,	5.1,	8.4],
                    [2.5,	2.8,	4.2]]

    fig = plt.figure(figsize=(14, 4), dpi=100)

    plt.subplot2grid((1,27), (0, 0), colspan=6)
    plot_bar(y_long_large, x=[2,4,8], xlabel="Parallel", legend=["TCP", "IPoIB", "IB"], title="Top. A Large Messages", plot=plt, y_std=y_long_large_std)

    plt.subplot2grid((1,27), (0, 7), colspan=6)
    plot_bar(y_long_small, x=[2,4,8], xlabel="Parallel", legend=["TCP", "IPoIB", "IB"], title="Top. A Small Messages", plot=plt, y_std=y_long_large_std)

    plt.subplot2grid((1,27), (0, 14), colspan=6)
    plot_bar(y_short_large, x=[2,4,8], xlabel="Parallel", legend=["TCP", "IPoIB", "IB"], title="Top. B Large Messages", plot=plt, y_std=y_long_large_std)

    plt.subplot2grid((1,27), (0, 21), colspan=6)
    plot_bar(y_short_small, x=[2,4,8], xlabel="Parallel", legend=["TCP", "IPoIB", "IB"], title="Top. B Small Messages",plot=plt, y_std=y_long_large_std)
    plt.show()


def main():
    plot_latency_ib()
    plot_latency_parallel_ib()

if __name__ == "__main__":
    main()