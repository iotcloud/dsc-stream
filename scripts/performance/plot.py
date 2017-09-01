import matplotlib.pyplot as plt
import numpy as np
import os

data = [1000, 2000, 4000, 8000, 16000, 32000]
xlabels = [1, 2, 4, 8, 16, 32]
markers=["o", "x", "^", "v", "D", "*"]
cls=["red", "blue", "green", "yellow", "black", "cyan", "magenta"]

def plot_line(y=None, x=None, xlabel=None, ylabel=None, title=None, col=None, legend=None, plot=None, logy=False, ylim=None, legendloc=None) :
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
    if not plot:
        p.show()
    return plt

def plot_bar(y=None, x=None, xlabel=None, ylabel=None, title=None, col=None, legend=None, plot=None, logy=False, ylim=None, legendloc=None, y_std=None) :
    N = 3
    ind = np.arange(N)

    if not plot:
        p = plt
    else:
        p = plot
    if not col:
        col = cls

    l = []
    for i in range(len(y)):
        temp = None
        if logy:
            temp = p.bar(ind, y[i], color=col[i], yerr=y_std[i])
        else:
            temp = p.bar(ind, y[i], color=col[i], yerr=y_std[i])
        l.append(temp[0])

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
            p.legend((l[0], l[1], l[2]),legend, loc=legendloc, fancybox=True, framealpha=0.25)
        else:
            p.legend((l[0], l[1], l[2]), legend, loc="upper left", fancybox=True, framealpha=0.25)
    p.minorticks_on()
    p.grid(b=True, which='major', color='k', linestyle='-')
    p.grid(b=True, which='minor', color='grey', linestyle='-', alpha=0.2)
    if not plot:
        p.show()
    return plt

def plot_latency_top_a():
    # plot_line([[1,2,3]], [3,4,5], [[.1,.1,.1]])
    plot_bar([[1,2,3]], [3,4,5], y_std=[[.1,.1,.1]])

def main():
    plot_latency_top_a()

if __name__ == "__main__":
    main()