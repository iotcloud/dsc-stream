import matplotlib.pyplot as plt
import numpy as np
import os

data = [1000, 2000, 4000, 8000, 16000, 32000]
data = [1000, 2000, 4000, 8000, 16000, 32000]

x_small = [16, 32, 64, 128, 256, 512]
xlabels_large = [16, 32, 64, 128, 256, 512]
xlabels_small = [16, 32, 64, 128, 256, 512]

markers=["o", "x", "^", "v", "D", "*"]
cls=["red", "blue", "green", "cyan", "yellow", "black", "magenta"]

def plot_line(y=None, x=None, xlabel=None, ylabel=None, title=None, col=None, legend=None, plot=None, logy=False, ylim=None, legendloc=None, ticks=None, ymin=None, ymax=None, mrks=True, y_ticks=None) :
    if not plot:
        p = plt
    else:
        p = plot
    if not col:
        col = cls

    for i in range(len(y)):
        if logy:
            if mrks:
                p.semilogy(x, y[i], color=col[i], marker=markers[i])
            else:
                p.semilogy(x, y[i], color=col[i], linewidth=2.0)
        else:
            if mrks:
                p.plot(x, y[i], color=col[i], marker=markers[i])
            else:
                p.plot(x, y[i], color=col[i], linewidth=2.0)

    if ylim:
        p.ylim(ylim)
    if not xlabel:
        xlabel = 'message size (KB)'
    p.xlabel(xlabel)
    if ylabel:
        # ylabel = 'time (ms)'
        p.ylabel(ylabel)

    if ymin:
        p.ylim(ymin=ymin)
    if ymax:
        p.ylim(ymax=ymax)
    if ticks:
        p.xticks(np.array([0, 64, 128,256,512]))

    if y_ticks != None and y_ticks.any():
        plt.yticks(y_ticks)

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
    p.grid(b=True, which='minor', color='grey', linestyle='-', alpha=0.1)
    # p.tight_layout()
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
            if y_std:
                temp = p.bar(ind + current_width, y[i], width, color=col[i], yerr=y_std[i])
            else:
                temp = p.bar(ind + current_width, y[i], width, color=col[i])
        l.append(temp[0])
        current_width = current_width + width

    p.xticks(ind + width / 2, x)

    if ylim:
        p.ylim(ylim)
    if not xlabel:
        xlabel = 'message size (KB)'
    p.xlabel(xlabel)
    if ylabel:
        # ylabel = 'time (ms)'
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
    y_long_large = [[14.2,	14.8,	18.9,	28.5,	52.4,	104.8],
                    [14.1,	14.6,	14.74,	14.95,	21,	    45],
                    [7.9,	8.2,	9.81,	10.2,	14.0,	27.8]]
    y_long_small = [[3.1,3.1,3.09,3.11,3.1,3.09],
                    [3.0,3.1,3.08,3.1,3.08,3.07],
                    [1.2,1.3,1.3,1.3,1.4,1.4]]

    y_long_small_parallel = [[3.1,	5.51,	10.2],
                    [3.1,	5.79,	10.8],
                    [1.4,2.4,5.4]]
    y_long_small_std = [[.4,	.9,	1.9],
                    [.5,	.9,	1.4],
                    [0.4,	1.1,	1.4]]

    y_long_large_parallel = [[29.7,	45.2,	71.68],
                    [14.9,	17.6,	27.33],
                    [10.2,	10.7,	13.8]]
    y_long_large_std = [[1.6,	5.15,	14.8],
                    [1,	2.4,	2.7],
                    [0.85,	1.1,	1.4]]

    fig = plt.figure(figsize=(18, 4), dpi=100)

    plt.subplot2grid((1,35), (0, 0), colspan=8)
    plot_line(y_long_large, x=x_small, legend=["TCP", "IPoIB", "IB"], title="a) Top. A Large Messages", plot=plt, ticks=xlabels_large, ylabel="time(ms)")

    plt.subplot2grid((1,35), (0, 9), colspan=8)
    plot_line(y_long_small, x=x_small, xlabel="Message size bytes", legend=["TCP", "IPoIB", "IB"], title="b) Top. A Small Messages", plot=plt, ticks=xlabels_small, ymin=0, ymax=4.5)

    plt.subplot2grid((1,35), (0, 18), colspan=8)
    plot_bar(y_long_large_parallel, x=[2,4,8], xlabel="Parallel", legend=["TCP", "IPoIB", "IB"], title="c) Top. A Large Messages", plot=plt, y_std=y_long_large_std, ylabel="time (ms)")

    plt.subplot2grid((1,35), (0, 27), colspan=8)
    plot_bar(y_long_small_parallel, x=[2,4,8], xlabel="Parallel", legend=["TCP", "IPoIB", "IB"], title="d) Top. A Small Messages", plot=plt, y_std=y_long_small_std)

    plt.subplots_adjust(left=0.06, right=0.98, top=0.9, bottom=0.2)
    fig.tight_layout()
    plt.show()

def plot_latency_parallel_ib():
    y_short_large = [[3.94,	5.46,	9.7,	19.8,	39.9,	81.8],
                     [2.9,	3.56,	4.9,	7.8,	13.9,	38.8],
                     [1.8,	2.0,	2.4,	3.9,	6.2,	20.2]]
    y_short_small = [[2.4,	2.36,	2.35,	2.3,	2.3,	2.33],
                     [2.36,	2.34,	2.33,	2.33,	2.32,	2.32],
                     [1.02,	1.01,	1.2,	1.2,	1.2,	1.3]]

    y_short_large_parallel = [[11.5,	21.5,	39.81],
                     [5.65,	8.32,	15.54],
                     [3.55,	4.8,	8.34]]
    y_short_large_std = [[9.4,	15.15,	14.8],
                        [1,	2.4,	2.7],
                        [1.5,	2.8,	4.1]]


    y_short_small_parallel = [[3.65,	5.2,	8.8],
                     [3.7,	5.1,	8.4],
                    [2.5,	2.8,	4.2]]
    y_short_small_std = [[.6,	.8,	1.8],
                        [.5,	.8,	1.4],
                        [0.5,	.6,	.8]]
    fig = plt.figure(figsize=(18, 4), dpi=100)

    plt.subplot2grid((1,35), (0, 0), colspan=8)
    plot_line(y_short_large, x=x_small, legend=["TCP", "IPoIB", "IB"], title="a) Top. B Large Messages", plot=plt, ticks=xlabels_large)

    plt.subplot2grid((1,35), (0, 9), colspan=8)
    plot_line(y_short_small, x=x_small, xlabel="Message size bytes", legend=["TCP", "IPoIB", "IB"], title="b) Top. B Small Messages", plot=plt, ticks=xlabels_small, ymin=0, ymax=3.5)

    plt.subplot2grid((1,35), (0, 18), colspan=8)
    plot_bar(y_short_large_parallel, x=[8,16,32], xlabel="Parallel", legend=["TCP", "IPoIB", "IB"], title="c) Top. B Large Messages", plot=plt, y_std=y_short_large_std)

    plt.subplot2grid((1,35), (0, 27), colspan=8)
    plot_bar(y_short_small_parallel, x=[8,16,32], xlabel="Parallel", legend=["TCP", "IPoIB", "IB"], title="d) Top. B Small Messages",plot=plt, y_std=y_short_small_std)
    plt.subplots_adjust(left=0.06, right=0.98, top=0.9, bottom=0.2)
    fig.tight_layout()
    plt.show()

def plot_yahoo_percentages():
    y = [[7.039797,10.117806,10.589877,10.873941,11.098632,11.28215,11.440547,11.579306,11.709781,11.824966,11.930828,12.03175,12.123004,12.209889,12.294789,12.375119,12.450744,12.523882,12.596281,12.666217,12.734619,12.798721,12.862758,12.923179,12.982774,13.041543,13.101455,13.159196,13.21688,13.270119,13.327192,13.382509,13.436452,13.487521,13.543312,13.596373,13.648969,13.701298,13.75451,13.809505,13.863191,13.915876,13.967294,14.018828,14.072233,14.124837,14.177013,14.230307,14.281666,14.333721,14.385401,14.437565,14.490591,14.54423,14.597051,14.652034,14.70735,14.759708,14.816276,14.873087,14.928759,14.985485,15.04317,15.098029,15.158724,15.218162,15.275214,15.337752,15.398731,15.461727,15.526041,15.592246,15.656146,15.725281,15.793151,15.864217,15.936833,16.011342,16.087012,16.169644,16.253351,16.340396,16.432418,16.525694,16.623472,16.729063,16.839053,16.95421,17.078758,17.211549,17.36127,17.511954,17.6956,17.911305,18.136767,18.40708,18.730862,19.15332,19.72492,20.759459,30.210159],
        [3.767712,5.856978,6.209369,6.447664,6.62835,6.77462,6.900314,7.012553,7.105544,7.187125,7.266193,7.340336,7.412796,7.483377,7.549925,7.613854,7.674134,7.730904,7.78501,7.836628,7.889687,7.941473,7.991609,8.04007,8.086344,8.131632,8.17257,8.213329,8.25371,8.294707,8.33526,8.377199,8.419833,8.463186,8.504432,8.545016,8.585796,8.625832,8.665502,8.704376,8.746594,8.784539,8.822784,8.86197,8.901284,8.939165,8.977604,9.016605,9.055745,9.095199,9.134379,9.172984,9.210105,9.244291,9.279829,9.318483,9.355261,9.393737,9.432999,9.474873,9.516201,9.554545,9.596222,9.639099,9.681407,9.722926,9.764344,9.810381,9.854142,9.899422,9.947953,9.996063,10.041337,10.089357,10.136615,10.184712,10.229935,10.276499,10.324289,10.375508,10.426951,10.481439,10.539428,10.601025,10.663702,10.728453,10.792754,10.864732,10.938187,11.017464,11.101996,11.18635,11.284065,11.39123,11.51097,11.661572,11.835572,12.049211,12.34516,12.809641,22.144451]
        ]
    print len(y[0]),len(y[1])
    x = np.arange(0,101)
    print len(x)
    print x
    fig = plt.figure(figsize=(4, 2.5), dpi=100)
    plot_line(y, x, mrks=False, ylabel="Time (ms)", xlabel="Percentage of tuples completed", ymax=35, ymin=3, legend=["TCP", "IB"], y_ticks=np.arange(3, 35, 5),plot=plt)
    plt.subplots_adjust(left=0.06, right=0.98, top=0.9, bottom=0.2)
    fig.tight_layout()
    plt.show()

def plot_inflight():
    y = [[1.42101, 3.60705, 3.813832, 3.933011, 4.023179, 4.095205, 4.15655, 4.209329, 4.2566, 4.301016, 4.342025, 4.37923, 4.412292, 4.44276, 4.473364, 4.503146, 4.532399, 4.559991, 4.587979, 4.613407, 4.638966, 4.662814, 4.686593, 4.709747, 4.733901, 4.756786, 4.779891, 4.802737, 4.823655, 4.845085, 4.865043, 4.885786, 4.905827, 4.927124, 4.946818, 4.966208, 4.985165, 5.003705, 5.024038, 5.043677, 5.062876, 5.082477, 5.101185, 5.119973, 5.138943, 5.15781, 5.1757, 5.193931, 5.212969, 5.231507, 5.250376, 5.26945, 5.288028, 5.307215, 5.326097, 5.345766, 5.364811, 5.386208, 5.407344, 5.426513, 5.447425, 5.468658, 5.48972, 5.51176, 5.533626, 5.554512, 5.575964, 5.600311, 5.623405, 5.646698, 5.669447, 5.692433, 5.717984, 5.742075, 5.767634, 5.792966, 5.819404, 5.847649, 5.875698, 5.904232, 5.93473, 5.96635, 5.999086, 6.033166, 6.068303, 6.106348, 6.146038, 6.186266, 6.229559, 6.276005, 6.328205, 6.386348, 6.449637, 6.518388, 6.595407, 6.690602, 6.806691, 6.953255, 7.183104, 7.600129, 54.972619],
         [11.976518, 29.466279, 30.49349, 31.109085, 31.533489, 31.874956, 32.171226, 32.456275, 32.6907, 32.916648, 33.118456, 33.302603, 33.472665, 33.633971, 33.782356, 33.934413, 34.076852, 34.211436, 34.338917, 34.475597, 34.602774, 34.718712, 34.834569, 34.951833, 35.069556, 35.185393, 35.298518, 35.410891, 35.517523, 35.621287, 35.722793, 35.824183, 35.924959, 36.025787, 36.120164, 36.215372, 36.311719, 36.404702, 36.505337, 36.60156, 36.699315, 36.798821, 36.896604, 36.994028, 37.091663, 37.192291, 37.288684, 37.39041, 37.490724, 37.585785, 37.68475, 37.78826, 37.891769, 37.98533, 38.081683, 38.176749, 38.278689, 38.382079, 38.48452, 38.590694, 38.694635, 38.797786, 38.901793, 39.0042, 39.103973, 39.213535, 39.323803, 39.433503, 39.545538, 39.666622, 39.793974, 39.919287, 40.046157, 40.176584, 40.310414, 40.439685, 40.569528, 40.702407, 40.844166, 40.993165, 41.144577, 41.306924, 41.468885, 41.627109, 41.794284, 41.962257, 42.152335, 42.35847, 42.569367, 42.816585, 43.089992, 43.352146, 43.641445, 43.977966, 44.356965, 44.794428, 45.359317, 46.050456, 47.020314, 48.775548, 116.765934],
         [3.134631, 5.915425, 6.410083, 6.707619, 6.934826, 7.122226, 7.265602, 7.391323, 7.508649, 7.613073, 7.7086, 7.799823, 7.883417, 7.958909, 8.032628, 8.09694, 8.162044, 8.224155, 8.282828, 8.338101, 8.395541, 8.452571, 8.505496, 8.557782, 8.608309, 8.660984, 8.710606, 8.756745, 8.803388, 8.849271, 8.895774, 8.941261, 8.987582, 9.033737, 9.076778, 9.117837, 9.158835, 9.199528, 9.242739, 9.283156, 9.32325, 9.364882, 9.405904, 9.446883, 9.486836, 9.527139, 9.566083, 9.6067, 9.645527, 9.685113, 9.726688, 9.766846, 9.807648, 9.847022, 9.885054, 9.925453, 9.966326, 10.007049, 10.046186, 10.084595, 10.125674, 10.167157, 10.209679, 10.251607, 10.295066, 10.340051, 10.382469, 10.427272, 10.472653, 10.519181, 10.565548, 10.614288, 10.663682, 10.713675, 10.766313, 10.817401, 10.871647, 10.930874, 10.990647, 11.049431, 11.108467, 11.173446, 11.244167, 11.315316, 11.387415, 11.4657, 11.548366, 11.634323, 11.731646, 11.834974, 11.945507, 12.072061, 12.208974, 12.369375, 12.554796, 12.787809, 13.080593, 13.4797, 14.159219, 15.420684, 61.139004],
         [9.281035, 42.544482, 46.645591, 48.519592, 49.856988, 50.826242, 51.627169, 52.276195, 52.846869, 53.378431, 53.839345, 54.3055, 54.702785, 55.069547, 55.423703, 55.769167, 56.088709, 56.384136, 56.67929, 56.958726, 57.222392, 57.486047, 57.741963, 57.981937, 58.228324, 58.460525, 58.680105, 58.892042, 59.101097, 59.322651, 59.538175, 59.748115, 59.949714, 60.146533, 60.352932, 60.538655, 60.719714, 60.903826, 61.075162, 61.251679, 61.435267, 61.607536, 61.782616, 61.953901, 62.120784, 62.30078, 62.477107, 62.647067, 62.81718, 62.986416, 63.148689, 63.3161, 63.473625, 63.639601, 63.804122, 63.974262, 64.12974, 64.296367, 64.459862, 64.623329, 64.788922, 64.965316, 65.135665, 65.308395, 65.47622, 65.651501, 65.817609, 65.988506, 66.155798, 66.327087, 66.507314, 66.688482, 66.869148, 67.054083, 67.251086, 67.451987, 67.657047, 67.852045, 68.046262, 68.250517, 68.455237, 68.66863, 68.900649, 69.140919, 69.383308, 69.638484, 69.902481, 70.173667, 70.438342, 70.743959, 71.067971, 71.418696, 71.786213, 72.196597, 72.643109, 73.140155, 73.729139, 74.5043, 75.480061, 77.065279, 144.001862]]
    x = np.arange(0,101)
    fig = plt.figure(figsize=(4, 2.5), dpi=100)
    plt.subplots_adjust(left=0.15, right=0.98, top=0.9, bottom=0.15)
    plot_line(y, x, mrks=False, ylabel="Time (ms)", xlabel="Percentage of tuples completed", ymax=150, ymin=0, legend=["IB-10", "IB-100", "TCP-10", "TCP-100"], y_ticks=np.arange(0, 150, 20), plot=plt)
    fig.tight_layout()
    plt.show()

def plot_throughput():
    large = [[122176,	60608,	30048,	14896,	7408,	3408],
    [172048,	127200,	82208,	43008,	22128,	7664],
    [420208,	326080,	188016,	78880,	34144,	13664]]

    small = [[2227536,	2201600,	2156768,	2068432,	1894352,	1566192],
             [2362592,	2324928,	2291040,	2188176,	2053456,	1844528],
             [3205568,	3188000,	3200704,	3070656,	2832448,	2809568]]

    small_parallel = [[1261888,	2068432,	1927104],
                      [1346432,	2188176,	1979488],
                      [1798960,	3070656,	3175296]]
    large_parallel = [[7760,	14896,	13920],
                       [29624,	43008,	41984],
                       [42304,	78880,	96960]]

    fig = plt.figure(figsize=(18, 4), dpi=100)

    plt.subplot2grid((1,35), (0, 0), colspan=8)
    plot_line(large, x=x_small, legend=["TCP", "IPoIB", "IB"], title="a) Top. B Large Messages", plot=plt, ticks=xlabels_large, ylabel="Messages per Sec (log)", logy=True)
    # plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

    plt.subplot2grid((1,35), (0, 9), colspan=8)
    plot_line(small, x=x_small, xlabel="Message size bytes", legend=["TCP", "IPoIB", "IB"], title="b) Top. B Small Messages", plot=plt, ticks=xlabels_small, ylabel="Messages per Sec")
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

    plt.subplot2grid((1,35), (0, 18), colspan=8)
    plot_bar(large_parallel, x=[8,16,32], xlabel="Parallelism", legend=["TCP", "IPoIB", "IB"], title="c) Top. B Large Messages", plot=plt,ylabel="Messages per Sec")
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

    plt.subplot2grid((1,35), (0, 27), colspan=8)
    plot_bar(small_parallel, x=[8,16,32], xlabel="Parallelism", legend=["TCP", "IPoIB", "IB"], title="d) Top. B Small Messages", plot=plt, ylabel="Messages per Sec")
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    plt.subplots_adjust(left=0.06, right=0.98, top=0.9, bottom=0.2)
    fig.tight_layout()
    plt.show()

def plot_omni():
    long = [[29.5,	36.01,	50.7,	74.2,	120.1,	297],
            [27.3,	32.4,	41.8,	57.8,	89.1,	248.1],
            [16.2,	17.7,	22.5,	31.2,	46.9,	137.1]]

    long_parallel = [[74,	116,	213],
                    [57.8,	96.4,	184],
                    [31.2,	51.1,	100.3]]
    long_parallel_std = [[8.1,	22.5,	65.7],
                         [6.2,	14.4,	38],
                         [4.4,	11.1,	22.4]
                         ]

    long_short = [[40,	40.58,	39.25,	40.7,	41.22,	42.5],
                  [38.3,	38.58,	39.1,	39.4,	40.6,	40.6],
                  [25.5,	25.9,	25.6,	25.4,	25.8,	26.5]]

    long_short_parallel = [[40.7,	79.6,	151],
                           [39.4,	79.5,	151],
                           [26.4,	49.8,	98.12]]
    long_short_parallel_std = [[5.5,	13.5,	25],
                               [5.1,	11.8,	25.2],
                               [5.1,	11.5,	18.4]]

    fig = plt.figure(figsize=(18, 4), dpi=100)

    plt.subplot2grid((1,35), (0, 0), colspan=8)
    plot_line(long, x=x_small, legend=["TCP", "IPoFabric", "Omni-path"], title="a) Top. A Large Messages", plot=plt, ticks=xlabels_large, ylabel="Latency (ms) Log", logy=True)

    plt.subplot2grid((1,35), (0, 9), colspan=8)
    plot_line(long_short, x=x_small, legend=["TCP", "IPoFabric", "Omni-path"], title="c) Top. A Small Messages", plot=plt, ticks=xlabels_large, logy=False, ymin=20, ymax=55)

    plt.subplot2grid((1,35), (0, 18), colspan=8)
    plot_bar(long_parallel, x=[2,4,8], xlabel="Parallelism", legend=["TCP", "IPoFabric", "Omni-path"], title="b) Top. A Large Messages", plot=plt, ylabel="Latency (ms)", y_std=long_parallel_std)
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

    plt.subplot2grid((1,35), (0, 27), colspan=8)
    plot_bar(long_short_parallel, x=[2,4,8], xlabel="Parallelism", legend=["TCP", "IPoFabric", "Omni-path"], title="d) Top. A Short Messages", plot=plt, y_std=long_short_parallel_std)
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    plt.subplots_adjust(left=0.06, right=0.98, top=0.9, bottom=0.2)
    fig.tight_layout()
    plt.show()

def plot_omni2():
    long = [[29.5,	36.01,	50.7,	74.2,	120.1,	297],
            [27.3,	32.4,	41.8,	57.8,	89.1,	248.1],
            [16.2,	17.7,	22.5,	31.2,	46.9,	137.1]]

    long_parallel = [[74,	116,	213],
                     [57.8,	96.4,	184],
                     [31.2,	51.1,	100.3]]
    long_parallel_std = [[8.1,	22.5,	65.7],
                         [6.2,	14.4,	38],
                         [4.4,	11.1,	22.4]
                         ]

    long_short = [[40,	40.58,	39.25,	40.7,	41.22,	42.5],
                  [38.3,	38.58,	39.1,	39.4,	40.6,	40.6],
                  [25.5,	25.9,	25.6,	25.4,	25.8,	26.5]]

    long_short_parallel = [[40.7,	79.6,	151],
                           [39.4,	79.5,	151],
                           [26.4,	49.8,	98.12]]
    long_short_parallel_std = [[5.5,	13.5,	25],
                               [5.1,	11.8,	25.2],
                               [5.1,	11.5,	18.4]]

    fig = plt.figure(figsize=(18, 4), dpi=100)

    plt.subplot2grid((1,35), (0, 0), colspan=8)
    plot_line(long, x=x_small, legend=["TCP", "IPoFabric", "Omni-path"], title="a) Top. A Large Messages", plot=plt, ticks=xlabels_large, ylabel="Latency (ms) Log", logy=True)

    plt.subplot2grid((1,35), (0, 9), colspan=8)
    plot_line(long_short, x=x_small, legend=["TCP", "IPoFabric", "Omni-path"], title="c) Top. A Small Messages", plot=plt, ticks=xlabels_large, logy=False, ymin=20, ymax=55)

    plt.subplot2grid((1,35), (0, 18), colspan=8)
    plot_bar(long_parallel, x=[2,4,8], xlabel="Parallelism", legend=["TCP", "IPoFabric", "Omni-path"], title="b) Top. A Large Messages", plot=plt, ylabel="Latency (ms)", y_std=long_parallel_std)
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

    plt.subplot2grid((1,35), (0, 27), colspan=8)
    plot_bar(long_short_parallel, x=[2,4,8], xlabel="Parallelism", legend=["TCP", "IPoFabric", "Omni-path"], title="d) Top. A Short Messages", plot=plt, y_std=long_short_parallel_std)
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    plt.subplots_adjust(left=0.06, right=0.98, top=0.9, bottom=0.2)
    fig.tight_layout()
    plt.show()

def proto_buf():
    y_large = [[664, 1246, 2744, 4614, 8136, 15343], [1193, 1492, 2005, 3633, 7084, 22624], [2597, 4954, 9893, 20034, 43722, 92636]]
    y_small = [[1048, 1152, 1133, 1255, 1234, 1300], [2881,2848,2901,2915,2956,2959]]

    fig = plt.figure(figsize=(18, 4), dpi=100)


def main():
    # plot_latency_ib()
    # plot_latency_parallel_ib()
    # plot_yahoo_percentages()
    # plot_inflight()
    # plot_throughput()
    plot_omni()

if __name__ == "__main__":
    main()