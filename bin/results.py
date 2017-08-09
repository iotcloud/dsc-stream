import matplotlib.pyplot as plt
import numpy as np
import os

main_folder="/home/supun/experiments/collectives2"
thrput_folder="/home/supun/experiments/collectives3"

data = [1000, 2000, 4000, 8000, 16000, 32000]
xlabels = [1, 2, 4, 8, 16, 32]
markers=["o", "x", "^", "v", "D", "*"]
cls=["red", "blue", "green", "yellow", "black", "cyan", "magenta"]

def plot(y=None, x=None, xlabel=None, ylabel=None, title=None, col=None, legend=None, plot=None, logy=False, ylim=None, legendloc=None) :
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
    if legendloc:
        p.legend(legend, loc=legendloc)
    else:
        p.legend(legend, loc="upper left")
    p.minorticks_on()
    p.grid(b=True, which='major', color='k', linestyle='-')
    p.grid(b=True, which='minor', color='grey', linestyle='-', alpha=0.2)
    if not plot:
        p.show()
    return plt

def go_through_file(indir, prefix):
    ret = {}
    for root, dirs, filenames in os.walk(indir):
        for f in filenames:
            if f.startswith(prefix):
                results_file = os.path.join(root, f)
                with open(results_file, "r") as in_f:
                    for line in in_f:
                        splits = line.split(" ")
                        if len(splits) > 2:
                            size = int(splits[2])
                            # print size
                            val = float(splits[5])
                            # print messages
                            ret[size] = val
    print indir, prefix, ret
    return ret

def average(file_name):
    with open(file_name, "r") as in_f:
        numbers = []
        for line in in_f:
            line = line.strip() # remove whitespace
            if line: # make sure there is something there
                n = line.split(',')
                number_on_line = long(n[0].strip())
                numbers.append(number_on_line)

        avg_of_numbers = 0

        clip = len(numbers) / 8
        if len(numbers) > 0:
            avg_of_numbers = sum(numbers[clip:(len(numbers) - clip)]) / (len(numbers) * .75)
    return (avg_of_numbers + 0.0) / 1000000

def plot_tallreduce(folder):
    tarc_folder = folder + "/tarc"
    tars_folder = folder + "/tars"
    tarc_vals_128 = go_through_file(tarc_folder, "128")
    tarc_vals_256 = go_through_file(tarc_folder, "256")
    tarc_vals_64 = go_through_file(tarc_folder, "64")
    tars_vals_128 = go_through_file(tars_folder, "128")
    tars_vals_256 = go_through_file(tars_folder, "256")
    tars_vals_64 = go_through_file(tars_folder, "64")

    serial_values = []
    serial_values.append([tars_vals_256[k] for k in data])
    serial_values.append([tars_vals_128[k] for k in data])
    serial_values.append([tars_vals_64[k] for k in data])

    binary_values = []
    binary_values.append([m/b for b,m in zip(serial_values[0], [tarc_vals_256[k] for k in data])])
    binary_values.append([m/b for b,m in zip(serial_values[0], [tarc_vals_128[k] for k in data])])
    binary_values.append([m/b  for b,m in zip(serial_values[0], [tarc_vals_64[k] for k in data])])

    plt.subplot2grid((1,13), (0, 0), colspan=6)
    plot(serial_values, xlabels, legend=["serial-256", "serial-128","serial-64"], title="Serial", plot=plt)
    plt.subplot2grid((1,13), (0, 7), colspan=6)
    plot(binary_values, xlabels, legend=["binary-256", "binary-128","binary-64"], title="Binary-Tree", plot=plt, ylabel="Serial throughput / Binary throughput")

    plt.show()

def plot_allreduce():
    folder_bc_128_2=main_folder + "/arc2/128_16_2x2_2x2"
    folder_bc_256_2=main_folder + "/arc2/256_16_2x2_2x2"
    folder_bc_64_2=main_folder + "/arc2/64_16_2x2_2x2"

    serial_128=main_folder + "/ars/128_16_2x2_2x64"
    serial_64=main_folder + "/ars/64_16_2x2_2x64"
    serial_256=main_folder + "/ars/256_16_2x2_2x64"

    binary_values = []
    avgs = get_avgs(folder_bc_256_2)
    binary_values.append(avgs)
    avgs = get_avgs(folder_bc_128_2)
    binary_values.append(avgs)
    avgs = get_avgs(folder_bc_64_2)
    binary_values.append(avgs)

    serial_values = []
    avgs = get_avgs(serial_256)
    serial_values.append(avgs)
    avgs = get_avgs(serial_128)
    serial_values.append(avgs)
    avgs = get_avgs(serial_64)
    serial_values.append(avgs)

    binary_speed = []
    binary_speed.append([b / m for b,m in zip(serial_values[0], binary_values[0])])
    binary_speed.append([b / m for b,m in zip(serial_values[1], binary_values[1])])
    binary_speed.append([b / m for b,m in zip(serial_values[2], binary_values[2])])

    # serial_values[0][5] = None
    # binary_speed[0][5] = None

    plt.subplot2grid((1,13), (0, 0), colspan=6)
    plot(serial_values, xlabels, legend=["serial-256", "serial-128","serial-64"], title="Serial", plot=plt)
    plt.subplot2grid((1,13), (0, 7), colspan=6)
    plot(binary_speed, xlabels, legend=["binary-256", "binary-128","binary-64"], title="Binary-Tree", plot=plt, ylabel="Serial time / Binary tree time")

    plt.show()

def plot_treduce(folder):
    tarc_folder = folder + "/trc"
    tars_folder = folder + "/trs"
    tarc_vals_128 = go_through_file(tarc_folder, "128")
    tarc_vals_256 = go_through_file(tarc_folder, "256")
    tarc_vals_64 = go_through_file(tarc_folder, "64")
    tars_vals_128 = go_through_file(tars_folder, "128")
    tars_vals_256 = go_through_file(tars_folder, "256")
    tars_vals_64 = go_through_file(tars_folder, "64")

    serial_values = []
    serial_values.append([tars_vals_256[k] for k in data])
    serial_values.append([tars_vals_128[k] for k in data])
    serial_values.append([tars_vals_64[k] for k in data])

    binary_values = []
    binary_values.append([m/b for b,m in zip(serial_values[0], [tarc_vals_256[k] for k in data])])
    binary_values.append([m/b for b,m in zip(serial_values[0], [tarc_vals_128[k] for k in data])])
    binary_values.append([m/b  for b,m in zip(serial_values[0], [tarc_vals_64[k] for k in data])])

    plt.subplot2grid((1,13), (0, 0), colspan=6)
    plot(serial_values, xlabels, legend=["serial-256", "serial-128","serial-64"], title="Serial", plot=plt)
    plt.subplot2grid((1,13), (0, 7), colspan=6)
    plot(binary_values, xlabels, legend=["binary-256", "binary-128","binary-64"], title="Binary-Tree", plot=plt, ylabel="Serial throughput / Binary throughput")

    plt.show()

def plot_reduce():
    folder_bc_128=main_folder + "/rc/128_16_64x64_2x2"
    folder_bc_256=main_folder + "/rc/256_16_64x64_2x2"
    folder_bc_64=main_folder + "/rc/64_16_64x64_2x2"

    folder_bc_128_2=main_folder + "/rc/128_16_2x2_2x2"
    folder_bc_256_2=main_folder + "/rc/256_16_2x2_2x2"
    folder_bc_64_2=main_folder + "/rc/64_16_2x2_2x2"

    serial_128=main_folder + "/rs2/128_16_2x2_2x2"
    serial_64=main_folder + "/rs2/64_16_2x2_2x2"
    serial_256=main_folder + "/rs2/256_16_2x2_2x2"

    flat_values = []
    avgs = get_avgs(folder_bc_256)
    flat_values.append(avgs)
    avgs = get_avgs(folder_bc_128)
    flat_values.append(avgs)
    avgs = get_avgs(folder_bc_64)
    flat_values.append(avgs)

    binary_values = []
    avgs = get_avgs(folder_bc_256_2)
    binary_values.append(avgs)
    avgs = get_avgs(folder_bc_128_2)
    binary_values.append(avgs)
    avgs = get_avgs(folder_bc_64_2)
    binary_values.append(avgs)

    serial_values = []
    avgs = get_avgs(serial_256)
    serial_values.append(avgs)
    avgs = get_avgs(serial_128)
    serial_values.append(avgs)
    avgs = get_avgs(serial_64)
    serial_values.append(avgs)

    binary_speed = []
    binary_speed.append([b / m for b,m in zip(serial_values[0], binary_values[0])])
    binary_speed.append([b / m for b,m in zip(serial_values[1], binary_values[1])])
    binary_speed.append([b / m for b,m in zip(serial_values[2], binary_values[2])])

    flat_speed = []
    flat_speed.append([b / m for b,m in zip(serial_values[0], flat_values[0])])
    flat_speed.append([b / m for b,m in zip(serial_values[1], flat_values[1])])
    flat_speed.append([b / m for b,m in zip(serial_values[2], flat_values[2])])

    # serial_values[0][5] = None
    # flat_speed[0][5] = None
    # binary_speed[0][5] = None

    plt.subplot2grid((1,20), (0, 0), colspan=6)
    plot(serial_values, xlabels, legend=["serial-256", "serial-128","serial-64"], title="Serial", plot=plt, ylabel="Serial time / Binary tree time")
    plt.subplot2grid((1,20), (0, 7), colspan=6)
    plot(binary_speed, xlabels, legend=["binary-256", "binary-128","binary-64"], title="Binary-Tree", plot=plt, ylabel="Serial time / Binary tree time")
    plt.subplot2grid((1,20), (0, 14), colspan=6)
    plot(flat_speed, xlabels, legend=["flat-256", "flat-128","flat-64"], title="Flat-Tree", plot=plt, ylabel="Serial time / Binary tree time")

    plt.show()

    binary_speed = []
    binary_speed.append([m for b,m in zip(serial_values[0], binary_values[0])])
    binary_speed.append([m for b,m in zip(serial_values[1], binary_values[1])])
    binary_speed.append([m for b,m in zip(serial_values[2], binary_values[2])])

    flat_speed = []
    flat_speed.append([m for b,m in zip(serial_values[0], flat_values[0])])
    flat_speed.append([m for b,m in zip(serial_values[1], flat_values[1])])
    flat_speed.append([m for b,m in zip(serial_values[2], flat_values[2])])

    # serial_values[0][5] = None
    # flat_speed[0][5] = None
    # binary_speed[0][5] = None

    plt.subplot2grid((1,20), (0, 0), colspan=6)
    plot(serial_values, xlabels, legend=["serial-256", "serial-128","serial-64"], title="Serial", plot=plt)
    plt.subplot2grid((1,20), (0, 7), colspan=6)
    plot(binary_speed, xlabels, legend=["binary-256", "binary-128","binary-64"], title="Binary-Tree", plot=plt)
    plt.subplot2grid((1,20), (0, 14), colspan=6)
    plot(flat_speed, xlabels, legend=["flat-256", "flat-128","flat-64"], title="Flat-Tree", plot=plt)

    plt.show()

def plot_tbcast(folder):
    tarc_folder = folder + "/tbc"
    tars_folder = folder + "/tbs"
    tarc_vals_128 = go_through_file(tarc_folder, "128")
    tarc_vals_256 = go_through_file(tarc_folder, "256")
    tarc_vals_64 = go_through_file(tarc_folder, "64")
    tars_vals_128 = go_through_file(tars_folder, "128")
    tars_vals_256 = go_through_file(tars_folder, "256")
    tars_vals_64 = go_through_file(tars_folder, "64")

    serial_values = []
    serial_values.append([tars_vals_256[k] for k in data])
    serial_values.append([tars_vals_128[k] for k in data])
    serial_values.append([tars_vals_64[k] for k in data])

    binary_values = []
    binary_values.append([m/b for b,m in zip(serial_values[0], [tarc_vals_256[k] for k in data])])
    binary_values.append([m/b for b,m in zip(serial_values[0], [tarc_vals_128[k] for k in data])])
    binary_values.append([m/b  for b,m in zip(serial_values[0], [tarc_vals_64[k] for k in data])])

    plt.subplot2grid((1,13), (0, 0), colspan=6)
    plot(serial_values, xlabels, legend=["serial-256", "serial-128","serial-64"], title="Serial", plot=plt, legendloc="upper right", logy=True)
    plt.subplot2grid((1,13), (0, 7), colspan=6)
    plot(binary_values, xlabels, legend=["binary-256", "binary-128","binary-64"], title="Binary-Tree", plot=plt, ylabel="Serial throughput / Binary throughput")

    fig = plt.gcf()
    fig.set_size_inches(10.5, 40.5)
    fig.savefig('test2png.png', dpi=100)

    plt.show()


def plot_bcast():
    folder_bc_128=main_folder + "/bc/128_16_2x2_64x64"
    folder_bc_256=main_folder + "/bc/256_16_2x2_64x64"
    folder_bc_64=main_folder + "/bc/64_16_2x2_64x64"

    folder_bc_128_2=main_folder + "/bc/128_16_2x2_2x2"
    folder_bc_256_2=main_folder + "/bc/256_16_2x2_2x2"
    folder_bc_64_2=main_folder + "/bc/64_16_2x2_2x2"

    serial_128=main_folder + "/bs/128_16_2x2_2x64"
    serial_64=main_folder + "/bs/64_16_2x2_2x64"
    serial_256=main_folder + "/bs/256_16_2x2_2x64"

    flat_values = []
    avgs = get_avgs(folder_bc_256)
    flat_values.append(avgs)
    avgs = get_avgs(folder_bc_128)
    flat_values.append(avgs)
    avgs = get_avgs(folder_bc_64)
    flat_values.append(avgs)

    binary_values = []
    avgs = get_avgs(folder_bc_256_2)
    binary_values.append(avgs)
    avgs = get_avgs(folder_bc_128_2)
    binary_values.append(avgs)
    avgs = get_avgs(folder_bc_64_2)
    binary_values.append(avgs)

    serial_values = []
    avgs = get_avgs(serial_256)
    serial_values.append(avgs)
    avgs = get_avgs(serial_128)
    serial_values.append(avgs)
    avgs = get_avgs(serial_64)
    serial_values.append(avgs)

    binary_speed = []
    binary_speed.append([b / m for b,m in zip(serial_values[0], binary_values[0])])
    binary_speed.append([b / m for b,m in zip(serial_values[1], binary_values[1])])
    binary_speed.append([b / m for b,m in zip(serial_values[2], binary_values[2])])

    flat_speed = []
    flat_speed.append([b / m for b,m in zip(serial_values[0], flat_values[0])])
    flat_speed.append([b / m for b,m in zip(serial_values[1], flat_values[1])])
    flat_speed.append([b / m for b,m in zip(serial_values[2], flat_values[2])])

    # plot(values, xlabels, legend=["collective-256", "collective-128","collective-64", "collective-256-2", "collective-128-2","collective-64-2", "serial-256", "serial-128", "serial-64"], title="Reduce")
    plt.subplot2grid((1,20), (0, 0), colspan=6)
    plot(serial_values, xlabels, legend=["serial-256", "serial-128","serial-64"], title="Serial", plot=plt)
    plt.subplot2grid((1,20), (0, 7), colspan=6)
    plot(binary_speed, xlabels, legend=["binary-256", "binary-128","binary-64"], title="Binary-Tree", plot=plt, ylabel="Serial time / Binary Tree time")
    plt.subplot2grid((1,20), (0, 14), colspan=6)
    plot(flat_speed, xlabels, legend=["flat-256", "flat-128","flat-64"], title="Flat-Tree", plot=plt, ylabel="Serial time / Binary tree time")

    plt.show()


def get_avgs(folder):
    avgs = []
    for d in data:
        file_name = folder + "/" + str(d)
        a = average(file_name)
        avgs.append(a)
    return avgs


def main():
    # plot_bcast()
    # plot_reduce()
    # plot_allreduce()
    # plot_tallreduce(thrput_folder)
    # plot_treduce(thrput_folder)
    plot_tbcast(thrput_folder)

def calc(folder, data, tasks):
    for d in data:
        s = str(d)
        for t in tasks:
            s = s + " " + str(average(folder + str(d), 1000000000))
        print s
    print "\n"

if __name__ == "__main__":
    main()