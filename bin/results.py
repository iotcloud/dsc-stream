import matplotlib.pyplot as plt
import numpy as np

main_folder="/home/supun/data/collectives"

data = [1000, 2000, 4000, 8000, 16000, 32000]
xlabels = [1, 2, 4, 8, 16, 32]
markers=["o", "x", "^", "v", "D", "*"]
cls=["red", "blue", "green", "yellow", "black", "cyan", "magenta"]

def plot(y=None, x=None, xlabel=None, ylabel=None, title=None, col=None, legend=None, plot=None) :
    if not plot:
        p = plt
    else:
        p = plot
    if not col:
        col = cls
    for i in range(len(y)):
        p.plot(x, y[i], color=col[i], marker=markers[i])

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
    p.legend(legend, loc="upper left")
    p.minorticks_on()
    p.grid(b=True, which='major', color='k', linestyle='-')
    p.grid(b=True, which='minor', color='g', linestyle='-', alpha=0.2)
    if not plot:
        p.show()
    return plt

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

def plot_allreduce():
    folder_bc_128_2=main_folder + "/arc/128_16_2x2_2x2"
    folder_bc_256_2=main_folder + "/arc/256_16_2x2_2x2"
    folder_bc_64_2=main_folder + "/arc/64_16_2x2_2x2"

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

    serial_values[0][5] = None
    binary_speed[0][5] = None

    plt.subplot2grid((1,11), (0, 0), colspan=5)
    plot(serial_values, xlabels, legend=["serial-256", "serial-128","serial-64"], title="Serial", plot=plt)
    plt.subplot2grid((1,11), (0, 6), colspan=5)
    plot(binary_speed, xlabels, legend=["binary-256", "binary-128","binary-64"], title="Binary-Tree", plot=plt, ylabel="Serial time / Binary tree time")

    plt.show()

def plot_reduce():
    folder_bc_128=main_folder + "/rc/128_16_64x64_2x2"
    folder_bc_256=main_folder + "/rc/256_16_64x64_2x2"
    folder_bc_64=main_folder + "/rc/64_16_64x64_2x2"

    folder_bc_128_2=main_folder + "/rc/128_16_2x2_2x2"
    folder_bc_256_2=main_folder + "/rc/256_16_2x2_2x2"
    folder_bc_64_2=main_folder + "/rc/64_16_2x2_2x2"

    serial_128=main_folder + "/rs/128_16_2x2_2x2"
    serial_64=main_folder + "/rs/64_16_2x2_2x2"
    serial_256=main_folder + "/rs/256_16_2x2_2x2"

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

    serial_values[0][5] = None
    flat_speed[0][5] = None
    binary_speed[0][5] = None

    plt.subplot2grid((1,11), (0, 0), colspan=3)
    plot(serial_values, xlabels, legend=["serial-256", "serial-128","serial-64"], title="Serial", plot=plt)
    plt.subplot2grid((1,11), (0, 4), colspan=3)
    plot(binary_speed, xlabels, legend=["binary-256", "binary-128","binary-64"], title="Binary-Tree", plot=plt)
    plt.subplot2grid((1,11), (0, 8), colspan=3)
    plot(flat_speed, xlabels, legend=["flat-256", "flat-128","flat-64"], title="Flat-Tree", plot=plt)

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
    plt.subplot2grid((1,11), (0, 0), colspan=3)
    plot(serial_values, xlabels, legend=["serial-256", "serial-128","serial-64"], title="Serial", plot=plt)
    plt.subplot2grid((1,11), (0, 4), colspan=3)
    plot(binary_speed, xlabels, legend=["binary-256", "binary-128","binary-64"], title="Binary-Tree", plot=plt, ylabel="Serial time / Binary Tree time")
    plt.subplot2grid((1,11), (0, 8), colspan=3)
    plot(flat_speed, xlabels, legend=["flat-256", "flat-128","flat-64"], title="Flat-Tree", plot=plt)

    plt.show()


def get_avgs(folder):
    avgs = []
    for d in data:
        file_name = folder + "/" + str(d)
        a = average(file_name)
        avgs.append(a)
    return avgs


def main():
    plot_bcast()
    plot_reduce()
    plot_allreduce()

def calc(folder, data, tasks):
    for d in data:
        s = str(d)
        for t in tasks:
            s = s + " " + str(average(folder + str(d), 1000000000))
        print s
    print "\n"

if __name__ == "__main__":
    main()