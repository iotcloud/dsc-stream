folder="/home/supun/dev/projects/stream/dsc-stream2/scripts/performance/results/tcp8/"
folder="/home/supun/dev/projects/stream/dsc-stream2/scripts/performance/latency/rdma_test/"
folder="/home/supun/dev/projects/stream/dsc-stream2/scripts/performance/latency/tcp_test/"

def average(file_name, upperbound):
    with open(file_name, "r") as in_f:
        numbers = []
        for line in in_f:
            line = line.strip() # remove whitespace
            if line: # make sure there is something there
                n = line.split(',')
                number_on_line = long(n[0].strip())
                if number_on_line < upperbound:
                    numbers.append(number_on_line)

        avg_of_numbers = 0
        if len(numbers) > 0:
            sum_of_numbers = sum(numbers)
            # avg_of_numbers = sum(numbers[1000:(len(numbers) - 100)])/(len(numbers) - 1100)
            avg_of_numbers = sum(numbers[1000:3900])/(3000)
            # print min(numbers[:(len(numbers) - 100)])
    return (avg_of_numbers + 0.0) / 1000000

def main():
    tasks = [10]
    data = [1000, 2000, 4000, 8000, 16000, 32000]
    calc("/home/supun/dev/projects/stream/dsc-stream2/bin/col/16_128_col/", data, tasks)
    data = [1000, 2000, 4000, 8000]
    calc("/home/supun/dev/projects/stream/dsc-stream2/bin/col/16_128_reduce/", data, tasks)
    data = [16000, 32000]
    calc("/home/supun/dev/projects/stream/dsc-stream2/bin/col/16_128_reduce2/", data, tasks)


def calc(folder, data, tasks):
    for d in data:
        s = str(d)
        for t in tasks:
            s = s + " " + str(average(folder + str(d), 1000000000))
        print s
    print "\n"

if __name__ == "__main__":
    main()