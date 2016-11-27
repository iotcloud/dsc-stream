
folder="/home/supun/dev/projects/stream/dsc-stream2/scripts/performance/results/tcp8/"
folder="/home/supun/dev/projects/stream/dsc-stream2/scripts/performance/latency/rdma3/"
folder="/home/supun/dev/projects/stream/dsc-stream2/scripts/performance/latency/tcp5/"

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
            avg_of_numbers = sum(numbers[100:(len(numbers) - 100)])/(len(numbers) - 100)
            # print min(numbers[:(len(numbers) - 100)])
    return avg_of_numbers

def main():
    tasks = [10]
    data = [32000, 64000, 128000, 256000, 512000]
    data = [1000,2000,4000,8000,16000,32000,64000]
    for d in data:
        s = str(d)
        for t in tasks:
            s = s + " " + str(average(folder + str(d), 1000000000))
        print s


if __name__ == "__main__":
    main()