
bins=100
results_file="/home/supun/dev/projects/stream/dsc-stream2/scripts/performance/march15/verbs/bench/latency_4_10000000_100000"
results_file="/home/supun/dev/projects/stream/dsc-stream2/scripts/performance/march15/ipib/bench/latency_2_2_10000000_100000"
#results_file="/home/supun/dev/projects/stream/dsc-stream2/scripts/performance/march15/tcp/bench/latency_4_2_10000000_100000"

def calculate_bins(file_name, upperbound):
    count = 0
    with open(file_name, "r") as in_f:
        numbers = []
        for line in in_f:
            count += 1
            if count < upperbound :
                continue
            line = line.strip() # remove whitespace
            if line: # make sure there is something there
                n = line.split(',')
                number_on_line = long(n[0].strip())
                if (number_on_line > 500000):
                    numbers.append(number_on_line)
        numbers = numbers[0:len(numbers) - upperbound * 2]
        numbers.sort()

        for n in numbers:
            print n

        size = len(numbers)
        bucket_size = size / bins
        final_numbers = []
        print "size " + str(size) + " bucket size: " + str(bucket_size)
        for i in range(0, bins):
            index = i * bucket_size
            if (index >= size):
                index = size - 1
            index___ = (numbers[index] + 0.0) / 1000000
            final_numbers.append(index___)
        final_numbers.append((numbers[size - 1] + 0.0) / 1000000)
        print final_numbers

def main():
    calculate_bins(results_file, 500000)

def calc(folder, data, tasks):
    for d in data:
        s = str(d)
        for t in tasks:
            s = s + " " + str(average(folder + str(d), 1000000000))
        print s
    print "\n"

if __name__ == "__main__":
    main()