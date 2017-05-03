
bins=100
results_file_verbs="/home/supun/data/juliet/verbs/benchmark_100/latency_7_1000000_100000"
results_file_ipib="/home/supun/data/juliet/ipib/benchmark_100/latency_7_1000000_100000"
results_file_tcp="/home/supun/data/juliet/tcp/benchmark_100/latency_7_1000000_100000"
#
results_file_verbs="/home/supun/data/juliet/verbs/benchmark/latency_7_1000000_100000"
results_file_ipib="/home/supun/data/juliet/ipib/benchmark/latency_7_1000000_100000"
results_file_tcp="/home/supun/data/juliet/tcp/benchmark/latency_7_1000000_100000"

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
                # if (number_on_line > 250000):
                numbers.append(number_on_line)
        numbers = numbers[0:len(numbers) - upperbound / 2]
        # for n in numbers:
        #     print n

        numbers.sort()

        size = len(numbers)
        bucket_size = size / bins
        final_numbers = []
        avg_of_numbers = (sum(numbers) + 0.0)/size
        print "size " + str(size) + " bucket size: " + str(bucket_size)
        for i in range(0, bins):
            index = i * bucket_size
            if (index >= size):
                index = size - 1
            index___ = (numbers[index] + 0.0) / 1000000
            final_numbers.append(index___)
        final_numbers.append((numbers[size - 1] + 0.0) / 1000000)
        print final_numbers
        print (avg_of_numbers + 0.0) / 1000000

def main():
    calculate_bins(results_file_verbs, 50000)
    calculate_bins(results_file_ipib, 50000)
    calculate_bins(results_file_tcp, 50000)

def calc(folder, data, tasks):
    for d in data:
        s = str(d)
        for t in tasks:
            s = s + " " + str(average(folder + str(d), 1000000000))
        print s
    print "\n"

if __name__ == "__main__":
    main()