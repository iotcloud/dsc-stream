import os
import numpy as np

bins=100

def go_through_file(indir, prefix):
    for root, dirs, filenames in os.walk(indir):
        for f in filenames:
            if f.startswith(prefix):
                splits = f.rsplit('_')
                if len(splits) > 2:
                    # results_file = open(os.path.join(root, f), 'r')
                    results_file = os.path.join(root, f)
                    size = int(splits[4])
                    # print size
                    messages = int(splits[3])
                    # print messages
                    pattern = splits[2]
                    # print pattern

                    calculate_bins(results_file, messages, pattern, size)



def calculate_bins(file_name, messages, pattern, message_size):
    upperbound = messages / 4
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
                numbers.append(number_on_line)

        numbers = numbers[0:len(numbers) - upperbound / 2]
        numbers.sort()

        size = len(numbers)
        bucket_size = size / bins
        final_numbers = []
        avg_of_numbers = ((sum(numbers) + 0.0)/size) / 1000000
        std_of_numbers = np.std(numbers, dtype=np.float64) / 1000000

        # print "size " + str(size) + " bucket size: " + str(bucket_size)
        for i in range(0, bins):
            index = i * bucket_size
            if (index >= size):
                index = size - 1
            index___ = (numbers[index] + 0.0) / 1000000
            final_numbers.append(index___)
        final_numbers.append((numbers[size - 1] + 0.0) / 1000000)

        # print final_numbers
        print pattern, ",", message_size, ",", avg_of_numbers, ",", std_of_numbers, ",", final_numbers[len(final_numbers) - 2]
        # print (avg_of_numbers + 0.0) / 1000000

def main():
    print "VERBS"
    go_through_file('/home/supun/data/juliet/verbs', 'largethroughputSpout')
    # go_through_file('/home/supun/data/KNL/verbs', 'largethroughputSpout')
    print "TCP"
    go_through_file('/home/supun/data/juliet/tcp', 'largethroughputSpout')
    # go_through_file('/home/supun/data/KNL/tcp', 'largethroughputSpout')
    print "IPIB"
    go_through_file('/home/supun/data/juliet/ipib', 'largethroughputSpout')
    # go_through_file('/home/supun/data/KNL/ipib', 'largethroughputSpout')



if __name__ == "__main__":
    main()