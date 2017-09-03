import os
import numpy as np

bins = 100


def go_through_file(indir, prefix):
    ret = {}
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

                    key, new_vals = calculate_bins(results_file, messages, pattern, size)

                    vals = []
                    if key in ret:
                        vals = ret[key]
                    else:
                        vals = [0,0,0,0]
                    vals[0] = vals[0] + new_vals[0]
                    vals[1] = vals[1] + new_vals[1]
                    vals[2] = vals[2] + new_vals[2]
                    vals[3] = vals[3] + 1
                    ret[key] = vals

    return ret

def calculate_bins(file_name, messages, pattern, message_size):
    upperbound = messages / 4
    count = 0
    ret = {}
    with open(file_name, "r") as in_f:
        numbers = []
        for line in in_f:
            count += 1
            if count < upperbound:
                continue
            line = line.strip()  # remove whitespace
            if line:  # make sure there is something there
                n = line.split(',')
                number_on_line = long(n[0].strip())
                numbers.append(number_on_line)

        numbers = numbers[0:len(numbers) - upperbound / 2]
        numbers.sort()
        numbers = numbers[0:len(numbers) - 2]
        size = len(numbers)
        bucket_size = size / bins
        final_numbers = []
        avg_of_numbers = ((sum(numbers) + 0.0) / size) / 1000000
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
        # print pattern, ",", message_size, ",", avg_of_numbers, ",", std_of_numbers, ",", final_numbers[
        #     len(final_numbers) - 2]
        # print final_numbers
        key = pattern + ", " + str(message_size)

        vals = [0,0,0,0]
        vals[0] = vals[0] + avg_of_numbers
        vals[1] = vals[1] + std_of_numbers
        vals[2] = vals[2] + final_numbers[len(final_numbers) - 2]
        vals[3] = 0
        ret[key] = vals

    return key, vals
        # print (avg_of_numbers + 0.0) / 1000000

def print_common(ret):
    print "The common values: "
    for key, val in ret.items():
        print key, ",", val[0] / val[3], ",", val[1] / val[3], ",", val[2] / val[3]

def old_result():
    print "VERBS"
    ret = go_through_file('/home/supun/data/long_topology_knl/verbs', 'largethroughputSpout')
    # ret = go_through_file('/home/supun/data/long_topology/verbs', 'largethroughputSpout')
    # ret = go_through_file('/home/supun/data/juliet/verbs', 'largethroughputSpout')
    print_common(ret)
    # go_through_file('/home/supun/data/KNL/verbs', 'largethroughputSpout')
    print "TCP"
    ret = go_through_file('/home/supun/data/long_topology_knl/tcp', 'largethroughputSpout')
    # ret = go_through_file('/home/supun/data/long_topology/tcp', 'largethroughputSpout')
    # ret = go_through_file('/home/supun/data/juliet/tcp', 'largethroughputSpout')
    print_common(ret)
    # go_through_file('/home/supun/data/KNL/tcp', 'largethroughputSpout')
    print "IPIB"
    ret = go_through_file('/home/supun/data/long_topology_knl/ipib', 'largethroughputSpout')
    # ret = go_through_file('/home/supun/data/long_topology/ipib', 'largethroughputSpout')
    # ret = go_through_file('/home/supun/data/juliet/ipib', 'largethroughputSpout')
    print_common(ret)
    # go_through_file('/home/supun/data/KNL/ipib', 'largethroughputSpout')

def main():
    print "VERBS"
    # ret = go_through_file('/home/supun/data/ib/ib_long_small', 'smallthroughputSpout')
    ret = go_through_file('/home/supun/data/ib/ib_short_small', 'smallthroughputSpout')
    # ret = go_through_file('/home/supun/data/long_topology/verbs', 'largethroughputSpout')
    # ret = go_through_file('/home/supun/data/juliet/verbs', 'largethroughputSpout')
    print_common(ret)
    # go_through_file('/home/supun/data/KNL/verbs', 'largethroughputSpout')
    print "TCP"
    # ret = go_through_file('/home/supun/data/ib/tcp_long_small', 'smallthroughputSpout')
    ret = go_through_file('/home/supun/data/ib/tcp_short_small', 'smallthroughputSpout')
    # ret = go_through_file('/home/supun/data/long_topology/tcp', 'largethroughputSpout')
    # ret = go_through_file('/home/supun/data/juliet/tcp', 'largethroughputSpout')
    print_common(ret)
    # go_through_file('/home/supun/data/KNL/tcp', 'largethroughputSpout')
    print "IPIB"
    # ret = go_through_file('/home/supun/data/ib/ipib_long_small', 'smallthroughputSpout')
    ret = go_through_file('/home/supun/data/ib/ipib_short_small', 'smallthroughputSpout')
    # ret = go_through_file('/home/supun/data/long_topology/ipib', 'largethroughputSpout')
    # ret = go_through_file('/home/supun/data/juliet/ipib', 'largethroughputSpout')
    print_common(ret)
    # go_through_file('/home/supun/data/KNL/ipib', 'largethroughputSpout')


if __name__ == "__main__":
    main()
