import csv
import collections

file1 = "latency_write_1r_3w_3n_5fuzz.csv"
file2 = "latency_write_1r_3w_3n_10fuzz.csv"
file3 = "latency_write_1r_3w_3n_20fuzz.csv"
file4 = "latency_write_1r_3w_3n_40fuzz.csv"
file5 = "latency_write_1r_3w_3n_60fuzz.csv"

result_file = "write_latency_1r_3w_3n.csv"
machineMap = collections.defaultdict(list)

def processFile(file):
    obj = open(file,"r")
    lines = obj.readlines()
    totalSum = 0
    totalCount = 0
    fuzzer = file.split("_")[len(file.split("_"))-1]
    line_count = 0
    for line in lines:
        if len(line) == 1: continue
        if(line_count == 0):
            line_count += 1
            continue
        # print("line: ",len(line))
        machineId = line.split("]")[0][1:]
        time = line.split(",")[1].strip()
        totalSum += int(time)
        totalCount += 1
        machineMap[machineId].append(time)
    obj.close()
    print(fuzzer.split(".")[0]+","+str(totalSum/totalCount))

def writetoCSV():
    file = open(result_file,'w')
    csv_writer = csv.writer(file)
    for key in machineMap:
        row = key+","+",".join(machineMap[key])
        csv_writer.writerow([row])
    file.close()

def main():
    processFile(file1)
    processFile(file2)
    processFile(file3)
    processFile(file4)
    processFile(file5)

    invalidKeys = []
    for key in machineMap:
        if len(machineMap[key]) != 5:
            invalidKeys.append(key)
    for key in invalidKeys:
        del machineMap[key]
    
    # for key in machineMap:
    #     print(machineMap[key])
    writetoCSV()


if __name__ == "__main__":
    main()