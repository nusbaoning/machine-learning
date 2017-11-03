# -*- coding: utf-8 -*-
"""
Created on Thu Nov  2 13:36:31 2017

@author: Bao Ning
"""
import codecs
import time
import traceback


datafile = 'fileserver_1h.log'
HEADLINENUMBER = 11
MINLENGH = 10
READ_FUNCTION_NAME = "generic_file_read_iter"
timeSep = 600
timeCountMax = 4


'''
main overflow of the program
'''
def load_file(datafile):
    i = 0
    line = ''
    r = 0
    page = {}
    file = {}
    process = {}
    # reflection = {}
    # uniqueInode = 0
    req = 1
    currentTime = -1
    
    record = True    
    timeCount = 0
    try:
        
        # with codecs.open(filename, 'r', encoding='utf-8', errors='ignore') as fin:
        #     for line in fin:

        s = time.time()
        fin = codecs.open(datafile, 'r', encoding='utf-8', errors='ignore')          
        lines = fin.readlines()
        e = time.time()
        print("load file consumed ", (e-s), "s")
        s = e
        lineNum = len(lines)
        for line in lines:
            i=i+1
            # print(i, readreq, writereq)
#            if line is null
            if len(line)<=10:
                continue
#            get head information
            if i==10:
                task_tail_loc = line.find('-')
                print(task_tail_loc)
                cpu_loc = line.find('CPU')
                print(cpu_loc)
                timestamp_loc = cpu_loc + 10
                function_loc = line.find('FUNCTION')
            if i<=HEADLINENUMBER:
                continue           
#           parse the head
            line = line.strip('\n')
            task = line[0:task_tail_loc].strip()
            pid = int(line[task_tail_loc+1:cpu_loc])
            timestamp = float(line[timestamp_loc:function_loc].replace(':',''))
            if currentTime<0:
                currentTime = int(timestamp)
                timeCount += 1
            elif timestamp - currentTime >= timeSep:
                record = not record
                currentTime = int(timestamp)
                # 完成一次记录+标注，需要输出+初始化
                if record:
                    output(page, process, file)
                    page = {}
                    file = {}
                    process = {}
                    req = 1                    
                timeCount += 1
                if timeCount > timeCountMax:
                    break
            # print(task, pid, timestamp)
            line = line[function_loc:]
            colon_loc = line.find(':')
            function_name = line[:colon_loc]
            # print(function_name)
            line = line[colon_loc+1:]
            items = line.split(',')
#           for return sentence
            if len(items)==2:
                continue
            j = 0
            inode = int(items[j])
            j+=1
            isize = None
            if function_name.strip() == READ_FUNCTION_NAME:
                # isize = int(items[j])
                # j+=1
                r = 0
                isize = int(items[j])
                j += 1
            else:
                r = 1
            pos = int(items[j])
            j+=1
            count = int(items[j])
            j+=2
            filename = items[j]            
#            if i<=30:
#                 print("line=", i, "task=", task, "pid=", pid, 
#                       "timestamp=", timestamp, "function_name=", function_name, 
#                       "inode=", inode, "isize=", isize, "pos=", pos, "count=", count, "filename=",filename)
#            else:
#                 break
            
            #映射inode
            # if not inode in reflection:
            #     reflection[inode] = {filename:uniqueInode}                
            #     inode = uniqueInode
            #     uniqueInode += 1
            # elif not filename in reflection[inode]:
            #     reflection[inode][filename] = uniqueInode
            #     inode = uniqueInode
            #     uniqueInode += 1
            # else:
            #     inode = reflection[inode][filename]
            if record:
                if not inode in file.keys():
                    file[inode] = filename
                # fileserver.f中没有重命名指令，filename不同说明文件被删除，那么所有历史数据一并删除，开始记录新的访问请求
                elif filename != file[inode]:
                    file[inode] = filename
                    del page[inode]
                if not inode in page.keys():
                    page[inode] = {}
                index0 = pos >> 12
                if r==0:
                    count = min(count, isize-pos)
                index1 = (pos + count - 1) >> 12
                for index in range(index0, index1+1):
                    access_page(page[inode], index, req, r, pid)
                    access_process(inode, index, pid, r, process)
                    req += 1
            else:
#               原来的文件被删除或者出现了新page的访问，不用记录
                if not inode in file.keys():
                    continue
                if not index in page[inode]:
                    continue
                tag_access_page(page[inode], index, r)

            if i%10000==0:
                e = time.time()
                print(i, int(100*i/lineNum), "%", "consumed", (e-s), "s")
                s = e
    except Exception as e:
        print(i, line)
        print(len(line))
        print (repr(e))
        print (traceback.print_exc())
    finally:
        
        
        fin.close()        
#         fout = open(datafile+".metadata", "w")
#         print(process, file=fout)
# #        print(reflection, file=fout)
#         fout.close()
        
def access_page(inodeDict, index, req, rw, pid):
    if index not in inodeDict:
        inodeDict[index] = (req, [], 1-rw, rw, [pid])
    else:
        (lastreq, reUseList, lR, lW, pidList) = inodeDict[index]
        reUseList.append(req-lastreq)
        if pid not in pidList:
            pidList.append(pid)
        inodeDict[index] = (req, reUseList, lR+1-rw, lW+rw, pidList)
    
def access_process(inode, index, pid, rw, process):
    key = inode
    if pid not in process:
        process[pid] = ([key], 1-rw, rw)
    else:
        (keyList, r, w) = process[pid]
        if key not in keyList:
            keyList.append(key)
        process[pid] = (keyList, r+1-rw, w+rw)

def tag_access_page(inodeDict, index, r):
    if len(inodeDict[index]) <= 5:
        (lastreq, reUseList, lR, lW, pidList) = inodeDict[index]
        inodeDict[index] = (lastreq, reUseList, lR, lW, pidList, 1-r, r)
    else:
        (lastreq, reUseList, lR, lW, pidList, ftR, ftW) = inodeDict[index]
        inodeDict[index] = (lastreq, reUseList, lR, lW, pidList, ftR+1-r, ftW+r)

def output(page, process, file):
    fout = open(datafile[:-4]+"_" + str(timeSep) + "s_" + str(timeCountMax) + ".parse", "a")
    i = 0
    print("pid#=", len(process))

# 获得good data的throttling
    l = []
    lrulist = []
    for inode in page.keys():
        for index in page[inode].keys():
            tupleValue = page[inode][index]
            lrulist.append(tupleValue[0])
            if len(tupleValue) <= 5:
                l.append(0)
            else:
                l.append(tupleValue[5]+tupleValue[6])
    l.sort(reverse=True)
    throttling = l[int(0.2*len(l))]
    print("20%=", throttling, l[0], l[-1], l[int(0.1*len(l))])
    throttling = max(throttling, 2)
    print("final throt=", throttling)
    lrulist.sort(reverse=True)
    lruthrot = lrulist[int(0.02*len(l))]
    print("lru throt=", lruthrot)
    print("len=", len(l), len(lrulist), lrulist[0], lrulist[-1])
    lruright = 0
    for inode in page.keys():
        loc = file[inode].rfind('.')
        if loc==-1:
            fileType = None
        else:
            fileType = file[inode][loc+1:]
        
        for index in page[inode].keys():
            i += 1
            tupleValue = page[inode][index]
            if len(tupleValue) <= 5:
                accessType=""
                ftR=ftW=0
                (req, reUseList, lR, lW, pidList) = page[inode][index]
                dataType = False
            else:
                (req, reUseList, lR, lW, pidList, ftR, ftW) = page[inode][index]
                readRatio = ftR/(ftR+ftW)
                if readRatio >= 0.95:
                    accessType = "R"
                elif readRatio <= 0.05:
                    accessType = "W"
                else:
                    accessType = "C"
                dataType = (ftR+ftW>=throttling)
            if reUseList!=[]:
                minr = min(reUseList)
                maxr = max(reUseList)
                avg = sum(reUseList)/len(reUseList)
            else:
                minr = maxr = avg = 10**15
            l=[[],[],[],[]]
            for pid in pidList:
                (keyList, r, w) = process[pid]
                avgr = r/len(keyList)
                avgw = w/len(keyList)
                l[0].append(r)
                l[1].append(w)
                l[2].append(avgr)
                l[3].append(avgw)
            
            if req >= lruthrot and dataType:
                lruright += 1

            print(inode, index, fileType, file[inode], req, minr, 
                  maxr, avg, lR, lW, lR+lW, len(pidList), min(l[0]), max(l[0]), sum(l[0])/len(l[0]),
                  min(l[1]), max(l[1]), sum(l[1])/len(l[1]),
                  min(l[2]), max(l[2]), sum(l[2])/len(l[2]),
                  min(l[3]), max(l[3]), sum(l[3])/len(l[3]), ftR, ftW, ftR+ftW, accessType, dataType, sep=',',file=fout)
    print("total number of lines=", i)
    print("lruright=", lruright, ",ratio=", round(100*lruright/i, 2), "%")
    fout.close()


start = time.time()
load_file(datafile)
end = time.time()
print("consumed ", end-start, "s")