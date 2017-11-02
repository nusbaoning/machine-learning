# -*- coding: utf-8 -*-
"""
Created on Thu Nov  2 13:36:31 2017

@author: Bao Ning
"""
import codecs
import time
import traceback

datafile = 'fileserver_60s.log'
HEADLINENUMBER = 11
MINLENGH = 10
READ_FUNCTION_NAME = "generic_file_read_iter"




'''
main overflow of the program
'''
def load_file(datafile):
    i = 0
    line = ''
    r = 0
    page = {}
    file = {}
    req = 1
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
            current = timestamp
#            if i<=30:
#                 print("line=", i, "task=", task, "pid=", pid, 
#                       "timestamp=", timestamp, "function_name=", function_name, 
#                       "inode=", inode, "isize=", isize, "pos=", pos, "count=", count, "filename=",filename)
#            else:
#                 break
            if not inode in file.keys():
                file[inode] = filename
# fileserver.f中没有重命名指令，filename不同说明文件被删除，那么所有历史数据一并删除，开始记录新的访问请求
            elif filename != file[inode]:
                file[inode] = filename
                del page[inode]
            if not inode in page.keys():
                page[inode] = {}
            index0 = pos >> 12
            index1 = (pos + count - 1) >> 12
            for index in range(index0, index1+1):
                access_page(page[inode], index, req, r, pid)
                req += 1
#            if r == 0:
#                readreq += index_1 - index_0 + 1
#                continue
#            for index in range(index_0, index_1 + 1):         
#                writereq += 1
#                if not index in page[inode].keys():
#                    indexNum += 1
#                    page[inode][index] = (timestamp, 0)
#                else:
#                    last, overwrite = page[inode][index]
#                    if timestamp - last <= DIRTY_THROT:
#                        page[inode][index] = (timestamp, overwrite + 1)
#                    else:
#                        # f.write("normal addover %d, %d, %d\n" % (inode, index, overwrite))
#                        addoverwrite(overwrite)
#                        page[inode][index] = (timestamp, 0)
            
            
            if i%10000==0:
                e = time.time()
                print(i, int(100*i/lineNum), "%", "consumed", (e-s), "s")
                s = e
    except Exception as e:
        print(i, line)
        print(len(line))
#        print ('str(Exception):\t', str(Exception))
#        print ('str(e):\t\t', str(e))
        print (repr(e))
#        print ('e.message:\t', e.message)
        print (traceback.print_exc())
        print (traceback.format_exc())
    finally:
        fout = open(datafile+".parse", "w")
        for inode in page.keys():
            loc = file[inode].rfind('.')
            if loc==-1:
                fileType = None
            else:
                fileType = file[inode][loc+1:]
            for index in page[inode].keys():                      
                (req, reUseList, lR, lW, pidList) = page[inode][index]
                if reUseList!=[]:
                    minr = min(reUseList)
                    maxr = max(reUseList)
                    avg = sum(reUseList)/len(reUseList)
                else:
                    minr = maxr = avg = -1
                print(inode, index, fileType, file[inode], req, minr, 
                      maxr, avg, lR, lW, lR+lW, len(pidList), sep=',',file=fout)
        fin.close()
        fout.close()
        
def access_page(inodeDict, index, req, rw, pid):
    if index not in inodeDict:
        inodeDict[index] = (req, [], 1-rw, rw, [pid])
    else:
        (lastreq, reUseList, lR, lW, pidList) = inodeDict[index]
        reUseList.append(req-lastreq)
        if pid not in pidList:
            pidList.append(pid)
        inodeDict[index] = (req, reUseList, lR+1-rw, lW+rw, pidList)
    

load_file(datafile)