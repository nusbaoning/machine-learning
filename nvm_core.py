import random
import numpy
from cache_algorithm import LRU
from time import time
writeArray = []
freePage = {}
mappingAddr = {}
mappingPage = {}
# minFreePage = -1

def initial(size, overProvision):
	global writeArray, freePage, mappingAddr, mappingPage, minFreePage
	writeArray = []
	freePage = {}
	mappingAddr = {}
	mappingPage = {}
	# minFreePage = int(overProvision*size)
	for i in range(0,size):
		writeArray.append(0)
		freePage[i] = True

def get_free_page():
	global freePage
	i = random.choice(list(freePage.keys()))
	del freePage[i]
	return i

def add_page_mapping(addr, page):
	global mappingAddr, mappingPage
	mappingAddr[addr] = page
	mappingPage[page] = addr

def delete_page_mapping(addr, page):
	global mappingAddr, mappingPage, freePage
	del mappingPage[page]
	del mappingAddr[addr]
	freePage[page] = True

def print_result():
	print(max(writeArray), numpy.mean(writeArray), numpy.std(writeArray))

def core_function(filename, size, overProvision=0.1):
	start = time()
	fin = open(filename, 'r', encoding='utf-8', errors='ignore')
	lines = fin.readlines()
	initial(size, overProvision)
	cache = LRU(int(size*(1-overProvision)))
	end = time()
	print("load file finished consumed", end-start, "s")
	start = end
	blkSize = 4096
	i = 0
	nrLine = len(lines)
	for line in lines:
		i += 1
		if i%50000==0:
			end = time()
			print(i, i/nrLine*100, "%", "consumed", end-start, "s")
			start = end
		line = line.strip().split(',')
		blkStart = int((int(line[4]))/blkSize)
		blkEnd = int((int(line[4])+int(line[5])-1)/blkSize)
		if line[3]=='Write':
			reqtype = 1
		else:
			reqtype = 0
		# print(mappingAddr, mappingPage)
		for block in range(blkStart,blkEnd+1):        	
			hit = cache.is_hit(block)				
			evtBlk = cache.update_cache(block)
			if hit:
				if reqtype==1:
					# print(i, block)
					# print(mappingAddr[block])
					writeArray[mappingAddr[block]] += 1
			else:
				if evtBlk != None:
					# print(i, evtBlk)
					delete_page_mapping(evtBlk, mappingAddr[evtBlk])
				nwpg = get_free_page()
				add_page_mapping(block, nwpg)
				writeArray[nwpg] += 1

	print_result()
start = time()
core_function("D:/trace/MSR-Cambridge/hm_0.csv", 2**19)
end = time()
print("consumed", end-start, "s")