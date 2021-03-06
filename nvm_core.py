import numpy
from mts_cache_algorithm import LRU
from time import time
import heapq
import traceback
import sys
from collections import deque
uclnDict = {
"rsrch_0": 93773,
"src2_0":	187918,
"wdev_0":	138102,
"prxy_0":	230657,
"mds_0":	818174,
"ts_0":	239289,
"stg_0":	1668913,
"proj_0":	833112,
"hm_0":	610404,
"prn_0":	3886547,
"web_0":	1915690,
"usr_0":	646969
}

class MaxHeapObj(object):
  def __init__(self,val): self.val = val
  def __lt__(self,other): return self.val > other.val
  def __eq__(self,other): return self.val == other.val
  def __str__(self): return str(self.val)

class PageManage(object):
	"""docstring for PageManage"""
	def __init__(self, size):
		self.writeArray = []
		self.freePage = {}
		self.mappingAddr = {}
		self.mappingPage = {}
		self.minheap = []
		self.maxheap = []
		self.rwdict = {}
		self.size = size
		# minFreePage = int(overProvision*size)
		for i in range(0,size):
			self.writeArray.append(0)
			self.freePage[i] = True
			self.minheap.append((0, i))
			self.maxheap.append(MaxHeapObj((0,i)))
		heapq.heapify(self.minheap)
		heapq.heapify(self.maxheap)
		
	def get_free_page(self):
		key, _ = self.freePage.popitem()
		return key

	def write_help(self, page):
		self.writeArray[page] += 1
		block = self.writeArray[page]
		tp = (block, page)		
		heapq.heapreplace(self.minheap, tp)
		heapq.heapreplace(self.maxheap, MaxHeapObj(tp))
		
	def add_page_mapping(self, addr, page):
		self.mappingAddr[addr] = page
		self.mappingPage[page] = addr
		self.write_help(page)

	def delete_page_mapping(self, addr, page):
		del self.mappingPage[page]
		del self.mappingAddr[addr]
		self.freePage[page] = True
		

	def modify_page_mapping(self, oriaddr, destaddr):
		page = self.mappingAddr[oriaddr]
		self.mappingPage[page] = destaddr
		del self.mappingAddr[oriaddr]
		self.mappingAddr[destaddr] = page
		self.write_help(page)
	
	def add_rwitem(self, block, rw):
		self.rwdict[block] = rw

	def del_rwitem(self, block):
		if block in self.rwdict:
			del self.rwdict[block]

	def optimized_exchange(self, number, cache):
		l = sorted(self.writeArray, reverse=True)
		maxPages = l[0:number]
		minPages = l[-1-number:-1]
		minPages.reverse()
		cacheTop = cache.get_top_n(number)
		cacheTail = cache.get_tail_n(number)
		topDict = {}
		tailDict = {}
		for head in cacheTop:
			topDict[head] = True
		for tail in cacheTail:
			tailDict[tail] = True
		l = [-1,-1,-1,-1]
		for page in maxPages:
			if page in self.mappingPage:
				block = self.mappingPage[page]
				rw = self.rwdict[block]
				inhead = block in topDict
				intail = block in tailDict
				# write head
				if rw and inhead:
					l[0] = page
					break
				if l[1] >= 0:
					continue
				# first read tail
				if not rw and intail:
					l[1] = page
					continue
				if (rw and intail) or (not rw and inhead):
				 	continue
				if rw and l[2]==-1:
					l[2] = page
				elif not rw and l[3]==-1:
					l[3] = page
		maxPage = None
		for page in l:
			if page > -1:
				maxPage = page
				break
		if maxPage == None:
			return
		l = [-1,-1,-1,-1]
		for page in minPages:
			if page not in self.mappingPage:
				l[0] = page
				break			
			block = self.mappingPage[page]
			rw = self.rwdict[block]
			inhead = block in topDict
			intail = block in tailDict
			# read head
			if not rw and inhead:
				l[0] = page
				break
			if l[1] >= 0:
				continue
			# first write tail
			if rw and intail:
				l[1] = page
				continue
			if (not rw and intail) or (rw and inhead):
			 	continue
			if not rw and l[2]==-1:
				l[2] = page
			elif rw and l[3]==-1:
				l[3] = page
		minPage = None
		for page in l:
			if page > -1:
				minPage = page
				break
		if minPage == None:
			return
		self.exchange_two_pages(maxPage, minPage)

					




	def exchange_two_pages(self, p1, p2):
		a1 = None
		a2 = None
		if p1 in self.mappingPage:			
			a1 = self.mappingPage[p1]
			self.mappingAddr[a1] = p2
		if p2 in self.mappingPage:
			a2 = self.mappingPage[p2]
			self.mappingAddr[a2] = p1
		if p2 in self.mappingPage:			
			self.mappingPage[p1] = a2
			self.write_help(p1)
		else:
			self.freePage[p1] = True
		if p1 in self.mappingPage:			
			self.mappingPage[p2] = a1		
			self.write_help(p2)
		else:
			self.freePage[p2] = True
		
	def get_max_min_pages(self):
		maxp = tuple(self.maxheap[0].val)
		minp = self.minheap[0]
		return (maxp, minp)

	def print_result(self, filename, size, mode, pr, th, cache, req, number=None):
		writeArray = self.writeArray
		print(round(100*cache.hit/req, 2), max(writeArray), numpy.mean(writeArray), numpy.std(writeArray))
		fout = open("result.log", "a")
		print(filename[filename.rfind("/")+1:filename.find(".")], size, number, mode, pr, th, round(100*cache.hit/req, 2), max(writeArray), numpy.mean(writeArray), numpy.std(writeArray), file=fout, 
    		sep=',')
		fout.close()



def parse(line):
	# blkSize = 4096
	# line = line.strip().split(',')
	# blkStart = int((int(line[4]))/blkSize)
	# blkEnd = int((int(line[4])+int(line[5])-1)/blkSize)	
	# if line[3]=='Write':
	# 	reqtype = 1
	# else:
	# 	reqtype = 0
	items = line.split(' ')
	reqtype = int(items[0])
	block = int(items[2])
	return (reqtype, block, block)

def optimized_wl(filename, size, pr, th, k):
	fin = open(filename, 'r', encoding='utf-8', errors='ignore')
	lines = fin.readlines()
	cache = LRU(size)
	pm = PageManage(size)	
	i = 0
	req = 0
	nrLine = len(lines)
	start = time()
	for line in lines:
		i += 1
		nrsegment = 100000
		if i%nrsegment==0:
			end = time()
			print(i, round(i/nrLine*100,2), "%", "consumed", round(end-start,2), "s", "expected remaining time", round((nrLine-i)/nrsegment*(end-start),2), "s")
			start = end
		reqtype, blkStart, blkEnd = parse(line)
		for block in range(blkStart,blkEnd+1):      	
			hit = cache.is_hit(block)
			pm.add_rwitem(block, reqtype)	
			req += 1
			if hit:
				cache.update_cache(block)
				if reqtype==1:
					pm.write_help(pm.mappingAddr[block])
			else:
				# cache is full
				if len(cache) >= size:
					evtPttl = cache.get_tail_n(k)
					minUpdate = -1
					minBlock = -1
					for evtBlk in evtPttl:
						page = pm.mappingAddr[evtBlk]
						write = pm.writeArray[page]
						if minUpdate < 0 or write < minUpdate:
							minUpdate = write
							minBlock = evtBlk
					cache.delete_cache(minBlock)
					pm.del_rwitem(minBlock)
					cache.update_cache(block)
					pm.modify_page_mapping(minBlock, block)
				else:
					cache.update_cache(block)
					page = pm.get_free_page()
					pm.add_page_mapping(block, page)
		if req%pr==0:
			(maxv, p1), (minv, p2) = pm.get_max_min_pages()
			if maxv-minv>=th:
				pm.optimized_exchange(k, cache)				
		
	pm.print_result(filename, size, "CacheWL", pr, th, cache, req, k)

def static_wear_leveling(filename, size, pr, th):
	fin = open(filename, 'r', encoding='utf-8', errors='ignore')
	lines = fin.readlines()
	cache = LRU(size)
	pm = PageManage(size)	
	i = 0
	req = 0
	nrLine = len(lines)
	start = time()
	for line in lines:
		i += 1
		nrsegment = 100000
		if i%nrsegment==0:
			end = time()
			print(i, round(i/nrLine*100,2), "%", "consumed", round(end-start,2), "s", "expected remaining time", round((nrLine-i)/nrsegment*(end-start),2), "s")
			start = end
		reqtype, blkStart, blkEnd = parse(line)
		# print(mappingAddr, mappingPage)
		for block in range(blkStart,blkEnd+1):      	
			hit = cache.is_hit(block)				
			evtBlk = cache.update_cache(block)
			req += 1
			if hit:
				if reqtype==1:
					# req += 
					# print(i, block)
					# print(mappingAddr[block])
					# try:
					pm.write_help(pm.mappingAddr[block])
					# except Exception as e:
					# 	print(block)
					# 	if block in pm.mappingAddr:
					# 		pg = pm.mappingAddr[block]
					# 		print(pg)
					# 		print(len(pm.writeArray))
					# 	traceback.print_exc()
					# 	exit(-1)
					
			else:
				# req += 1
				# if block == badblk:
				# 	print(evtBlk, pm.mappingAddr[errorblk])
				if evtBlk != None:					
					pm.modify_page_mapping(evtBlk, block)
					# if block == badblk:
					# 	print(evtBlk, block)
					
				else:
					page = pm.get_free_page()
					pm.add_page_mapping(block, page)
					# if block == badblk:
					# 	print(block, page)
					# if block == errorblk:
					# 	print("add", page, pm.mappingAddr[block])
			# if errorblk in pm.mappingAddr and pm.mappingAddr[errorblk]!=121:
			# 	print(reqtype, block, hit, pm.mappingAddr[errorblk], pm.mappingAddr[block])
			# 	if not hit:
			# 		print(evtBlk)
			# 	exit(-1)
		# 	print(pm.mappingPage, pm.mappingAddr, reqtype, block, hit, sum(pm.writeArray))
			# if errorblk in pm.mappingAddr and errorPage!=pm.mappingAddr[errorblk]:
		# 	print(reqtype, block, hit, pm.mappingAddr[errorblk], pm.mappingAddr[block])
			# 	exit(-1)
		# if i>=10:
		# 	exit(-1)
		if req%pr==0:
			(maxv, p1), (minv, p2) = pm.get_max_min_pages()
			if maxv-minv>=th:
				pm.exchange_two_pages(p1,p2)				
	pm.print_result(filename, size, "Traditional", pr, th, cache, req)

print("Version 4.0")
# traces = [
# "web_0", "stg_0", "mds_0", "src2_0", "rsrch_0", "ts_0",
# "usr_0", "prn_0", "prxy_0", "proj_0", "wdev_0", "hm_0"]

# ts_0 rsrch, src2, wdev, stg, mds

# for trace in traces:
start = time()
trace = sys.argv[2]
filename = "/mnt/raid5/trace/MS-Cambridge/" + trace + ".csv.req"
size = int(0.1*uclnDict[trace])
if sys.argv[1] == 0:		
	static_wear_leveling(filename, size, 10000, 5)
	static_wear_leveling(filename, size, 1000, 5)
	static_wear_leveling(filename, size, 10000, 50)
	static_wear_leveling(filename, size, 1000, 50)
elif sys.argv[1] == 1:
	# def optimized_wl(filename, size, pr, th, k)
	optimized_wl(filename, size, 10000, 5, 1000)
	optimized_wl(filename, size, 1000, 5, 1000)
	optimized_wl(filename, size, 10000, 50, 1000)
	optimized_wl(filename, size, 1000, 50, 1000)

end = time()
print(trace, "consumed", end-start, "s")


