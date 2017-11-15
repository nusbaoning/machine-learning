import numpy
from cache_algorithm import LRU
from time import time
import heapq
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
		heapq.heapreplace(self.minheap, (self.writeArray[page], page))
		heapq.heapreplace(self.maxheap, MaxHeapObj((self.writeArray[page], page)))

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
		self.mappingAddr[page] = destaddr
		del self.mappingAddr[oriaddr]
		self.mappingAddr[destaddr] = page
		self.write_help(page)

	def exchange_two_pages(self, p1, p2):
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
	def print_result(self, filename, size, mode, parameter):
		writeArray = self.writeArray
		print(max(writeArray), numpy.mean(writeArray), numpy.std(writeArray))
		fout = open("result.log", "a")
		print(filename[filename.rfind("/")+1:filename.find(".")], size, mode, parameter, max(writeArray), numpy.mean(writeArray), numpy.std(writeArray), file=fout, 
    		seq=',')
		fout.close()

# def core_function(filename, size, overProvision=0.1):
# 	start = time()
# 	fin = open(filename, 'r', encoding='utf-8', errors='ignore')
# 	lines = fin.readlines()
# 	initial(size, overProvision)
# 	cache = LRU(int(size*(1-overProvision)))
# 	end = time()
# 	print("load file finished consumed", end-start, "s")
# 	start = end
# 	blkSize = 4096
# 	i = 0
# 	nrLine = len(lines)
# 	for line in lines:
# 		i += 1
# 		if i%50000==0:
# 			end = time()
# 			print(i, i/nrLine*100, "%", "consumed", end-start, "s")
# 			start = end
# 		line = line.strip().split(',')
# 		blkStart = int((int(line[4]))/blkSize)
# 		blkEnd = int((int(line[4])+int(line[5])-1)/blkSize)
# 		if line[3]=='Write':
# 			reqtype = 1
# 		else:
# 			reqtype = 0
# 		# print(mappingAddr, mappingPage)
# 		for block in range(blkStart,blkEnd+1):        	
# 			hit = cache.is_hit(block)				
# 			evtBlk = cache.update_cache(block)
# 			if hit:
# 				if reqtype==1:
# 					# print(i, block)
# 					# print(mappingAddr[block])
# 					writeArray[mappingAddr[block]] += 1
# 			else:
# 				if evtBlk != None:
# 					# print(i, evtBlk)
# 					delete_page_mapping(evtBlk, mappingAddr[evtBlk])
# 				nwpg = get_free_page()
# 				add_page_mapping(block, nwpg)
# 				writeArray[nwpg] += 1

# 	print_result()


def parse(line):
	blkSize = 4096
	line = line.strip().split(',')
	blkStart = int((int(line[4]))/blkSize)
	blkEnd = int((int(line[4])+int(line[5])-1)/blkSize)	
	if line[3]=='Write':
		reqtype = 1
	else:
		reqtype = 0
	return (reqtype, blkStart, blkEnd)


def static_wear_leveling(filename, size, mode, parameter):
	fin = open(filename, 'r', encoding='utf-8', errors='ignore')
	lines = fin.readlines()
	cache = LRU(size)
	pm = PageManage(size)	
	i = 0
	nrLine = len(lines)
	start = time()
	for line in lines:
		i += 1
		nrsegment = 50000
		if i%nrsegment==0:
			end = time()
			print(i, i/nrLine*100, "%", "consumed", end-start, "s", "expected remaining time", round((nrLine-i)/nrsegment*(end-start),3), "s")
			start = end
		reqtype, blkStart, blkEnd = parse(line)
		# print(mappingAddr, mappingPage)
		for block in range(blkStart,blkEnd+1):        	
			hit = cache.is_hit(block)				
			evtBlk = cache.update_cache(block)
			if hit:
				if reqtype==1:
					# print(i, block)
					# print(mappingAddr[block])
					pm.write_help(pm.mappingAddr[block])
			else:
				if evtBlk != None:
					# print(i, evtBlk)
					pm.modify_page_mapping(evtBlk, block)
				else:
					page = pm.get_free_page()
					pm.add_page_mapping(block, page)
		if mode == 'P':
			if i%parameter==0:
				(maxv, p1), (minv, p2) = pm.get_max_min_pages()
				pm.exchange_two_pages(p1, p2)
		elif mode == 'T':
			(maxv, p1), (minv, p2) = pm.get_max_min_pages()
			if maxv-minv>=parameter:
				pm.exchange_two_pages(p1,p2)				
	pm.print_result(filename, size, mode, parameter)
	
start = time()
static_wear_leveling("/mnt/raid5/trace/MS-Cambridge/hm_0.csv", 2**19, "P", 10000)
static_wear_leveling("/mnt/raid5/trace/MS-Cambridge/hm_0.csv", 2**19, "T", 5)
end = time()
print("consumed", end-start, "s")