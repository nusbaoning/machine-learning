

fin = open("D:/data.log", 'r', encoding='utf-8', errors='ignore')
fout1 = open("optane.log", "w")
fout2 = open("optane.csv", "w")
lines = fin.readlines()
for i in range(0, len(lines)):
	line = lines[i]
	if line.startswith("sysbench --threads=16"):
		print(line[0:-1], file=fout1)
	if line.startswith("File operations:"):
		for j in range(0,3):
			i+=1
			value=lines[i].split(' ')[-1][0:-1]
			print(value, end=',', file=fout2)
			print(value, end=',', file=fout1)
		i+=2
		for j in range(0,2):
			i+=1
			value=lines[i].split(' ')[-1][0:-1]
			print(value, end=',', file=fout2)
			print(value, end=',', file=fout1)
		i+=6
		for j in range(0,5):
			i+=1
			value=lines[i].split(' ')[-1][0:-1]
			print(value, end=',', file=fout2)
			print(value, end=',', file=fout1)
		print(file=fout2)
		print(file=fout1)
			

fin.close()
fout1.close()
fout2.close()
