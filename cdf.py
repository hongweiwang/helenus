import os, sys, numpy

if len(sys.argv) < 4:
	print 'usage: python cdf.py [option] [mean_interval] [nqueries]'
	sys.exit(1)

option = sys.argv[1]
dir = 'log/'

data = []
for file in os.listdir(dir):
	args = file.split('_')
	if args[1] == sys.argv[1] and args[2] == sys.argv[2] and args[3] == sys.argv[3] and '.cdf' not in file:
		f = open(dir + file)
		if option == '1':
			for line in f:
				data.append(float(line))
		if option == '2':
			lines = f.readlines()
			i = 0
			while i < len(lines):
				val1 = float(lines[i])
				val2 = float(lines[i+1])
				min_val = min(val1, val2)
				data.append(min_val)
				i = i + 2
		f.close()

f_out = open(dir + sys.argv[1] + '_' + sys.argv[2] + '_' + sys.argv[3]  + '.cdf', 'w')
data = sorted(data)

sum = 0.0
for val in data:
	sum = sum + val
mean = sum / float(len(data))
f_out.write('#Mean = ' + str(mean) + '\n')

min_val = data[0]
max_val = data[-1]

bins = 100000
freq_list, bins = numpy.histogram(data, bins)

total = len(data)
i = 0
count = 0
for freq in freq_list:
	lval = bins[i]
	rval = bins[i+1]
	count = count + freq
	fraction = float(count) / float(total)
	f_out.write(str(lval) + ',' + str(rval) + ',' + str(fraction) + '\n')
	i = i + 1
f_out.close()

print 'count: ' + str(len(data))
print 'mean: ' + str(mean)
print 'min: ' + str(data[0])
print 'max: ' + str(data[-1])







