import os

dir = 'log/'
for file in os.listdir(dir):
	client, option, mean, nquery = file.split("_")
	os.system("python cdf.py " + option + " "+ mean + " " + nquery)
	print (client,option, mean, nquery)
