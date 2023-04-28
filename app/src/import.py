#%%
import urllib
import os
import time
import sys

nyc_path = '/files/nyc-ytaxi'
if not os.path.isdir(nyc_path): os.mkdir(nyc_path)
# Url path
bUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
# File prefix
ycabPrx = "yellow_tripdata_"

#Availaiblity of data set by month & year
yearsDict = {}
years = range(2009, 2022)
months = range(1,13)

for year in years:    
    yearsDict[year] = months

ycabUrls = []
ycabFnames = []
for year, months in yearsDict.iteritems():
    year_string = str(year)
    for month in months:
        month_string = str(month)
        if len(month_string) == 1:
            month_string = "0"+month_string
        url = bUrl+ycabPrx+year_string+'-'+month_string+".parquet"
        ycabUrls.append(url)
        ycabFnames.append(ycabPrx+year_string+'-'+month_string+".parquet")

def reporthook(count, block_size, total_size):
    global start_time
    if count == 0:
        start_time = time.time()
        return
    duration = time.time() - start_time
    progress_size = int(count * block_size)
    speed = int(progress_size / (1024 * duration))
    percent = int(count * block_size * 100 / total_size)
    size_mb = progress_size / (1024 * 1024)
    sys.stdout.write(f"\r...{percent}%%, {size_mb} MB, {speed} KB/s, {duration} seconds passed")
    sys.stdout.flush()

def save(url, filename):
    urllib.urlretrieve(url, filename, reporthook)

#%%
for link, filename in zip(ycabUrls,ycabFnames):
    print(link, filename)
    save(url, nyc_path + '/' + filename)
