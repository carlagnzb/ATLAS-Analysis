import sys
import time

file = sys.argv[1]
long = sys.argv[2]

start = time.time()
with open(file) as f:
    lines = f.read().splitlines()
print(len(lines), int(long))

if (len(lines)) == int(long):
    print('OK')
else:
    print('Some job fail')

stop = time.time()
print('exec del merge:', str(stop-start))
