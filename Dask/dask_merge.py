import sys
import time
import ast

file = sys.argv[1]
long = sys.argv[2]

res = []
start = time.time()
with open(file) as f:
    for line in f:
         lista = ast.literal_eval(line)
         res += lista

print('file',len(res), 'num longitud entrat',int(long))

if (len(res)) == int(long):
    print('OK')
else:
    print('Some job fail')
stop = time.time()
print('exec del merge:', str(stop-start))
