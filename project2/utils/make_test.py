import sys
import random

count = int(sys.argv[1])

print('open test.db')

indicies = list(range(count))
random.shuffle(indicies)
for i in indicies:
    print('insert {} DATA{:03d}'.format(i+1, i))

