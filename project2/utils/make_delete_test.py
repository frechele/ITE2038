import sys
import random

count = int(sys.argv[1])

print('open test.db')

indicies = list(range(count-10))
random.shuffle(indicies)
for i in indicies:
    print('delete {}'.format(i+1))

for i in range(count+10):
    print('find {}'.format(i+1))
