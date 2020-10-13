import sys
import random

count = int(sys.argv[1])

print('open test.db')

indicies = list(range(count))

random.shuffle(indicies)
for i in indicies:
    print('insert {} DATA{}'.format(i+1, i))

print('sep')

for i in range(count+10):
    print('find {}'.format(i+1))

print('sep')

random.shuffle(indicies)
for i in indicies:
    print('delete {}'.format(i+1))

print('sep')

for i in range(count+10):
    print('find {}'.format(i+1))

print('sep')

random.shuffle(indicies)
for i in indicies:
    print('insert {} NEW_DATA{}'.format(i+1, i))

print('sep')

for i in range(count+10):
    print('find {}'.format(i+1))
