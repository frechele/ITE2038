import sys

count = int(sys.argv[1])

with open(sys.argv[2], 'rt') as f:
    lines = f.readlines()

phase = 0

for idx, line in enumerate(lines, 1):
    line = line.strip()

    if line == 'sep':
        phase += 1
        print('new phase {} at {}'.format(phase, idx))
        continue

    if phase == 0 or phase == 2 or phase == 4:
        if line.find('fail') != -1:
            print('check failed (phase {})'.format(phase))
            print(line)
            sys.exit()

    elif phase == 1:
        tokens = line.split()

        num = int(tokens[0])
        if num <= count and tokens[2] != 'DATA{}'.format(num-1):
            print('check failed (phase {})'.format(phase))
            print(line)
            sys.exit()
        elif num > count and line.find('not') == -1:
            print('check failed (phase {})'.format(phase))
            print(line)
            sys.exit()

    elif phase == 3:
        if line.find('not') == -1:
            print('check failed (phase {})'.format(phase))
            print(line)
            sys.exit()
            
    elif phase == 5:
        tokens = line.split()

        num = int(tokens[0])
        if num <= count and tokens[2] != 'NEW_DATA{}'.format(num-1):
            print('check failed (phase {})'.format(phase))
            print(line)
            sys.exit()
        elif num > count and line.find('not') == -1:
            print('check failed (phase {})'.format(phase))
            print(line)
            sys.exit()

print('check success')