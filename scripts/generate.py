import argparse
import random


"""
Main function
"""
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Number generator')
    parser.add_argument('--length', type=int, help='Length of array')
    parser.add_argument('--count', type=int, help='Count of arrays', default=1)
    parser.add_argument('--output', type=str, help='Output file name', default='data')

    options = parser.parse_args()

    with open(options.output, 'w+') as f:
        for i in range(options.count):
            line = ' '.join(list(map(str, [random.randint(1, 100) for k in range(options.length)])))
            f.write(line + '\n')
