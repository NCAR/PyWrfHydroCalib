import os
import argparse

def main():
    parser = argparse.ArgumentParser(description='Main program to start the check of the Troute processes and creating a Troute')
    parser.add_argument('pid', type=str, help='List containing info about each basin Troute process.')

    args = parser.parse_args()
    print(args.pid)

    print('I am here\n')

if __name__ == '__main__':
    main()
