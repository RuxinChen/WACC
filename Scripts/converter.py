import csv
import argparse

def converter(old_csv, new_csv):

    with open(old_csv, 'r') as f1:
        with open(new_csv, 'w') as f2:
            reader = csv.reader(f1, delimiter='\t')
            writer = csv.writer(f2, delimiter=',')
            for line in reader:
                writer.writerow(line)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='Convert mrjob output csv file to properly delimited csv file',
            )

    parser.add_argument(
            'old_csv',
            type=str,
            help='The mrjob output csv file to convert.',
            )

    args = parser.parse_args()
    old_csv = args.old_csv
    new_csv = '{0}.csv'.format("new_" + "".join(old_csv.split('.csv')))

    converter(old_csv, new_csv)