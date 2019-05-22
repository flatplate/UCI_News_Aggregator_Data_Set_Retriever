import csv
import multiprocessing
import os
import sys

from newspaper import fulltext
import requests


in_folder = None
out_folder = None

def process_raw(f):
    with open(os.path.join(in_folder, f)) as f_in, \
            open(os.path.join(out_folder, f), 'w') as f_out:
        reader = csv.reader(f_in, delimiter='\t')
        writer = csv.writer(f_out, delimiter=',')
        for row in reader:
            title = row[1]
            url = row[2]
            cat = row[4]
            id_ = row[5]
            request = requests.get(url)
            if request.status_code != 200:
                print('The url {} does not exist. Skip.'.format(url))
                continue
            text = fulltext(request.text)
            try:
                writer.writerow([title, text, cat, id_])
            except Exception as _:
                print('The url {} cannot be processed. Skip.'.format(url))


def main():
    global in_folder, out_folder
    in_folder = sys.argv[1]
    out_folder = sys.argv[2]

    if not os.path.isdir(out_folder):
        os.mkdir(out_folder)

    raw_files = set(os.listdir(in_folder))
    out_files = set(os.listdir(out_folder))

    raw_files -= out_files
    with multiprocessing.Pool(
            processes=max(1, multiprocessing.cpu_count() - 2)) as pool:
        pool.map(process_raw, raw_files)


if __name__ == "__main__":
    main()
