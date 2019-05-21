import csv
import os
import sys

from newspaper import Article

def main():
    in_folder = sys.argv[1]
    out_folder = sys.argv[2]

    if not os.path.isdir(out_folder):
        os.mkdir(out_folder)

    raw_files = set(os.listdir(in_folder))
    out_files = set(os.listdir(out_folder))

    raw_files -= out_files

    for f in raw_files:
        print(f)
        with open(os.path.join(in_folder, f)) as f_in, \
                open(os.path.join(out_folder, f), 'w') as f_out:
            reader = csv.reader(f_in, delimiter='\t')
            writer = csv.writer(f_out, delimiter=',')
            for row in reader:
                title = row[1]
                url = row[2]
                cat = row[4]
                id_ = row[5]
                try:
                    article = Article(url)
                    article.download()
                    article.parse()

                    writer.writerow([title, article.text, cat, id_])
                except Exception as _:
                    print('The url {} cannot be processed. Skip.'.format(url))


if __name__ == "__main__":
    main()
