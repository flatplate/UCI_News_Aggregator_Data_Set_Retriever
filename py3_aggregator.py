import csv
import multiprocessing
import os
import sys
import concurrent.futures

from newspaper import fulltext
import requests


def get_content(url):
    try:
        request = requests.get(url, timeout=30)
        if request.status_code != 200:
            print('The url {} does not exist. Skip.'.format(url))
            return ""
        return request.text
    except requests.exceptions.Timeout as _:
        print('The url {} timeout. Skip.'.format(url))
    except Exception as e:
        print('The url {} cannot be processed. Skip.'.format(url), str(e))
    return ""

def get_fulltext_catch_exceptions(text):
    try:
        return fulltext(text)
    except Exception as e:
        print('The text cannot be processed. Skip.', str(e))
        return ""

def process_raw(args):
    f = args[0]
    in_folder = args[1]
    out_folder = args[2]
    in_file = os.path.join(in_folder, f)
    out_file = os.path.join(out_folder, f)
    
    with open(in_file, "r", encoding="utf-8") as f_in, open(out_file, 'w', encoding="utf-8") as f_out:
        reader = csv.reader(f_in, delimiter='\t')
        writer = csv.writer(f_out, delimiter=',')

        articles = [{
            "title": row[1],
            "content": "",
            "cat": row[4],
            "id_": row[5],
            "url": row[2]
        } for row in reader]

        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as concurrentPool:
            contents = [get_fulltext_catch_exceptions(text) for text in concurrentPool.map(get_content, [article["url"] for article in articles])]

        for article, content in zip(articles, contents):
            article["content"] = content

        writer.writerows([article.values() for article in articles])

def main():
    global in_folder, out_folder

    if len(sys.argv) < 3:
        print("Usage: python UCI_news_aggregator_DS_retriever.py <input_folder_name> <output_folder_name>")

    in_folder = sys.argv[1]
    out_folder = sys.argv[2]

    if not os.path.isdir(out_folder):
        os.mkdir(out_folder)

    raw_files = set(os.listdir(in_folder))
    out_files = set(os.listdir(out_folder))

    raw_files -= out_files
    with multiprocessing.Pool(
            processes=max(1, multiprocessing.cpu_count())) as pool:
        pool.map(process_raw, raw_files)


if __name__ == "__main__":
    main()
