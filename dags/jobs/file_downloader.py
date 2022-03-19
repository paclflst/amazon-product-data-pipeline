import asyncio
import concurrent.futures
import requests
import os

MAX_RETRIES = 1
URL = 'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Movies_and_TV.json.gz'
# OUTPUT = 'amazon/video.json.gz'
OUTPUT = '/usr/local/spark/resources/data/meta_movies_and_tv/meta_movies_and_tv.json.gz'


async def get_size(url):
    response = requests.head(url)
    size = int(response.headers['Content-Length'])
    return size


def download_range(url, start, end, output, retries = 0):
    try:
        if end > 10 * 1000000 and end < 90 * 1000000:
            test = 1/0
        headers = {'Range': f'bytes={start}-{end}'}
        response = requests.get(url, headers=headers)

        with open(output, 'wb') as f:
            for part in response.iter_content(1024):
                f.write(part)
        return True
    except Exception as e:
        print(e, retries, end, '\n')
        if retries < MAX_RETRIES:
            download_range(url, start, end, output, retries+1)
        else:
            raise e

def is_success(future):
   return future.done() and not future.cancelled() and future.exception() is None

async def download(executor, url, output, chunk_size=1000000):
    loop = asyncio.get_event_loop()

    file_size = await get_size(url)
    chunks = range(0, file_size, chunk_size)

    tasks = [
        loop.run_in_executor(
            executor,
            download_range,
            url,
            start,
            start + chunk_size - 1,
            f'{output}.part{i}',
        )
        for i, start in enumerate(chunks)
    ]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    if all(is_success(f) for f in done):
        with open(output, 'wb') as o:
            for i in range(len(chunks)):
                chunk_path = f'{output}.part{i}'

                with open(chunk_path, 'rb') as s:
                    o.write(s.read())

                os.remove(chunk_path)
    else:
        for f in pending:
            f.cancel()
        failed_future = next(f for f in done if not is_success(f))
        raise failed_future.exception()

def parallel_download():
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(
            download(executor, URL, OUTPUT)
        )
    finally:
        loop.close()

if __name__ == '__main__':
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(
            download(executor, URL, OUTPUT)
        )
    finally:
        loop.close()