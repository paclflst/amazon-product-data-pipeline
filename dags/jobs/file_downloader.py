import asyncio
from concurrent.futures import ThreadPoolExecutor
import requests
import os
from jobs.services.file_serivce import FileService
from utils.logging import get_logger

MAX_RETRIES = 1

fs = FileService()
logger = get_logger(__name__)


async def get_size(url):
    response = requests.head(url)
    size = int(response.headers['Content-Length'])
    return size


def download_range(url: str, start: int, end: int, output: str, retries: int=0):
    try:
        headers = {'Range': f'bytes={start}-{end}'}
        with requests.get(url, headers=headers, stream=True) as response:
            return fs.save_file_from_response(output, response)
    except Exception as e:
        if retries < MAX_RETRIES:
            download_range(url, start, end, output, retries+1)
            logger.warning(f'Error while downloading chunk from {url}:\n{e}')
        else:
            raise e


def is_success(future):
    return future.done() and not future.cancelled() and future.exception() is None


async def download(executor: ThreadPoolExecutor, url: str, output: str, chunk_size: int):
    loop = asyncio.get_event_loop()

    file_size = await get_size(url)
    logger.debug(f'File size from {url} is {file_size/(1024*1024)}M')
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
        fs.merge_files_into_one(
            output, [f'{output}.part{i}' for i in range(len(chunks))])
    else:
        for f in pending:
            f.cancel()
        failed_future = next(f for f in done if not is_success(f))
        raise failed_future.exception()


def get_file_name_from_url(url):
    return url.split('/')[-1].lower()


def parallel_download(url: str, target_folder: str, chunk_size: int=1024*1024, max_workers: int=4):
    logger.debug(f'Start downloading {url} to {target_folder}')
    target_file_name = get_file_name_from_url(url)

    executor = ThreadPoolExecutor(max_workers=max_workers)
    loop = asyncio.get_event_loop()
    output_file = os.path.join(target_folder, target_file_name)
    try:
        loop.run_until_complete(download(executor, url, output_file, chunk_size))
        logger.debug(f'Finish downloading {url} to {target_folder}')
    except Exception as e:
        logger.error(f'Failed downloading {url}:\n{e}')
        raise e
    finally:
        loop.close()
