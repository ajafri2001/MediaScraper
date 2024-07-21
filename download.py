import aiohttp
import asyncio
from tqdm import tqdm
import os
import mimetypes
from urllib.parse import urlparse
from aiofiles import open as aioopen
from aiohttp import ClientSession, TCPConnector
from asyncio import Queue, Semaphore
from dotenv import load_dotenv

load_dotenv()

CONCURRENT_DOWNLOADS = int(os.getenv('CONCURRENT_DOWNLOADS', '10'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
DOWNLOAD_DIR: str = os.getenv('DOWNLOAD_DIR', './downloads')


def get_filename(url, content_type):
    # Try to get the filename from the URL.
    parsed_url = urlparse(url)
    path = parsed_url.path
    filename = os.path.basename(path)

    if not filename or "." not in filename:
        ext = mimetypes.guess_extension(
            content_type
        )  # My first approach to avoid downloading bin files keeping it here just in case, Don't need it though until I branch to differnet file types
        if ext:
            filename = f"file{ext}"
        else:
            filename = "file.bin"

    return filename


async def fetch(session, url, semaphore, retries=0):
    async with semaphore:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.read()
                    content_type = response.headers.get("Content-Type", "").split(";")[
                        0
                    ]
                    filename = get_filename(url, content_type)
                    return content, filename
                else:
                    raise aiohttp.ClientResponseError(
                        response.request_info, response.history, status=response.status
                    )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if retries < MAX_RETRIES:
                await asyncio.sleep(
                    2**retries
                )  # logic to wait more as the number of retries increase
                return await fetch(session, url, semaphore, retries + 1)
            else:
                print(f"Failed to download {url}: {str(e)}")
                return None, None


async def download_worker(queue, session, semaphore, pbar):  # Creating Worker Tasks
    while True:  # Will go on until queue is empty
        url, i = await queue.get()
        content, filename = await fetch(session, url, semaphore)
        if content and filename:
            safe_filename = "".join(
                c for c in filename if c.isalnum() or c in "._-"
            )  # Cleaning File name and adding numbering
            full_path = os.path.join(DOWNLOAD_DIR, f"{i}_{safe_filename}")
            async with aioopen(full_path, "wb") as f:
                await f.write(content)
        pbar.update(1)
        queue.task_done()


async def download_all(urls):
    queue = Queue()
    semaphore = Semaphore(CONCURRENT_DOWNLOADS)
    connector = TCPConnector(limit=CONCURRENT_DOWNLOADS)

    async with ClientSession(connector=connector) as session:
        pbar = tqdm(total=len(urls), desc="Downloading")
        workers = [
            asyncio.create_task(download_worker(queue, session, semaphore, pbar))
            for _ in range(CONCURRENT_DOWNLOADS)
        ]

        for i, url in enumerate(urls):
            await queue.put((url, i))

        await queue.join()
        pbar.close()

        for worker in workers:
            worker.cancel()

        await asyncio.gather(*workers, return_exceptions=True)


async def read_urls_from_file(file_path):
    urls = []
    async with aioopen(file_path, "r") as f:
        async for line in f:
            url = line.strip()
            if url:
                urls.append(url)
    return urls


async def main():
    file_path = "./downloads/fetched-urls.txt"
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return

    urls = await read_urls_from_file(file_path)
    if not urls:
        print("No URLs found in the file.")
        return

    # Create the download directory if it doesn't exist
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    print(f"Found {len(urls)} URLs. Starting download...")
    await download_all(urls)


if __name__ == "__main__":
    asyncio.run(main())
