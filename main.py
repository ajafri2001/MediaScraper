import time
import os
from collections import deque
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib import robotparser
from dotenv import load_dotenv

load_dotenv()


def crawler(start_url, max_depth):
    queue = deque([(start_url, 0)])  # (url, depth)
    visited = set([start_url])
    total_time = 0.0
    total_links = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    session = requests.Session()
    robot_parsers = {}

    # Ensure the downloads directory exists
    os.makedirs("./downloads", exist_ok=True)

    # Open the file to log URLs
    with open("./downloads/fetched-urls.txt", "w") as url_file:
        while queue:
            url, depth = queue.popleft()
            if depth > max_depth:
                continue

            parsed_url = urlparse(url)
            domain = f"{parsed_url.scheme}://{parsed_url.netloc}"

            if domain not in robot_parsers:
                rp = robotparser.RobotFileParser()
                rp.set_url(f"{domain}/robots.txt")
                rp.read()
                robot_parsers[domain] = rp

            rp = robot_parsers[domain]

            if not rp.can_fetch(headers["User-Agent"], url):
                print(f"Robots.txt disallows crawling: {url}")
                continue

            try:
                start_time = time.time()
                response = session.get(
                    url, headers=headers, timeout=10
                )  # 10 second session
                response.raise_for_status()
                end_time = time.time()
                elapsed_time = end_time - start_time
                total_time += elapsed_time
                soup = BeautifulSoup(response.content, "lxml")

                # Process images
                for img in soup.find_all("img", src=True):
                    img_url = urljoin(url, img["src"])
                    print(f"Image found: {img_url}")
                    # Log the image URL to the file
                    url_file.write(f"{img_url}\n")

                # Traversing Logic
                for a in soup.find_all("a", href=True):
                    link = urljoin(url, a["href"])
                    if link.startswith("http") and link not in visited:
                        total_links += 1
                        visited.add(link)
                        if depth < max_depth:
                            queue.append((link, depth + 1))

            except requests.RequestException as e:
                print(f"Error crawling {url}: {e}")
                pass

            crawl_delay = rp.crawl_delay(headers["User-Agent"])
            if crawl_delay:
                time.sleep(crawl_delay)

    print(f"Total time taken for crawling: {total_time:.2f} seconds")
    print(f"Total number of links found: {total_links}")


start_url = os.getenv("URL")
max_depth : int = int(os.getenv("MAX_DEPTH", "0"))
crawler(start_url, max_depth)
