# Mashhad Leather Product Crawler

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Crawlee](https://img.shields.io/badge/Powered%20by-Crawlee-green.svg)](https://crawlee.dev/)

An asynchronous web crawler built with **Crawlee** and **aiohttp** to extract structured product data from **Mashhad Leather**. It combines static HTML parsing for product metadata with direct API requests for current variant pricing and, where available, size and stock-quantity data.

## Features

- **Asynchronous Crawling**: Built on `crawlee` for asynchronous and concurrent crawling.
- **Dynamic Variant Extraction**: Sends direct asynchronous requests to internal API endpoints to retrieve current variant prices and, when exposed by the site, size and stock-quantity data.
- **Image Handling**: Extracts main listing images and full gallery images, handling lazy-loading attributes.
- **Pagination Support**: Automatically detects and crawls paginated category pages.
- **Structured Storage**: Saves extracted records to a Crawlee Dataset and crawl metadata to a `KeyValueStore`.
- **Robust Logging**: Detailed logging via `loguru` with file rotation and console output.
- **Error Handling**: Graceful fallbacks for missing data or API failures.

## Installation

### Prerequisites

- Python 3.10 or higher

### Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/Saeiii-d/mashhad-leather-crawler
   cd mashhad-leather-crawler
   ```

2. **Create a virtual environment**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

- Run the crawler from the terminal:

  ```bash
  python main.py
  ```

  By default, results are stored in Crawlee’s `Dataset` and `KeyValueStore` (e.g., under a `storage` directory, depending on your config).
  You can limit the crawl scope using `max_requests` in the configuration (see below) for testing.

### Configuration

You can adjust the crawler behavior by editing the CONFIG dictionary in main.py.

```python
CONFIG = {
     "max_requests": n,
     "site_name": "site-name",
     "start_urls": ["https://example.com/"],

     "selectors": {
         # CSS Selectors for navigation and content extraction
         "category_links": "",
         "detail_links": "",
         "sku": "",
         "title": "",
         # you can add or remove things ...
     },

     "dataset_name": "_dataset",
     "kv_store_name": "_kv_store",
 }
```

## Project Structure

```text
├── main.py              # Main crawler script and logic
├── requirements.txt     # Python dependencies
├── LICENSE.txt
└── README.md
```

## 🛠️ System Architecture

The crawler uses a layered extraction strategy to separate page discovery, static HTML parsing, and variant-specific API requests:

- **Discovery Layer**: Utilizes Crawlee's `BeautifulSoupCrawler` and custom `Router` logic to efficiently navigate paginated categories and enqueue product URLs.
- **Static Extraction**: Parses the DOM for product metadata such as SKUs, titles, descriptions, categories, and images using CSS selectors.
- **Direct API Requests**: Uses `aiohttp` to request data from internal endpoints such as `ChangePriceByColor` and `GetSizesForColor`.
- **Observability**: Uses `loguru` for file-rotated logs and console logging, while `rich` provides formatted configuration summaries and error panels in the terminal.

## Tech Stack

- **Language**: Python 3.10 or higher
- **Crawling Framework**: Crawlee (Python)
- **HTTP Client**: aiohttp (for async API requests)
- **HTML Parsing**: BeautifulSoup4
- **Logging**: loguru
- **CLI**: rich (for pretty terminal output)

## Related Article

For a detailed explanation of the crawler's architecture, API workflow, design decisions, and limitations, see:

**[Combining HTML Crawling with Direct API Requests for Product Variants](https://medium.com/@saeiiid.khazaei/building-a-high-performance-hybrid-web-crawler-with-api-interception-3d3e357da55b)**

## Author

- **GitHub**: https://github.com/Saeiii-d
- **LinkedIn**: https://www.linkedin.com/in/saeidkhazaei/

## License

This project is licensed under the MIT License.  
 Copyright (c) 2026 Saeid Khazaei  
 Shahid Beheshti University (SBU)

See the [LICENSE](./LICENSE.txt) file for details.
