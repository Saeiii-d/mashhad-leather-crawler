# Mashhad Leather Product Crawler

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Crawlee](https://img.shields.io/badge/Powered%20by-Crawlee-green.svg)](https://crawlee.dev/)

An asynchronous web crawler built with **Crawlee** and **aiohttp** designed to extract comprehensive product data from **Mashhad Leather**. It handles static HTML parsing for basic details and dynamic API calls for real-time pricing and stock levels.

## Features

- **Asynchronous Crawling**: Built on `crawlee` for high-performance, concurrent scraping.
- **Dynamic Data Extraction**: Intercepts internal AJAX/API calls to fetch real-time prices and stock quantities for product variants (color/size).
- **Image Handling**: Extracts main listing images and full gallery images, handling lazy-loading attributes.
- **Pagination Support**: Automatically detects and crawls paginated category pages.
- **Structured Storage**: Saves data to `Dataset` (JSON/Parquet) and metadata to `KeyValueStore`.
- **Robust Logging**: Detailed logging via `loguru` with file rotation and console output.
- **Error Handling**: Graceful fallbacks for missing data or API failures.

## Installation

### Prerequisites
- Python 3.9 or higher

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-username/mashhad-leather-crawler.git
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

   You can adjust the crawler behavior by editing the `CONFIG` dictionary in `main.py` or by providing a `config.json` or `config.yaml`.
   
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
   If `config.json` or `config.yaml` exists, it will override or extend these defaults.

## Project Structure
   ```text
   ├── main.py              # Main crawler script and logic
   ├── requirements.txt     # Python dependencies
   ├── LICENSE
   └── README.md
   ```

## Tech Stack

   - **Language**: Python 3.9+
   - **Crawling Framework**: Crawlee (Python)
   - **HTTP Client**: aiohttp (for async API requests)
   - **HTML Parsing**: BeautifulSoup4
   - **Logging**: loguru
   - **CLI**: rich (for pretty terminal output)

## Author
   - **GitHub**: https://github.com/Saeiii-d
   - **LinkedIn**: https://ir.linkedin.com/in/saeid-khazaei-a14b52406

## License

   This project is licensed under the MIT License.  
   Copyright (c) 2026 Saeid Khazaei  
   Shahid Beheshti University (SBU)

   See the [LICENSE](./LICENSE.txt) file for details.