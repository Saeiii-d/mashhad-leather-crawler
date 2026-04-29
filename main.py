"""
Mashhad Leather Product Crawler
================================

This script is designed to crawl 'mashadleather.com' to extract comprehensive
product data. It utilizes the `crawlee` library for asynchronous web crawling
and `aiohttp` for fetching dynamic pricing and stock information via internal
API endpoints.

Features:
    - Extraction of basic product info (SKU, Title, Description, Category).
    - Collection of Main and Gallery images.
    - Handling of product variants (Color, Size) including dynamic price/stock
      retrieval via AJAX/API calls.
    - Pagination handling for category pages.
    - Robust error handling and logging via `loguru`.

Usage:
    Run this script directly to start the crawl. Ensure all dependencies
    (crawlee, aiohttp, loguru, rich) are installed.

    -- pip install crawlee aiohttp loguru rich
"""

import re
import sys
import os
import asyncio
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse, urlunparse, urljoin, parse_qs, urlencode
from datetime import datetime
from dataclasses import dataclass, field, asdict

import aiohttp
from loguru import logger
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.storages import Dataset, KeyValueStore
from crawlee.router import Router

# Initialize Rich Console for pretty printing to the terminal
console = Console()

# -----------------------------------------------------------------------------
# Data Models
# -----------------------------------------------------------------------------
@dataclass
class ProductVariant:
    """
    Represents a specific variant of a product (e.g., Red, Size L).
    
    Attributes:
        variant_sku: Unique identifier for this specific variant.
        quantity: Stock quantity available.
        price: Original price string.
        discounted_price: Sale price string, if applicable.
        size: Size attribute (e.g., 'M', '42').
        color: Color attribute (e.g., 'Black', 'Red').
    """
    variant_sku: str
    quantity: Optional[int] = None
    price: Optional[str] = None
    discounted_price: Optional[str] = None
    size: Optional[str] = None
    color: Optional[str] = None

@dataclass
class CrawledData:
    """
    Represents the full data structure for a single crawled product.
    
    Attributes:
        sku: Product Stock Keeping Unit.
        title: Product title.
        ts: Timestamp of the crawl.
        url: Canonical URL of the product.
        source: Source website identifier.
        is_synced: Flag indicating if data has been synced to external DB.
        listing_image: Thumbnail image URL from the listing page.
        images: List of full-resolution gallery image URLs.
        category: Product category name.
        description: HTML-cleaned product description.
        variants: List of ProductVariant objects.
    """
    sku: str
    title: str
    ts: datetime
    url: str
    source: str
    is_synced: bool = False
    listing_image: Optional[str] = None
    images: List[str] = field(default_factory=list)
    category: Optional[str] = None
    description: Optional[str] = None
    variants: List[ProductVariant] = field(default_factory=list)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def remove_query_params(url: str) -> str:
    """
    Removes query parameters from a URL to ensure consistent storage keys.
    
    Args:
        url: The raw URL string.
        
    Returns:
        The URL without query parameters.
    """
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, "", ""))

# -----------------------------------------------------------------------------
# Main Crawler Class
# -----------------------------------------------------------------------------
class MashhadLeather:
    """
    Crawler for Mashhad Leather website (mashadleather.com).
    
    This class manages the crawling lifecycle, including routing, data extraction,
    and storage of product information. It handles static HTML parsing for
    basic details and asynchronous HTTP requests for dynamic pricing/stock data.
    """
    
    # Configuration dictionary. Edit this to change behavior or selectors.
    CONFIG: Dict[str, Any] = {
        "max_requests": 10000,          # Limit total pages crawled to prevent bans/overload
        "site_name": "mashadleather",   # Used for log file naming and source identification
        "start_urls": ["https://www.mashadleather.com/"],
        
        "selectors": {
            # CSS Selectors for navigation and content extraction
            "category_links": ".mega-menu-submenu-toggler li:not(.mega-menu-title) > a",
            "detail_links": "div.product-image a:first-of-type",
            "sku": "div.product-title h1 a",
            "title": "div.product-title h1 a",
            "images": "div.col-md-6.gallery-shell div.product-image a img",
            "category": "li.breadcrumb-item.active a",
            "description": "div.accordion div.accordion-content",
            "price": 'div.product-description div.product-price ins',
            "main_price": 'div.product-description div.product-price del',
            "discounted_price": 'div.product-description div.product-price ins',
            "total_page": 'div.pagination.pagination-simple ul li:last-child a',
            "color_option": 'div#color-picker',
            "size_options": 'ul.product-size'
        },
        
        "dataset_name": "mashadleather_dataset",
        "kv_store_name": "mashadleather_kv_store",
    }

    def __init__(self, mode: str = "crawl"):
        """
        Initialize the Crawler.
        
        Args:
            mode: Operational mode (e.g., 'crawl', 'update').
        """
        self.mode = mode
        self.router = Router[BeautifulSoupCrawlingContext]()
        self.dataset: Optional[Dataset] = None
        self.kv_store: Optional[KeyValueStore] = None
        
        # Cache for listing images 
        self.listing_image_cache: Dict[str, str] = {}
        self.base_url = "https://www.mashadleather.com"
        
        self._setup_logger()
        self._register_routes()

    def _setup_logger(self) -> None:
        """
        Configures Loguru logger to save logs to a file and print to console.
        Ensures the logs directory exists.
        """
        logger.remove()
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        
        # Log to file with colorization and detailed format
        logger.add(
            f"{log_dir}/{self.CONFIG['site_name']}.log",
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
            level="DEBUG",
            colorize=True,
            rotation="10 MB" # Rotate log files to prevent them from getting too large
        )
        
        # Log to stderr (console)
        logger.add(
            sys.stderr, 
            format="{time} | {level} | {message}", 
            level="INFO", 
            colorize=True
        )

    def _register_routes(self) -> None:
        """
        Registers the router handlers for different request types:
        1. START: Finds category links and enqueues them.
        2. CATEGORY: Finds product links on category pages and enqueues them.
        3. DETAIL: Extracts full product data.
        """
        if self.mode == "crawl":
            # -------------------------------------------------------------
            # Route 1: Start Page / Homepage
            # -------------------------------------------------------------
            @self.router.default_handler
            async def start_handler(context: BeautifulSoupCrawlingContext) -> None:
                logger.info(f"Starting crawl from: {context.request.url}")
                sel = self.CONFIG["selectors"]["category_links"]
                try:
                    # Enqueue all category links found on the start page
                    await context.enqueue_links(selector=sel, label="CATEGORY", unique=True)
                except Exception as e:
                    logger.warning(f"Could not find category links on {context.request.url}: {e}")

            # -------------------------------------------------------------
            # Route 2: Category Pages
            # -------------------------------------------------------------
            @self.router.handler("CATEGORY")
            async def category_handler(context: BeautifulSoupCrawlingContext) -> None:
                logger.info(f"Processing category: {context.request.url}")
                
                # Handle Pagination
                last_page_sel = self.CONFIG["selectors"].get("total_page")
                last_page_el = context.soup.select_one(last_page_sel) if last_page_sel else None
                last_page_num = int(last_page_el.get_text(strip=True)) if last_page_el else 1
                
                if last_page_num > 1:
                    logger.info(f"Category '{context.request.url}' has pagination with {last_page_num} pages")
                    
                    parsed_url = urlparse(context.request.url)
                    query_params = parse_qs(parsed_url.query)
                    pagination_urls = []
                    
                    for page in range(1, last_page_num + 1):
                        query_params["pageid"] = [str(page)]
                        new_query = urlencode(query_params, doseq=True)
                        pagination_urls.append(f"{self.base_url}?{new_query}")
                    
                    try:
                        # Create a temporary container to hold pagination links for enqueueing
                        temp_div = context.soup.new_tag('div', **{'class': 'temp-pagination-links'})
                        for url in pagination_urls:
                            link = context.soup.new_tag('a', href=url)
                            link.string = f"Page {url.split('pageid=')[-1]}"  
                            temp_div.append(link)
                        
                        context.soup.body.append(temp_div)
                        
                        await context.enqueue_links(
                            selector='div.temp-pagination-links a',
                            label="CATEGORY"
                        )
                        
                        temp_div.decompose()
                        logger.info(f"Enqueued {len(pagination_urls)} pagination URLs")
                    except Exception as e:
                        logger.error(f"Failed to enqueue pagination URLs: {e}")

                # Extract Product Links
                sel = self.CONFIG["selectors"]["detail_links"]
                all_product_links = context.soup.select(sel)
                
                for link_el in all_product_links:
                    img_el = link_el.select_one("img")
                    if img_el:
                        # Get main image source (supports lazy loading data-src)
                        listing_image = img_el.get("src") or img_el.get("data-src")
                        
                        if listing_image:
                            # Convert relative URLs to absolute
                            listing_image = urljoin(self.base_url, listing_image)
                            
                            # Get product URL from href, data-url, or data-href
                            product_url = link_el.get("href") or link_el.get("data-url") or link_el.get("data-href")
                            
                            if product_url:
                                product_url = urljoin(self.base_url, product_url)
                                # Cache the listing image for potential future use
                                self.listing_image_cache[product_url] = listing_image
                
                try:
                    # Enqueue all product detail pages found in this category
                    await context.enqueue_links(selector=sel, label="DETAIL", unique=True)
                except Exception as e:
                    logger.warning(f"Could not enqueue detail links: {e}")

            # -------------------------------------------------------------
            # Route 3: Product Detail Pages
            # -------------------------------------------------------------
            @self.router.handler("DETAIL")
            async def detail_handler(context: BeautifulSoupCrawlingContext) -> None:
                logger.info(f"Processing product: {context.request.url}")
                try:
                    # Extract structured data from the page
                    data = await self._extract_product_data(context)
                    
                    # Save to Crawlee Dataset
                    await self.dataset.push_data(asdict(data))
                    
                    # Save metadata to Key-Value Store (useful for status tracking)
                    await self.kv_store.set_value(data.sku, {
                        "status": "crawled", 
                        "url": data.url,
                        "timestamp": str(datetime.utcnow())
                    })
                    
                    logger.info(f"Saved SKU: {data.sku} with {len(data.variants)} variants")
                    
                except Exception as e:
                    logger.error(f"Error processing product {context.request.url}: {e}", exc_info=True)

    async def _extract_product_data(self, context: BeautifulSoupCrawlingContext) -> CrawledData:
        """
        Extracts all static and dynamic data from a product page.
        
        Args:
            context: The Crawlee crawling context containing the parsed HTML.
            
        Returns:
            A CrawledData object populated with product details.
            
        Raises:
            Exception: If critical data extraction fails.
        """
        soup = context.soup
        product_url = context.request.url
        
        # --- 1. Extract SKU ---
        sku = None
        # Method A: Regex from URL
        sku_match = re.search(r'/product-detail/(\d+)', product_url)
        if sku_match:
            sku = sku_match.group(1)
        
        # Method B: Meta Tag
        if not sku:
            meta_sku = soup.select_one('meta[name="product-sku"]')
            if meta_sku:
                sku = meta_sku.get('content', '').strip()
        
        # Method C: Fallback to text extraction
        if not sku:
            sku_el = soup.select_one(self.CONFIG["selectors"]["sku"])
            if sku_el:
                txt = sku_el.get_text(strip=True)
                parts = txt.split()
                # Assume the last alphanumeric part is the SKU
                if parts and re.match(r'^[A-Za-z0-9\-]+$', parts[-1]):
                    sku = parts[-1]
                else:
                    sku = "unknown_" + str(datetime.now().timestamp())
            else:
                sku = "unknown_" + str(datetime.now().timestamp())
                logger.warning(f"SKU not found in URL, meta, or text for: {product_url}")

        # --- 2. Extract Title ---
        title_el = soup.select_one(self.CONFIG["selectors"]["title"])
        title = title_el.get_text(strip=True) if title_el else "No Title"
        
        # --- 3. Extract Listing Image (from cache populated in Category handler) ---
        listing_image = self.listing_image_cache.get(context.request.url, None)
        
        # --- 4. Extract All Gallery Images ---
        images = []
        images_set = set() # Use set to avoid duplicates
        image_els = soup.select(self.CONFIG["selectors"]["images"])
        for img_el in image_els:
            img_src = img_el.get("src")
            if img_src:
                # Normalize URL to absolute
                images_set.add(urljoin(self.base_url, img_src))
        images = list(images_set)
        
        # --- 5. Extract Category ---
        category = ''
        category_el = soup.select_one(self.CONFIG["selectors"]["category"])
        if category_el:
            category = category_el.get_text(strip=True)
            
        # --- 6. Extract Description ---
        description = ''
        description_el = soup.select_one(self.CONFIG["selectors"]["description"])
        if description_el:
            # Replace <br> tags with newlines for cleaner text
            for br in description_el.find_all('br'):
                br.replace_with('\n')
            description = description_el.get_text(separator="\n", strip=True)
            # Clean up multiple newlines
            description = re.sub(r'\n\s*\n', '\n\n', description)
            
        # --- 7. Extract Variants (Dynamic Data) ---
        variants = await self.extract_variants(context, sku)
        
        return CrawledData(
            sku=sku,
            title=title,
            ts=datetime.utcnow(),
            url=remove_query_params(product_url),
            source=self.CONFIG["site_name"],
            listing_image=listing_image,
            images=images,
            category=category,
            description=description,
            variants=variants
        )

    async def extract_variants(self, context: BeautifulSoupCrawlingContext, sku: str) -> List[ProductVariant]:
        """
        Extracts variants by checking for color/size selectors.
        If dynamic selection exists, it calls the backend API to get specific prices/stocks.
        
        Args:
            context: The Crawlee crawling context.
            sku: The base product SKU.
            
        Returns:
            A list of ProductVariant objects.
        """
        variants: List[ProductVariant] = []
        soup = context.soup
        
        # Use a single aiohttp session for all API calls in this product to reuse connections
        async with aiohttp.ClientSession() as session:
            color_option = soup.select_one(self.CONFIG["selectors"]["color_option"])
            size_options = soup.select_one(self.CONFIG["selectors"]["size_options"])
            
            # Case 1: Simple Product (No color/size selector)
            if not color_option:
                variant = self._process_simple_product(soup, sku)
                if variant:
                    variants.append(variant)
                return variants
                
            # Case 2: Product with Color Options
            color_labels = color_option.select("label input") if color_option else []
            if not color_labels:
                # Fallback if selector is present but empty
                variant = self._process_simple_product(soup, sku)
                if variant:
                    variants.append(variant)
                return variants
                
            # Iterate through each color option
            for inp in color_labels:
                try:
                    color_variants = await self._process_color(inp, sku, size_options, session)
                    if color_variants:
                        variants.extend(color_variants)
                except Exception as e:
                    logger.warning(f"Failed to process color variant: {e}")
                    # If processing fails, try fallback simple product only if no variants exist yet
                    if not variants:
                        variant = self._process_simple_product(soup, sku)
                        if variant:
                            variants.append(variant)
            
            # Final fallback if no variants were found at all
            if not variants:
                variant = self._process_simple_product(soup, sku)
                if variant:
                    variants.append(variant)
                    
        return variants        

    async def _get_price_async(self, session: aiohttp.ClientSession, product_id: str, color_id: str) -> Dict[str, str]:
        """
        Fetches price and discounted price from the website's internal API.
        
        Args:
            session: Active aiohttp ClientSession.
            product_id: The product ID.
            color_id: The selected color ID.
            
        Returns:
            A dictionary containing 'price' and 'discountPrice' strings.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
        try:
            # Use timeout to prevent hanging on slow responses
            async with session.post(
                "https://www.mashadleather.com/Products/ChangePriceByColor",
                headers=headers,
                data={"id": product_id, "colorId": color_id},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"API returned status {response.status} for product {product_id}")
                    return {}
        except Exception as e:
            logger.warning(f"Error fetching price for product {product_id}: {e}")
            return {}

    async def _get_sizes_async(self, session: aiohttp.ClientSession, color_id: str, product_id: str) -> list:
        """
        Fetches available sizes and stock quantities for a specific color.
        
        Args:
            session: Active aiohttp ClientSession.
            color_id: The selected color ID.
            product_id: The product ID.
            
        Returns:
            A list of dictionaries containing size details.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        try:
            async with session.get(
                f"https://www.mashadleather.com/Products/GetSizesForColor?colorId={color_id}&productId={product_id}",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"API returned status {response.status} for sizes")
                    return []
        except Exception as e:
            logger.warning(f"Error fetching sizes: {e}")
            return []

    async def _process_color(self, inp, sku: str, size_options, session: aiohttp.ClientSession) -> List[ProductVariant]:
        """
        Processes a single color option to generate variants based on available sizes.
        
        Args:
            inp: The HTML element representing the color input.
            sku: Base product SKU.
            size_options: HTML element for size options (if any).
            session: Active aiohttp ClientSession.
            
        Returns:
            A list of ProductVariant objects for this color.
        """
        variants = []
        color_id = inp.get("id")
        product_id = inp.get("data-model-id")
        color_name = inp.get("data-selected-color-title") or ""
        
        # Clean color name for use in SKU
        safe_color = color_name.replace(" ", "_").replace("(", "").replace(")", "")
        
        if not color_id or not product_id:
            return variants 
        
        # --- Scenario A: Product has Sizes (Dynamic Fetch) ---
        if size_options:
            try:
                # Fetch available sizes for this color
                size_resp = await self._get_sizes_async(session, color_id, product_id)
                
                if not size_resp or not isinstance(size_resp, list):
                    logger.warning(f"Skipping color '{color_name}' (failed to fetch sizes)")
                    return variants
                
                # For each size, fetch the specific price/stock
                for size in size_resp:
                    # Note: The API might return price for the whole color, 
                    # or specific size. Here we assume price is per color but stock is per size.
                    price_data = await self._get_price_async(session, product_id, color_id)
                    
                    variants.append(ProductVariant(
                        variant_sku=f"{sku}_{size.get('Title', 'Unknown')}_{safe_color}",
                        size=size.get("Title") or "",
                        color=color_name,
                        quantity=int(size.get("Quantity", 0) or 0),
                        price=price_data.get("price") or "",
                        discounted_price=price_data.get("discountPrice") or "",
                    ))
            except Exception as e:
                logger.warning(f"Skipping color '{color_name}' due to error: {e}")
            return variants
            
        # --- Scenario B: No Sizes, Just Color ---
        else:
            price_data = await self._get_price_async(session, product_id, color_id)
            
            # Check if the input element is disabled (out of stock)
            is_disabled = "disabled" in inp.attrs
            quantity = 0 if is_disabled else 1
            
            variants.append(ProductVariant(
                variant_sku=f"{sku}_{safe_color}",
                size="",
                color=color_name,
                quantity=quantity,
                price=price_data.get("price") or "",
                discounted_price=price_data.get("discountPrice") or "",
            ))
            return variants
    
    def _process_simple_product(self, soup, sku: str) -> Optional[ProductVariant]:
        """
        Handles products that do not have dynamic variants (no color/size selection).
        Extracts price directly from the HTML.
        
        Args:
            soup: BeautifulSoup object of the product page.
            sku: Product SKU.
            
        Returns:
            A ProductVariant object or None if data cannot be extracted.
        """
        price_el = soup.select_one(self.CONFIG["selectors"]["price"]) if self.CONFIG["selectors"].get("price") else None
        discounted_el = soup.select_one(self.CONFIG["selectors"]["discounted_price"]) if self.CONFIG["selectors"].get("discounted_price") else None
        
        price = price_el.get_text(strip=True) if price_el else ""
        discounted_price = discounted_el.get_text(strip=True) if discounted_el else ""
        
        # Logic to handle cases where discounted price is shown but original isn't
        if discounted_price and not price:
            price = discounted_price
            discounted_price = ""
        elif price and discounted_price and price == discounted_price:
            # If prices are same, treat as non-discounted
            discounted_price = ""
            
        # Check stock quantity if available
        txt_count = soup.select_one("#txtCount")
        quantity = 1 if txt_count else 0 # Default to 1 if count not found
        
        return ProductVariant(
            variant_sku=sku,
            size="",
            color="",
            quantity=quantity,
            price=price,
            discounted_price=discounted_price,
        )

    async def run(self) -> None:
        """
        Main entry point: Initializes datasets and runs the crawler.
        """
        try:
            # Open Crawlee storage entities
            self.dataset = await Dataset.open(name=self.CONFIG["dataset_name"])
            self.kv_store = await KeyValueStore.open(name=self.CONFIG["kv_store_name"])
            
            # Print Config Summary
            table = Table(title="Crawler Config")
            table.add_column("Key", style="cyan")
            table.add_column("Value", style="green")
            table.add_row("Site", self.CONFIG["site_name"])
            table.add_row("Mode", self.mode)
            table.add_row("Max Requests", str(self.CONFIG["max_requests"]))
            console.print(Panel(table, title="Start", border_style="blue"))
            
            # Initialize and run the crawler
            crawler = BeautifulSoupCrawler(
                max_requests_per_crawl=self.CONFIG["max_requests"],
                request_handler=self.router
            )
            
            logger.info("Starting crawler...")
            await crawler.run(self.CONFIG["start_urls"])
            
            logger.info("Crawling finished successfully.")
            
        except Exception as e:
            console.print(Panel(f"[red]Error: {e}[/red]", title="Error", border_style="red"))
            logger.error(f"Crawler failed: {e}", exc_info=True)

if __name__ == "__main__":
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)
    
    # Create and run crawler
    crawler = MashhadLeather(mode="crawl")
    asyncio.run(crawler.run())