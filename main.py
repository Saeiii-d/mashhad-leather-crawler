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
    (crawlee, aiohttp, loguru, rich, beautifulsoup4) are installed.

    -- pip install crawlee aiohttp loguru rich beautifulsoup4
"""

import re
import sys
import os
import asyncio
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse, urlunparse, urljoin, parse_qs, urlencode
from datetime import datetime
from dataclasses import dataclass, field, asdict
import hashlib

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
            "images": "div.gallery-shell img, div.gallery-shell source, div.product-gallery img, div.product-gallery source, div.product-images img, div.product-images source",
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
                primary_selector = self.CONFIG["selectors"]["category_links"]
                category_links = context.soup.select(primary_selector)

                if category_links:
                    logger.info(
                        f"Found {len(category_links)} category links "
                        f"with the primary selector"
                    )
                    try:
                        await context.enqueue_links(
                            selector=primary_selector,
                            label="CATEGORY",
                            unique=True,
                        )
                    except Exception as e:
                        logger.warning(
                            f"Could not enqueue category links on "
                            f"{context.request.url}: {e}"
                        )
                    return

                logger.warning(
                    "Primary category selector found no links; "
                    "trying URL-pattern fallback."
                )

                category_urls: List[str] = []
                seen_urls = set()
                expected_host = urlparse(self.base_url).netloc

                for anchor in context.soup.select('a[href*="/category/"]'):
                    href = anchor.get("href")
                    if not href:
                        continue

                    absolute_url = urljoin(self.base_url, href)
                    parsed = urlparse(absolute_url)

                    if parsed.netloc != expected_host:
                        continue

                    path = parsed.path.rstrip("/")
                    if not path.startswith("/category/"):
                        continue

                    last_part = path.split("/")[-1]
                    if not last_part or last_part.isdigit():
                        continue

                    normalized = remove_query_params(absolute_url).rstrip("/")
                    if normalized not in seen_urls:
                        seen_urls.add(normalized)
                        category_urls.append(normalized)

                logger.info(
                    f"Found {len(category_urls)} category links "
                    f"with the fallback"
                )

                if not category_urls or not context.soup.body:
                    logger.error("No category links were found on the start page.")
                    return

                temp_div = context.soup.new_tag(
                    "div",
                    **{"class": "temp-category-links"},
                )
                for url in category_urls:
                    temp_div.append(context.soup.new_tag("a", href=url))

                context.soup.body.append(temp_div)
                try:
                    await context.enqueue_links(
                        selector="div.temp-category-links a",
                        label="CATEGORY",
                        unique=True,
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not enqueue fallback category links: {e}"
                    )
                finally:
                    temp_div.decompose()

            # -------------------------------------------------------------
            # Route 2: Category Pages
            # -------------------------------------------------------------
            @self.router.handler("CATEGORY")
            async def category_handler(context: BeautifulSoupCrawlingContext) -> None:
                logger.info(f"Processing category: {context.request.url}")

                # Pagination
                last_page_sel = self.CONFIG["selectors"].get("total_page")
                last_page_el = context.soup.select_one(last_page_sel) if last_page_sel else None
                last_page_num = 1

                if last_page_el:
                    try:
                        last_page_num = int(last_page_el.get_text(strip=True))
                    except (TypeError, ValueError):
                        logger.warning(f"Could not parse last page number on {context.request.url}")

                parsed_url = urlparse(context.request.url)
                query_params = parse_qs(parsed_url.query)
                current_page_raw = query_params.get("pageid", ["1"])[0]
                current_page = int(current_page_raw) if str(current_page_raw).isdigit() else 1

                if last_page_num > 1 and current_page == 1:
                    pagination_urls = []
                    for page in range(2, last_page_num + 1):
                        page_params = dict(query_params)
                        page_params["pageid"] = [str(page)]
                        new_query = urlencode(page_params, doseq=True)
                        pagination_urls.append(parsed_url._replace(query=new_query).geturl())

                    if pagination_urls and context.soup.body:
                        temp_div = context.soup.new_tag("div", **{"class": "temp-pagination-links"})
                        for url in pagination_urls:
                            temp_div.append(context.soup.new_tag("a", href=url))
                        context.soup.body.append(temp_div)
                        try:
                            await context.enqueue_links(
                                selector="div.temp-pagination-links a",
                                label="CATEGORY",
                                unique=True,
                            )
                            logger.info(f"Enqueued {len(pagination_urls)} pagination URLs")
                        except Exception as e:
                            logger.warning(f"Failed to enqueue pagination URLs: {e}")
                        finally:
                            temp_div.decompose()

                # Product links
                primary_selector = self.CONFIG["selectors"]["detail_links"]
                product_links = context.soup.select(primary_selector)
                selector_to_enqueue = primary_selector
                temp_product_div = None

                if not product_links:
                    logger.warning(
                        f"Primary product selector found no products on {context.request.url}; trying URL fallback."
                    )
                    found_urls = []
                    seen_urls = set()
                    for anchor in context.soup.select('a[href*="/category/"]'):
                        href = anchor.get("href")
                        if not href:
                            continue
                        absolute_url = urljoin(self.base_url, href)
                        path = urlparse(absolute_url).path.rstrip("/")
                        last_part = path.split("/")[-1]
                        if last_part.isdigit() and absolute_url not in seen_urls:
                            seen_urls.add(absolute_url)
                            found_urls.append(absolute_url)

                    if found_urls and context.soup.body:
                        temp_product_div = context.soup.new_tag("div", **{"class": "temp-product-links"})
                        for url in found_urls:
                            temp_product_div.append(context.soup.new_tag("a", href=url))
                        context.soup.body.append(temp_product_div)
                        selector_to_enqueue = "div.temp-product-links a"
                        product_links = context.soup.select(selector_to_enqueue)

                logger.info(f"Found {len(product_links)} product links on {context.request.url}")

                # Cache listing images from category-page product cards.
                for anchor in context.soup.select('a[href*="/category/"]'):
                    href = anchor.get("href")
                    if not href:
                        continue

                    product_url = urljoin(self.base_url, href)
                    parsed_product_url = urlparse(product_url)
                    path = parsed_product_url.path.rstrip("/")
                    last_part = path.split("/")[-1]

                    if (
                        parsed_product_url.netloc
                        != urlparse(self.base_url).netloc
                        or not last_part.isdigit()
                    ):
                        continue

                    listing_image = self._find_product_image_near_anchor(anchor)
                    if listing_image:
                        cache_key = remove_query_params(
                            product_url
                        ).rstrip("/")
                        self.listing_image_cache[cache_key] = listing_image

                if product_links:
                    try:
                        await context.enqueue_links(
                            selector=selector_to_enqueue,
                            label="DETAIL",
                            unique=True,
                        )
                    except Exception as e:
                        logger.warning(f"Could not enqueue detail links: {e}")
                else:
                    logger.warning(f"No product links found on category: {context.request.url}")

                if temp_product_div:
                    temp_product_div.decompose()

            # -------------------------------------------------------------
            # Route 3: Product Detail Pages
            # -------------------------------------------------------------
            @self.router.handler("DETAIL")
            async def detail_handler(
                context: BeautifulSoupCrawlingContext,
            ) -> None:
                logger.info(f"Processing product: {context.request.url}")

                try:
                    data = await self._extract_product_data(context)
                    await self.dataset.push_data(asdict(data))
                except Exception:
                    logger.exception(
                        f"Failed to extract/store product "
                        f"{context.request.url}"
                    )
                    raise

                try:
                    await self.kv_store.set_value(
                        data.sku,
                        {
                            "status": "crawled",
                            "url": data.url,
                            "timestamp": str(datetime.utcnow()),
                        },
                    )
                except Exception:
                    logger.exception(
                        f"Could not store crawl metadata for SKU {data.sku}"
                    )

                logger.info(
                    f"Saved SKU: {data.sku} "
                    f"with {len(data.variants)} variants"
                )

    def _image_url_from_element(self, element) -> Optional[str]:
        """
        Return the best image URL exposed by an <img> or <source> element.
        """
        if not element:
            return None

        for attr in (
            "data-zoom-image",
            "data-large",
            "data-original",
            "data-lazy-src",
            "data-src",
            "src",
        ):
            value = element.get(attr)
            if value and isinstance(value, str):
                value = value.strip()
                if value and not value.startswith("data:"):
                    return urljoin(self.base_url, value)

        for attr in ("data-srcset", "srcset"):
            value = element.get(attr)
            if not value or not isinstance(value, str):
                continue

            candidates = []
            for item in value.split(","):
                url_part = item.strip().split(" ")[0].strip()
                if url_part and not url_part.startswith("data:"):
                    candidates.append(url_part)

            if candidates:
                return urljoin(self.base_url, candidates[-1])

        return None

    def _image_url_from_style(self, element) -> Optional[str]:
        """
        Extract a background-image URL from an element's inline style.
        """
        if not element:
            return None

        style = element.get("style")
        if not style or not isinstance(style, str):
            return None

        match = re.search(
            r"background-image\s*:\s*url\(\s*['\"]?([^'\")]+)['\"]?\s*\)",
            style,
            flags=re.IGNORECASE,
        )
        if not match:
            return None

        candidate = match.group(1).strip()
        if not candidate or candidate.startswith("data:"):
            return None

        return urljoin(self.base_url, candidate)

    def _is_product_image_url(self, url: str) -> bool:
        """
        Reject obvious badges, site chrome, placeholders, and after-sale media.
        """
        if not url:
            return False

        clean = url.split("?", 1)[0].lower()

        if not clean.endswith((".jpg", ".jpeg", ".png", ".webp", ".avif")):
            return False

        rejected_parts = (
            "/images/aftersale/",
            "/images/discount-lable/",
            "/uploads/customimage/",
            "/images/logo",
            "/images/icon",
            "spinner",
            "loading",
            "placeholder",
        )

        return not any(part in clean for part in rejected_parts)

    def _candidate_images_from_container(self, container) -> List[str]:
        """
        Collect valid image URLs from a small product/gallery container.
        """
        candidates: List[str] = []
        seen = set()

        def add(candidate: Optional[str]) -> None:
            if not candidate:
                return

            absolute = urljoin(self.base_url, candidate.strip())
            if (
                absolute not in seen
                and self._is_product_image_url(absolute)
            ):
                seen.add(absolute)
                candidates.append(absolute)

        if not container:
            return candidates

        for element in container.select("img, source"):
            add(self._image_url_from_element(element))
            add(self._image_url_from_style(element))

        for anchor in container.select("a[href]"):
            add(anchor.get("href"))
            add(self._image_url_from_style(anchor))

        for element in container.select('[style*="background-image"]'):
            add(self._image_url_from_style(element))

        add(self._image_url_from_style(container))
        return candidates

    def _find_product_image_near_anchor(self, anchor) -> Optional[str]:
        """
        Find the first valid product thumbnail around a category product link.
        """
        containers = [anchor]
        parent = anchor.parent
        for _ in range(4):
            if not parent:
                break
            containers.append(parent)
            parent = parent.parent

        for container in containers:
            candidates = self._candidate_images_from_container(container)
            if candidates:
                return candidates[0]

        return None

    def _extract_gallery_images(self, soup) -> List[str]:
        """
        Extract product-gallery images while preserving page order.
        """
        images: List[str] = []
        seen = set()

        def add(candidate: Optional[str]) -> None:
            if not candidate:
                return

            absolute = urljoin(self.base_url, candidate.strip())
            if (
                absolute not in seen
                and self._is_product_image_url(absolute)
            ):
                seen.add(absolute)
                images.append(absolute)

        for element in soup.select(self.CONFIG["selectors"]["images"]):
            add(self._image_url_from_element(element))
            add(self._image_url_from_style(element))

            parent_link = element.find_parent("a")
            if parent_link:
                add(parent_link.get("href"))

        for container in soup.select(
            "div.gallery-shell, "
            "div.product-gallery, "
            "div.product-images"
        ):
            for candidate in self._candidate_images_from_container(container):
                add(candidate)

        # Layout-change fallback: only explicit /Uploads/Product/ assets.
        # /Uploads/ProductCategory/ is intentionally excluded because the
        # current site also uses it for non-gallery instructional banners.
        if not images:
            for element in soup.select(
                "img, source, [style*='background-image']"
            ):
                candidate = (
                    self._image_url_from_element(element)
                    or self._image_url_from_style(element)
                )
                if not candidate:
                    continue

                clean = candidate.split("?", 1)[0].lower()
                if "/uploads/product/" in clean:
                    add(candidate)

        return images

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
        normalized_url = remove_query_params(product_url).rstrip("/")

        # --- 1. Extract Title ---
        title_el = soup.select_one(self.CONFIG["selectors"]["title"])
        title = title_el.get_text(strip=True) if title_el else "No Title"

        # --- 2. Extract SKU ---
        sku = None

        # Method A: Legacy URL pattern, kept for backward compatibility.
        sku_match = re.search(r"/product-detail/(\d+)", normalized_url)
        if sku_match:
            sku = sku_match.group(1)

        # Method B: Product SKU meta tag, if the site provides one.
        if not sku:
            meta_sku = soup.select_one('meta[name="product-sku"]')
            if meta_sku:
                candidate = meta_sku.get("content", "").strip()
                if candidate:
                    sku = candidate

        # Method C: Extract a SKU-like token from the product title.
        # Only accept alphanumeric tokens that contain at least one digit,
        # so words such as "Liquid" are not incorrectly treated as SKUs.
        if not sku:
            sku_el = soup.select_one(self.CONFIG["selectors"]["sku"])
            sku_text = sku_el.get_text(strip=True) if sku_el else title
            parts = sku_text.split()

            if parts:
                candidate = parts[-1]
                if re.fullmatch(r"(?=.*\d)[A-Za-z0-9-]+", candidate):
                    sku = candidate

        # Method D: Current Mashhad Leather product URLs end with a numeric
        # internal product ID. Use it as a stable fallback identifier.
        if not sku:
            path_id = urlparse(normalized_url).path.rstrip("/").split("/")[-1]
            if path_id.isdigit():
                sku = f"product_{path_id}"

        # Method E: Last-resort deterministic hash of the canonical URL.
        if not sku:
            fallback_hash = hashlib.sha256(
                normalized_url.encode("utf-8")
            ).hexdigest()[:16]
            sku = f"unknown_{fallback_hash}"
            logger.warning(f"SKU not found in URL, meta, or title for: {product_url}")

        # --- 3. Extract Listing Image (from cache populated in Category handler) ---
        listing_image = self.listing_image_cache.get(normalized_url)

        # --- 4. Extract All Gallery Images ---
        images = self._extract_gallery_images(soup)

        # If the category-page thumbnail was not cached, use the first real
        # product image as the listing image.
        if not listing_image and images:
            listing_image = images[0]

        # Conversely, if the page exposes only a listing image, do not leave
        # the product's images list empty.
        if listing_image and not images:
            if self._is_product_image_url(listing_image):
                images = [listing_image]

        logger.info(
            f"Extracted {len(images)} images for SKU {sku}"
            + (
                f" | listing image: {listing_image}"
                if listing_image
                else " | listing image: none"
            )
        )

        # --- 5. Extract Category ---
        category = ""
        category_el = soup.select_one(self.CONFIG["selectors"]["category"])
        if category_el:
            category = category_el.get_text(strip=True)

        # --- 6. Extract Description ---
        description = ""
        description_el = soup.select_one(self.CONFIG["selectors"]["description"])
        if description_el:
            # Replace <br> tags with newlines for cleaner text.
            for br in description_el.find_all("br"):
                br.replace_with("\n")

            description = description_el.get_text(separator="\n", strip=True)
            description = re.sub(r"\n\s*\n", "\n\n", description)

        # --- 7. Extract Variants (Dynamic Data) ---
        variants = await self.extract_variants(context, sku)

        return CrawledData(
            sku=sku,
            title=title,
            ts=datetime.utcnow(),
            url=normalized_url,
            source=self.CONFIG["site_name"],
            listing_image=listing_image,
            images=images,
            category=category,
            description=description,
            variants=variants,
        )

    async def extract_variants(
        self,
        context: BeautifulSoupCrawlingContext,
        sku: str,
    ) -> List[ProductVariant]:
        variants: List[ProductVariant] = []
        soup = context.soup

        color_option = soup.select_one(self.CONFIG["selectors"]["color_option"])
        size_options = soup.select_one(self.CONFIG["selectors"]["size_options"])
        has_sizes = bool(
            size_options
            and size_options.select("li, label, input, option")
        )

        if not color_option:
            variant = self._process_simple_product(soup, sku)
            if variant:
                variants.append(variant)
            return variants

        color_labels = color_option.select("label input")
        if not color_labels:
            logger.warning(f"Color selector exists but contains no color inputs for SKU {sku}")
            return variants

        async with aiohttp.ClientSession(
            headers={"Accept-Encoding": "gzip, deflate"}
        ) as session:
            for inp in color_labels:
                try:
                    color_variants = await self._process_color(
                        inp, sku, has_sizes, session
                    )
                    variants.extend(color_variants)
                except Exception as e:
                    logger.warning(
                        f"Failed to process a color variant for SKU {sku}: {e}"
                    )
                    continue

        if not variants:
            logger.warning(
                f"No variants extracted for dynamic product SKU {sku}; base product will still be stored."
            )

        return variants

    async def _get_price_async(
        self,
        session: aiohttp.ClientSession,
        product_id: str,
        color_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetches price and discounted price from the website's internal API.

        Returns:
            A dictionary on success, or None if the API request fails.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept-Encoding": "gzip, deflate",
        }

        try:
            async with session.post(
                f"{self.base_url}/Products/ChangePriceByColor",
                headers=headers,
                data={"id": product_id, "colorId": color_id},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                if response.status != 200:
                    logger.warning(
                        f"Price API returned status {response.status} "
                        f"for product {product_id}, color {color_id}"
                    )
                    return None

                data = await response.json(content_type=None)
                if not isinstance(data, dict):
                    logger.warning(
                        f"Unexpected price API response for product {product_id}, "
                        f"color {color_id}: {type(data).__name__}"
                    )
                    return None

                return data

        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            logger.warning(
                f"Error fetching price for product {product_id}, "
                f"color {color_id}: {e}"
            )
            return None

    async def _get_sizes_async(
        self,
        session: aiohttp.ClientSession,
        color_id: str,
        product_id: str,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches available sizes and stock quantities for a specific color.

        Returns:
            A list on success (possibly empty), or None if the API request fails.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Accept-Encoding": "gzip, deflate",
        }

        try:
            async with session.get(
                f"{self.base_url}/Products/GetSizesForColor",
                params={"colorId": color_id, "productId": product_id},
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                if response.status != 200:
                    logger.warning(
                        f"Size API returned status {response.status} "
                        f"for product {product_id}, color {color_id}"
                    )
                    return None

                data = await response.json(content_type=None)
                if not isinstance(data, list):
                    logger.warning(
                        f"Unexpected size API response for product {product_id}, "
                        f"color {color_id}: {type(data).__name__}"
                    )
                    return None

                return data

        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            logger.warning(
                f"Error fetching sizes for product {product_id}, "
                f"color {color_id}: {e}"
            )
            return None

    async def _process_color(
        self,
        inp,
        sku: str,
        has_sizes: bool,
        session: aiohttp.ClientSession,
    ) -> List[ProductVariant]:
        """
        Processes one color option and returns all available variants for it.
        """
        variants: List[ProductVariant] = []

        color_id = inp.get("id")
        product_id = inp.get("data-model-id")
        color_name = inp.get("data-selected-color-title") or ""

        if not color_id or not product_id:
            raise ValueError(
                f"Missing color/product identifier for SKU {sku}: "
                f"color_id={color_id!r}, product_id={product_id!r}"
            )

        # Keep the original color text in the variant SKU while replacing
        # characters that are awkward as separators.
        safe_color = color_name.replace(" ", "_").replace("(", "").replace(")", "")
        if not safe_color:
            safe_color = str(color_id)

        # Price is selected by product + color, not by size, so fetch it once.
        price_data = await self._get_price_async(session, product_id, color_id)
        if price_data is None:
            logger.warning(
                f"Price API failed for SKU {sku}, color {color_name or color_id}; price fields will be empty."
            )
            price_data = {}

        price = str(price_data.get("price") or "")
        discounted_price = str(price_data.get("discountPrice") or "")

        # Scenario A: Product has sizes.
        if has_sizes:
            size_resp = await self._get_sizes_async(session, color_id, product_id)
            if size_resp is None:
                logger.warning(
                    f"Size API failed for SKU {sku}, color {color_name or color_id}; skipping this color."
                )
                return variants

            if not size_resp:
                logger.info(
                    f"No available sizes for SKU {sku}, color {color_name or color_id}"
                )
                return variants

            for size_data in size_resp:
                if not isinstance(size_data, dict):
                    logger.warning(
                        f"Ignoring malformed size entry for SKU {sku}, "
                        f"color {color_name or color_id}: {size_data!r}"
                    )
                    continue

                size_title = str(size_data.get("Title") or "")

                try:
                    quantity = int(size_data.get("Quantity", 0) or 0)
                except (TypeError, ValueError):
                    logger.warning(
                        f"Invalid quantity for SKU {sku}, size {size_title!r}, "
                        f"color {color_name or color_id}; using 0"
                    )
                    quantity = 0

                safe_size = size_title.replace(" ", "_") or "Unknown"

                variants.append(
                    ProductVariant(
                        variant_sku=f"{sku}_{safe_size}_{safe_color}",
                        size=size_title,
                        color=color_name,
                        quantity=quantity,
                        price=price,
                        discounted_price=discounted_price,
                    )
                )

            return variants

        # Scenario B: Product has color but no size selector.
        is_disabled = "disabled" in inp.attrs
        quantity = 0 if is_disabled else 1

        variants.append(
            ProductVariant(
                variant_sku=f"{sku}_{safe_color}",
                size="",
                color=color_name,
                quantity=quantity,
                price=price,
                discounted_price=discounted_price,
            )
        )

        return variants

    def _process_simple_product(self, soup, sku: str) -> Optional[ProductVariant]:
        """
        Handles products that do not have dynamic color variants.
        Extracts the current/original price directly from the HTML.
        """
        current_price_el = (
            soup.select_one(self.CONFIG["selectors"]["price"])
            if self.CONFIG["selectors"].get("price")
            else None
        )
        original_price_el = (
            soup.select_one(self.CONFIG["selectors"]["main_price"])
            if self.CONFIG["selectors"].get("main_price")
            else None
        )

        current_price = current_price_el.get_text(strip=True) if current_price_el else ""
        original_price = original_price_el.get_text(strip=True) if original_price_el else ""

        if original_price and current_price:
            price = original_price
            discounted_price = current_price
        else:
            price = current_price or original_price
            discounted_price = ""

        # #txtCount is present on purchasable simple-product pages. If it is
        # missing or disabled, treat the product as unavailable.
        txt_count = soup.select_one("#txtCount")
        quantity = 0 if not txt_count or txt_count.has_attr("disabled") else 1

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
            logger.exception(f"Crawler failed: {e}")

if __name__ == "__main__":
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)
    
    # Create and run crawler
    crawler = MashhadLeather(mode="crawl")
    asyncio.run(crawler.run())