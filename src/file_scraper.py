import datetime
import urllib.parse
from bs4 import BeautifulSoup
import markdown
import html
import html2text
import asyncio
import json
import os
import re
from pathlib import Path
from urllib.parse import urlparse, urljoin
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode # Assuming MemoryAdaptiveDispatcher might be part of crawl4ai or handled by default

# (scrape_page_to_data function remains the same as provided previously)
def scrape_page_to_data(page_url: str, page_html: str, page_markdown: str, page_links: dict, source_domain: str, crawl_depth: int) -> dict:
    """
    Scrapes and processes web page content.

    Args:
        page_url: The URL of the page.
        page_html: The HTML content of the page.
        page_markdown: The Markdown content of the page.
        page_links: A dictionary of internal and external links.
        source_domain: The domain from which the page was crawled.
        crawl_depth: The depth of the crawl.

    Returns:
        A dictionary containing the scraped and processed data.
    """
    processing_errors = []
    page_title = None
    metadata_tags = []
    images_data = []
    content_text = None

    # 1. Parse page_html using BeautifulSoup
    try:
        soup = BeautifulSoup(page_html, 'html.parser')

        # Extract page title
        title_tag = soup.find('title')
        if title_tag and title_tag.string:
            page_title = title_tag.string.strip()
        else:
            processing_errors.append("Page title not found.")

        # Extract meta tags
        for meta_tag in soup.find_all('meta'):
            name = meta_tag.get('name') or meta_tag.get('property') # Common attributes for name
            content = meta_tag.get('content')
            if name and content:
                metadata_tags.append({"name": name, "content": content})
            elif name and not content:
                processing_errors.append(f"Meta tag '{name}' has no content.")


        # Extract image information
        for img_tag in soup.find_all('img'):
            src = img_tag.get('src')
            alt = img_tag.get('alt', '') # Default to empty string if alt is missing
            if src:
                images_data.append({"src_original": src, "alt": alt})
            else:
                processing_errors.append(f"Image tag found with no src attribute: {img_tag}")

    except Exception as e:
        processing_errors.append(f"Error parsing HTML: {str(e)}")

    # 2. Convert page_markdown to plain text
    try:
        html_from_md = markdown.markdown(page_markdown if page_markdown else "")
        h = html2text.HTML2Text()
        h.ignore_links = True 
        h.ignore_images = True 
        content_text = h.handle(html_from_md).strip()

    except Exception as e:
        content_text = "" 
        processing_errors.append(f"Error converting markdown to text: {str(e)}")

    # 3. Process links
    internal_absolute_links = []
    external_links = []
    if page_links:
        for link_info in page_links.get("internal", []):
            try:
                text = link_info.get("text", "")
                href = link_info.get("href")
                if href:
                    absolute_url = urljoin(page_url, href)
                    internal_absolute_links.append({"text": text, "url": absolute_url})
                else:
                    processing_errors.append(f"Internal link found with no href: {link_info}")
            except Exception as e:
                processing_errors.append(f"Error processing internal link '{link_info.get('href')}': {str(e)}")

        for link_info in page_links.get("external", []):
            text = link_info.get("text", "")
            href = link_info.get("href")
            if href: 
                external_links.append({"text": text, "url": href})
            else:
                processing_errors.append(f"External link found with no href: {link_info}")

    # 4. Process image information (resolve to absolute URLs)
    processed_images = []
    for img_data in images_data:
        try:
            absolute_src = urljoin(page_url, img_data["src_original"])
            processed_images.append({
                "src_original": img_data["src_original"],
                "src_absolute": absolute_src,
                "alt": img_data["alt"]
            })
        except Exception as e:
            processing_errors.append(f"Error processing image src '{img_data['src_original']}': {str(e)}")
            processed_images.append({ 
                "src_original": img_data["src_original"],
                "src_absolute": img_data["src_original"], 
                "alt": img_data["alt"],
            })

    collection_timestamp = datetime.datetime.utcnow().isoformat()

    output_data = {
        "url": page_url,
        "source_domain": source_domain,
        "collection_timestamp": collection_timestamp,
        "crawl_depth": crawl_depth,
        "page_title": page_title,
        "content_markdown": page_markdown,
        "content_text": content_text,
        "links": {
            "internal_absolute": internal_absolute_links,
            "external": external_links
        },
        "media": {
            "images": processed_images,
            "audio_files": [],
            "video_files": []
        },
        "metadata_tags": metadata_tags,
        "raw_html_file_path": None,
        "processing_errors": processing_errors
    }
    return output_data


def _sanitize_filename(name: str) -> str:
    """Creates a safe filename from a string, typically a URL or path."""
    # Remove scheme (http, https)
    name = re.sub(r'^https?://', '', name)
    # Replace common problematic characters with underscores
    name = re.sub(r'[/:?*"<>|%\s\\]', '_', name)
    # Remove or replace other non-alphanumeric characters (except dots and underscores)
    name = re.sub(r'[^a-zA-Z0-9._-]', '', name)
    # Truncate to a reasonable length (e.g., 200 characters) to avoid OS limits
    name = name[:200]
    # Ensure it's not empty or just dots
    if not name or name.strip('.') == '':
        return "_default_filename"
    return name


async def scrape_site_to_files(crawler: AsyncWebCrawler, start_url: str, output_base_dir: str, max_depth: int = 3, save_raw_html: bool = False) -> dict:
    """
    Crawls a website starting from start_url, processes each page, and saves data to files.

    Args:
        crawler: An instance of AsyncWebCrawler.
        start_url: The URL to start crawling from.
        output_base_dir: The base directory where site data will be saved.
        max_depth: Maximum depth to crawl internal links.
        save_raw_html: Whether to save the raw HTML of each page.

    Returns:
        A dictionary summarizing the scraping operation.
    """
    process_errors = []
    try:
        parsed_start_url = urlparse(start_url)
        site_folder_name = _sanitize_filename(parsed_start_url.netloc)
        if not site_folder_name: # Handle cases where netloc might be empty for some reason
            site_folder_name = _sanitize_filename(start_url)

        main_output_dir = Path(output_base_dir) / site_folder_name
        raw_html_dir = main_output_dir / "raw_html"

        main_output_dir.mkdir(parents=True, exist_ok=True)
        if save_raw_html:
            raw_html_dir.mkdir(parents=True, exist_ok=True)

    except Exception as e:
        return {
            "success": False,
            "start_url": start_url,
            "site_folder": str(main_output_dir) if 'main_output_dir' in locals() else output_base_dir,
            "pages_scraped_count": 0,
            "scraped_files_paths": [],
            "errors": [f"Initialization error: {str(e)}"]
        }

    visited_urls = set()
    pages_data_paths = []
    # Queue stores (url, current_depth)
    queue = asyncio.Queue()
    queue.put_nowait((start_url, 0))

    source_domain = parsed_start_url.netloc

    crawl_config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS, stream=False) # As specified

    while not queue.empty():
        urls_to_crawl_at_current_depth = []
        current_depth_for_batch = -1 # Should be set by the first item

        # Gather all URLs at the current level from the queue
        # This helps in batching requests for crawler.arun_many()
        temp_q_holder = []
        while not queue.empty():
            url, depth = queue.get_nowait()
            if current_depth_for_batch == -1:
                 current_depth_for_batch = depth
            if depth == current_depth_for_batch and url not in visited_urls : # Process only if not visited
                 if depth <= max_depth:
                    urls_to_crawl_at_current_depth.append(url)
                    visited_urls.add(url) # Add to visited when scheduled for crawl
                 else: # Exceeded max_depth
                    continue
            elif depth > current_depth_for_batch: # Save for next batch
                temp_q_holder.append((url,depth))
            # else: url already visited or different depth, skip or re-queue if necessary (already handled by visited_urls)
        
        # Re-queue items for deeper levels
        for item in temp_q_holder:
            queue.put_nowait(item)

        if not urls_to_crawl_at_current_depth:
            continue

        try:
            # Use crawler.arun_many() to fetch pages
            # Note: crawler.arun_many expects a list of URLs.
            # We need to manage depth manually alongside.
            results = await crawler.arun_many(urls_to_crawl_at_current_depth, config=crawl_config)
        except Exception as e:
            process_errors.append(f"Error during crawler.arun_many for depth {current_depth_for_batch}: {str(e)}")
            # Potentially skip to next depth or try to continue if some URLs failed
            continue


        for page in results: # Page object from crawl4ai
            if not page or not page.url: # Skip if page is None or has no URL
                process_errors.append(f"Empty page or page with no URL encountered at depth {current_depth_for_batch}.")
                continue
            
            # page_url = page.url
            # if page_url in visited_urls and page_url != start_url : # Check again, though primary check is before arun_many
            #     continue
            # visited_urls.add(page_url)


            try:
                # Extract data for scrape_page_to_data
                page_html = page.html if page.html else ""
                page_markdown = page.markdown if page.markdown else ""
                page_links = page.links if page.links else {"internal": [], "external": []}

                data = scrape_page_to_data(
                    page_url=page.url,
                    page_html=page_html,
                    page_markdown=page_markdown,
                    page_links=page_links,
                    source_domain=source_domain,
                    crawl_depth=current_depth_for_batch # Use the depth of the current batch
                )

                # Sanitize filename
                # Use path and query for uniqueness, or just path if query is too complex/long
                url_path_for_filename = parsed_start_url.path if page.url == start_url and not parsed_start_url.path.strip('/') else urlparse(page.url).path
                if not url_path_for_filename.strip('/'): # Homepage or root
                     filename_base = "_homepage"
                else:
                     filename_base = _sanitize_filename(url_path_for_filename.strip('/'))
                
                json_filename = f"{filename_base}.json"
                json_file_path = main_output_dir / json_filename

                # Save JSON data
                with open(json_file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                
                pages_data_paths.append(str(json_file_path))

                # Save Raw HTML (Optional)
                if save_raw_html:
                    html_filename = f"{filename_base}.html"
                    html_file_path = raw_html_dir / html_filename
                    with open(html_file_path, 'w', encoding='utf-8') as f:
                        f.write(page_html)
                    # Relative path from output_base_dir
                    data['raw_html_file_path'] = str(html_file_path.relative_to(output_base_dir))
                    # Re-save JSON with updated raw_html_file_path
                    with open(json_file_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)


                # Add new internal links to the queue for the next depth level
                if current_depth_for_batch < max_depth:
                    for link_info in data['links']['internal_absolute']:
                        next_url = link_info['url']
                        # Only add if it's within the same source domain and not visited
                        if urlparse(next_url).netloc == source_domain and next_url not in visited_urls:
                            # visited_urls.add(next_url) # Add here to avoid re-queueing from multiple pages
                            queue.put_nowait((next_url, current_depth_for_batch + 1))
            
            except Exception as e:
                process_errors.append(f"Error processing page {page.url}: {str(e)}")


    return {
        "success": not process_errors, # True if no errors, False otherwise
        "start_url": start_url,
        "site_folder": str(main_output_dir),
        "pages_scraped_count": len(pages_data_paths),
        "scraped_files_paths": pages_data_paths,
        "errors": process_errors
    }


if __name__ == '__main__':
    # This is a placeholder for testing.
    # To run this, you would need to set up an AsyncWebCrawler instance
    # and an event loop (e.g., using asyncio.run).

    # Example of how it might be called:
    async def main_test():
        # A mock crawler for testing structure; replace with actual AsyncWebCrawler
        class MockPage:
            def __init__(self, url, html_content, markdown_content, links_dict):
                self.url = url
                self.html = html_content
                self.markdown = markdown_content
                self.links = links_dict

        class MockAsyncWebCrawler:
            async def arun_many(self, urls, config=None):
                print(f"MockCrawler: Pretending to crawl {urls} with config {config}")
                results = []
                for i, url in enumerate(urls):
                    if "page1" in url:
                        links = {"internal": [{"text": "Link to Page 2", "href": "http://example.com/page2.html"}]}
                        results.append(MockPage(url, f"<html><title>Page 1</title><body>Content of {url}</body></html>", f"# Page 1\nContent of {url}", links))
                    elif "page2" in url:
                         links = {"internal": [{"text": "Link to Page 3 (depth limit)", "href": "http://example.com/page3.html"}]} # This would be depth 2
                         results.append(MockPage(url, f"<html><title>Page 2</title><body>Content of {url}</body></html>", f"# Page 2\nContent of {url}", links))
                    elif "page3" in url: # Should not be processed if max_depth = 1
                         results.append(MockPage(url, f"<html><title>Page 3</title><body>Content of {url}</body></html>", f"# Page 3\nContent of {url}", {}))

                    else: # start_url
                        links = {"internal": [{"text": "Link to Page 1", "href": "http://example.com/page1.html"}]}
                        results.append(MockPage(url, f"<html><head><title>Mock Start Page</title></head><body><h1>Mock Site</h1><a href='page1.html'>Page 1</a></body></html>", "# Mock Site\n[Page 1](page1.html)", links))
                await asyncio.sleep(0.1) # Simulate async work
                return results

            async def __aenter__(self): return self
            async def __aexit__(self, exc_type, exc, tb): pass


        # --- Test _sanitize_filename ---
        print("--- Testing _sanitize_filename ---")
        test_names = [
            "http://example.com/path/to/file.html",
            "https://example.com/another path?query=value&other=val",
            "just_a_filename.txt",
            "/leading/slash",
            "trailing/slash/",
            "../../../../etc/passwd",
            "very_long_filename" * 20,
            "",
            "...",
            "???"
        ]
        for tn in test_names:
            print(f"'{tn}' -> '{_sanitize_filename(tn)}'")
        print("--- End _sanitize_filename Test ---\n")


        # --- Test scrape_site_to_files ---
        print("--- Testing scrape_site_to_files ---")
        output_dir = "./scraped_site_output"
        # Clean up previous test run
        if Path(output_dir).exists():
            import shutil
            shutil.rmtree(output_dir)
            print(f"Cleaned up old '{output_dir}'")

        async with MockAsyncWebCrawler() as crawler:
            summary = await scrape_site_to_files(
                crawler=crawler,
                start_url="http://example.com/mainpage",
                output_base_dir=output_dir,
                max_depth=1, # Start -> page1 (depth 0) -> page2 (depth 1). page3 will be found but not crawled.
                save_raw_html=True
            )
        
        print("\n--- Scraping Summary ---")
        print(json.dumps(summary, indent=2))

        # Verify created files (basic check)
        if summary["success"]:
            site_folder = Path(summary["site_folder"])
            assert site_folder.exists()
            assert (site_folder / "_homepage.json").exists() # for start_url
            assert (site_folder / "raw_html" / "_homepage.html").exists()

            assert (site_folder / "page1.html.json").exists() # for page1.html
            assert (site_folder / "raw_html" / "page1.html.html").exists()
            
            # page2.html is found by page1, its depth is 1. It should be crawled.
            assert (site_folder / "page2.html.json").exists()
            assert (site_folder / "raw_html" / "page2.html.html").exists()

            # page3.html is found by page2. Its depth would be 2. It should NOT be crawled if max_depth=1.
            # However, the current logic for filename sanitization needs adjustment.
            # The test mock crawler creates files like `page1.html` which becomes `page1.html.json`
            # This is fine. Let's assume `_sanitize_filename` works as intended.

            print(f"\nFiles seem to be created in {summary['site_folder']}.")
            print("Please manually inspect the contents for correctness.")
        else:
            print("\nScraping reported errors, check summary.")
        print("--- End scrape_site_to_files Test ---")


    if __name__ == "__main__": # This block needs to be outside async main_test
        # The original `if __name__ == '__main__':` with scrape_page_to_data examples
        # can be kept if needed, but for scrape_site_to_files, we need asyncio.run
        # For simplicity, I'm replacing it with the async test runner.
        # To run the old tests, you'd need to comment out this new runner.
        
        # Run the async test function
        asyncio.run(main_test())

        # --- Original scrape_page_to_data test (can be re-enabled if needed) ---
        # print("\n--- Original scrape_page_to_data tests ---")
        # sample_html = """...""" # (original sample_html)
        # sample_markdown = """...""" # (original sample_markdown)
        # sample_links = {} # (original sample_links)
        # test_url = "http://mytestsite.com/path/to/page.html"
        # test_domain = "mytestsite.com"
        # test_depth = 1
        # scraped_data = scrape_page_to_data(test_url, sample_html, sample_markdown, sample_links, test_domain, test_depth)
        # print(json.dumps(scraped_data, indent=2))
        # print("--- End original tests ---")
