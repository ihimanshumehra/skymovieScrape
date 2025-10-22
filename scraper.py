"""
SkymoviesHD Movie Scraper - FINAL OPTIMIZED VERSION with concurrency
====================================================
Complete scraper with enhanced data extraction
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
from datetime import datetime
import re
from tqdm import tqdm
from urllib.parse import urljoin
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================
# CONFIGURATION
# ============================
BASE_URL = "https://skymovieshd.mba"
OUTPUT_FILE = f"skymovieshd_movies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
REQUEST_DELAY = 2  # Seconds between requests
MAX_RETRIES = 3
TIMEOUT = 30
SCRAPE_MOVIE_DETAILS = False  # Set to True if you want to visit each movie page for extra details

# Predefined categories
CATEGORIES = [
    {'name': 'Bollywood Movies', 'url': 'https://skymovieshd.mba/category/Bollywood-Movies.html'},
    {'name': 'South Indian Hindi Dubbed Movies', 'url': 'https://skymovieshd.mba/category/South-Indian-Hindi-Dubbed-Movies.html'},
    {'name': 'Hollywood English Movies', 'url': 'https://skymovieshd.mba/category/Hollywood-English-Movies.html'},
    {'name': 'Hollywood Hindi Dubbed Movies', 'url': 'https://skymovieshd.mba/category/Hollywood-Hindi-Dubbed-Movies.html'},
    {'name': 'WWE TV Shows', 'url': 'https://skymovieshd.mba/category/WWE-TV-Shows.html'},
    {'name': 'TV Serial Episodes', 'url': 'https://skymovieshd.mba/category/TV-Serial-Episodes.html'},
    {'name': 'Hot Short Film', 'url': 'https://skymovieshd.mba/category/Hot-Short-Film.html'},
    {'name': 'All Web Series', 'url': 'https://skymovieshd.mba/category/All-Web-Series.html'}
]

# ============================
# LOGGING SETUP
# ============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================
# SESSION SETUP
# ============================
def create_session():
    """Create requests session with retry and browser headers"""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        read=MAX_RETRIES,
        connect=MAX_RETRIES,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504)
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    })
    
    return session

# ================
# Fetch page 
# ===============
def fetch_page(session, url):
    """Fetch a page and return its HTML or None."""
    try:
        r = session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return url, r.text
    except Exception as e:
        logger.warning(f"Error fetching {url}: {e}")
        return url, None


# ============================
# DATA EXTRACTION
# ============================
def extract_movie_info(title_text):
    """Extract all metadata from movie title"""
    info = {
        'title': title_text,
        'year': None,
        'quality': None,
        'language': None,
        'file_size': None,
    }
    
    # Year
    year_match = re.search(r'\((\d{4})\)', title_text)
    if year_match:
        info['year'] = year_match.group(1)
    
    # Quality (720p focused)
    quality_match = re.search(r'(720p|1080p|480p|4K|2160p)', title_text, re.IGNORECASE)
    if quality_match:
        info['quality'] = quality_match.group(1)
    
    # File size
    size_match = re.search(r'\[([0-9.]+\s?(?:GB|MB))\]', title_text, re.IGNORECASE)
    if size_match:
        info['file_size'] = size_match.group(1)
    
    # Language detection
    languages = ['Hindi', 'English', 'Tamil', 'Telugu', 'Bengali', 'Punjabi', 
                 'Marathi', 'Malayalam', 'Kannada', 'Gujarati', 'Urdu', 'Dual Audio']
    for lang in languages:
        if lang.lower() in title_text.lower():
            info['language'] = lang
            break
    
    # Clean title
    clean_title = re.sub(r'\(\d{4}\)', '', title_text)
    clean_title = re.sub(r'(720p|1080p|480p|4K|2160p)', '', clean_title, flags=re.IGNORECASE)
    clean_title = re.sub(r'\[.*?\]', '', clean_title)
    clean_title = re.sub(r'(HDRip|BluRay|WEB-DL|HEVC|x264|x265|AAC|ESubs?|ORG\.?|Full|Movie)', '', clean_title, flags=re.IGNORECASE)
    clean_title = re.sub(r'\s+', ' ', clean_title).strip()
    info['title'] = clean_title
    
    return info

def scrape_movie_detail_page(session, movie_url):
    """Scrape individual movie page for additional details"""
    try:
        content = get_page_content(session, movie_url)
        if not content:
            return {}
        
        soup = BeautifulSoup(content, 'html.parser')
        details = {}
        
        # Extract Genre
        genre_div = soup.find('div', class_='L')
        if genre_div:
            genre_text = genre_div.get_text()
            genre_match = re.search(r'Genre\s*:\s*([^,]+)', genre_text)
            if genre_match:
                details['genre'] = genre_match.group(1).strip()
        
        # Extract other details from 'Let' class divs
        let_divs = soup.find_all('div', class_='Let')
        for div in let_divs:
            text = div.get_text(strip=True)
            if 'Release Date' in text:
                date_match = re.search(r'Release Date\s*:\s*(.+)', text)
                if date_match:
                    details['release_date'] = date_match.group(1).strip()
            elif 'Stars' in text:
                stars_match = re.search(r'Stars\s*:\s*(.+)', text)
                if stars_match:
                    details['stars'] = stars_match.group(1).strip()
        
        return details
    except Exception as e:
        logger.warning(f"Error scraping detail page {movie_url}: {str(e)}")
        return {}

def get_page_content(session, url, retries=3):
    """Fetch page with retry logic"""
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1}/{retries} failed for {url}: {str(e)}")
            if attempt < retries - 1:
                time.sleep(REQUEST_DELAY * (attempt + 1))
            else:
                logger.error(f"Failed to fetch {url}")
                return None
    return None

# ============================
# CATEGORY SCRAPING
# ============================

def scrape_category_page(session, category_url, category_name, max_pages=20, workers=10):
    base_slug = category_url.rstrip('.html').split('/')[-1]
    urls = [
        (i, category_url if i == 1 else f"{category_url.rstrip('.html')}/{i}.html")
        for i in range(1, max_pages + 1)
    ]

    movies = []
    page_results = []
    print(f"\n📂 Category: {category_name}")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_page, session, url): page_num 
                  for page_num, url in urls}

        for future in as_completed(futures):
            page_num = futures[future]
            page_url, html = future.result()
            if not html:
                continue

            soup = BeautifulSoup(html, 'html.parser')
            page_movies = []

            # Extract movies from this page
            for div in soup.find_all('div', class_='L', align='left'):
                a = div.find('a', href=True)
                if not a or a['href'] == 'movie/.html':
                    continue

                title = a.get_text(strip=True)
                href = a['href']
                movie_url = urljoin(BASE_URL, href)
                info = extract_movie_info(title)

                page_movies.append({
                    'category': category_name,
                    'title': info['title'],
                    'year': info['year'],
                    'quality': info['quality'],
                    'language': info['language'],
                    'file_size': info['file_size'],
                    'download_url': movie_url,
                    'full_title': title
                })

            page_results.append((page_num, page_movies))

    # Sort by page number and flatten
    page_results.sort(key=lambda x: x[0])
    for page_num, page_movies in page_results:
        movies.extend(page_movies)

    print(f"Completed {category_name}: {len(movies)} movies in page order\n")
    return movies


# ============================
# EXCEL EXPORT
# ============================
def save_to_excel(data, filename):
    """Save data to formatted Excel"""
    logger.info(f"Saving to {filename}")
    print(f"\n{'='*60}")
    print(f"💾 Saving to Excel: {filename}")
    print(f"{'='*60}")
    
    df = pd.DataFrame(data)
    
    columns = ['category', 'title', 'year', 'quality', 'language', 'genre', 
               'file_size', 'release_date', 'stars', 'download_url', 'poster_url', 'full_title']
    df = df[[col for col in columns if col in df.columns]]
    
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Movies', index=False)
        
        worksheet = writer.sheets['Movies']
        
        # Header formatting
        header_fill = PatternFill(start_color='366092', end_color='366092', fill_type='solid')
        header_font = Font(bold=True, color='FFFFFF', size=12)
        
        for cell in worksheet[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal='center', vertical='center')
        
        # Column widths
        widths = {'A': 25, 'B': 40, 'C': 10, 'D': 12, 'E': 15, 'F': 20, 
                  'G': 12, 'H': 15, 'I': 50, 'J': 50, 'K': 50, 'L': 60}
        for col, width in widths.items():
            worksheet.column_dimensions[col].width = width
        
        worksheet.freeze_panes = 'A2'
    
    print(f"✅ Saved {len(df)} movies")
    logger.info(f"Saved {len(df)} movies to {filename}")

# ============================
# MAIN EXECUTION
# ============================
def main():
    """Main scraper"""
    print("\n" + "="*60)
    print("🎬 SkymoviesHD Complete Movie Scraper")
    print("="*60)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    logger.info("Starting scraper")
    session = create_session()
    
    # Checkpoint 1
    print(f"\n🔹 CHECKPOINT 1: Loaded {len(CATEGORIES)} Categories")
    for idx, cat in enumerate(CATEGORIES, 1):
        print(f"  {idx}. {cat['name']}")
    
    # Checkpoint 2
    print("\n🔹 CHECKPOINT 2: Scraping Movies")
    all_movies = []
    
    with tqdm(total=len(CATEGORIES), desc="Progress", position=0) as pbar:
        for idx, category in enumerate(CATEGORIES, 1):
            pbar.set_description(f"[{idx}/{len(CATEGORIES)}] {category['name'][:30]}")
            
            #movies = scrape_category_page(session, category['url'], category['name'])
            movies = movies = scrape_category_page(session,category['url'],category['name'],max_pages=20,workers=10)
            all_movies.extend(movies)
            
            pbar.update(1)
            print(f"  ✓ {category['name']}: {len(movies)} movies")
            print(f"  📊 Total: {len(all_movies)}")
            
            time.sleep(REQUEST_DELAY)
    
    # Checkpoint 3
    print("\n🔹 CHECKPOINT 3: Saving Data")
    if all_movies:
        save_to_excel(all_movies, OUTPUT_FILE)
        
        # Summary
        print("\n" + "="*60)
        print("📈 SUMMARY")
        print("="*60)
        print(f"Categories: {len(CATEGORIES)}")
        print(f"Total Movies: {len(all_movies)}")
        print(f"Output: {OUTPUT_FILE}")
        print(f"Completed: {datetime.now().strftime('%H:%M:%S')}")
        print("="*60)
        
        df = pd.DataFrame(all_movies)
        print("\n📊 Category Breakdown:")
        for cat, count in df['category'].value_counts().items():
            print(f"  {cat}: {count}")
    else:
        print("⚠️ No movies found")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        print(f"\n❌ Error: {str(e)}")
