"""
Indeed.com scraper for fractional executive job listings.

Indeed is the primary data source due to:
- Highest volume (300+ fractional CFO roles alone)
- Richest structured data (pay, location, experience level, benefits)
- Consistent format for scraping
"""

import time
import random
import re
from datetime import datetime
from typing import List, Dict, Optional, Generator
from dataclasses import dataclass
from urllib.parse import urlencode, urljoin
import requests
from bs4 import BeautifulSoup

import sys
sys.path.append('..')
from utils.parsers import (
    detect_function, detect_seniority, detect_location_type,
    extract_hours, parse_compensation, normalize_compensation,
    normalize_company_name, parse_date_posted
)


@dataclass
class IndeedJob:
    """Parsed job listing from Indeed."""
    source_id: str
    source_url: str
    title: str
    company_name: str
    location_raw: str
    compensation_raw: str
    job_type: str
    date_posted_raw: str
    description_snippet: str
    easy_apply: bool
    benefits: List[str]


class IndeedScraper:
    """Scraper for Indeed fractional job listings."""
    
    BASE_URL = "https://www.indeed.com"
    
    # Search terms to scrape
    SEARCH_TERMS = [
        "fractional CFO",
        "fractional CMO", 
        "fractional CRO",
        "fractional CTO",
        "fractional COO",
        "fractional CPO",
        "fractional CHRO",
        "fractional executive",
        "fractional controller",
        "fractional VP",
        "fractional head of",
        "part-time CFO",
        "part-time CMO",
        "interim CFO",
        "interim CMO",
        "interim CRO",
    ]
    
    # User agents to rotate
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    ]
    
    def __init__(self, delay_range: tuple = (2, 4)):
        """
        Initialize scraper.
        
        Args:
            delay_range: Min and max seconds to wait between requests
        """
        self.delay_range = delay_range
        self.session = requests.Session()
        self._update_headers()
    
    def _update_headers(self):
        """Rotate user agent."""
        self.session.headers.update({
            'User-Agent': random.choice(self.USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def _delay(self):
        """Wait between requests to be respectful."""
        time.sleep(random.uniform(*self.delay_range))
    
    def _build_search_url(self, query: str, location: str = "", start: int = 0) -> str:
        """
        Build Indeed search URL.
        
        Args:
            query: Search query
            location: Location filter (empty for remote)
            start: Pagination offset
            
        Returns:
            Full search URL
        """
        params = {
            'q': query,
            'l': location,
            'start': start,
            'sort': 'date',  # Sort by date for freshness
        }
        
        # Add remote filter if no location specified
        if not location:
            params['remotejob'] = '032b3046-06a3-4876-8dfd-474eb5e7ed11'  # Indeed's remote job filter ID
        
        return f"{self.BASE_URL}/jobs?{urlencode(params)}"
    
    def _parse_job_card(self, card: BeautifulSoup) -> Optional[IndeedJob]:
        """
        Parse a job card from search results.
        
        Args:
            card: BeautifulSoup element for job card
            
        Returns:
            IndeedJob or None if parsing fails
        """
        try:
            # Extract job ID from data attribute or link
            job_link = card.find('a', {'class': re.compile(r'jcs-JobTitle|tapItem')})
            if not job_link:
                return None
            
            href = job_link.get('href', '')
            source_id_match = re.search(r'jk=([a-f0-9]+)', href)
            source_id = source_id_match.group(1) if source_id_match else None
            
            if not source_id:
                return None
            
            source_url = urljoin(self.BASE_URL, href)
            
            # Title
            title_elem = card.find(['h2', 'span'], {'class': re.compile(r'jobTitle|title')})
            title = title_elem.get_text(strip=True) if title_elem else "Unknown"
            
            # Company name
            company_elem = card.find(['span', 'div'], {'class': re.compile(r'companyName|company')})
            company_name = company_elem.get_text(strip=True) if company_elem else "Unknown"
            
            # Location
            location_elem = card.find(['div', 'span'], {'class': re.compile(r'companyLocation|location')})
            location_raw = location_elem.get_text(strip=True) if location_elem else ""
            
            # Compensation
            salary_elem = card.find(['div', 'span'], {'class': re.compile(r'salary|estimated-salary|salaryText')})
            compensation_raw = salary_elem.get_text(strip=True) if salary_elem else ""
            
            # Job type (full-time, part-time, contract)
            metadata_elems = card.find_all(['div', 'span'], {'class': re.compile(r'metadata|attribute')})
            job_type = ""
            benefits = []
            for elem in metadata_elems:
                text = elem.get_text(strip=True).lower()
                if any(jt in text for jt in ['full-time', 'part-time', 'contract', 'temporary']):
                    job_type = text
                elif any(b in text for b in ['401', 'health', 'dental', 'vision', 'pto', 'remote']):
                    benefits.append(text)
            
            # Date posted
            date_elem = card.find(['span', 'div'], {'class': re.compile(r'date|posted')})
            date_posted_raw = date_elem.get_text(strip=True) if date_elem else ""
            
            # Description snippet
            snippet_elem = card.find(['div', 'td'], {'class': re.compile(r'job-snippet|snippet')})
            description_snippet = snippet_elem.get_text(strip=True) if snippet_elem else ""
            
            # Easy apply indicator
            easy_apply = bool(card.find(['span', 'div'], string=re.compile(r'easily apply', re.I)))
            
            return IndeedJob(
                source_id=source_id,
                source_url=source_url,
                title=title,
                company_name=company_name,
                location_raw=location_raw,
                compensation_raw=compensation_raw,
                job_type=job_type,
                date_posted_raw=date_posted_raw,
                description_snippet=description_snippet,
                easy_apply=easy_apply,
                benefits=benefits,
            )
            
        except Exception as e:
            print(f"Error parsing job card: {e}")
            return None
    
    def _search_page(self, url: str) -> List[IndeedJob]:
        """
        Fetch and parse a single search results page.
        
        Args:
            url: Search URL
            
        Returns:
            List of parsed jobs
        """
        self._update_headers()
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Request failed for {url}: {e}")
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find job cards - Indeed uses various class names
        job_cards = soup.find_all(['div', 'li'], {'class': re.compile(r'job_seen_beacon|resultContent|jobCard')})
        
        if not job_cards:
            # Try alternative selectors
            job_cards = soup.find_all('div', {'data-jk': True})
        
        jobs = []
        for card in job_cards:
            job = self._parse_job_card(card)
            if job:
                jobs.append(job)
        
        return jobs
    
    def search(self, query: str, location: str = "", max_pages: int = 10) -> Generator[IndeedJob, None, None]:
        """
        Search Indeed for jobs and yield results.
        
        Args:
            query: Search query
            location: Location filter
            max_pages: Maximum pages to scrape per query
            
        Yields:
            IndeedJob objects
        """
        seen_ids = set()
        
        for page in range(max_pages):
            start = page * 10
            url = self._build_search_url(query, location, start)
            
            print(f"Scraping: {query} (page {page + 1})")
            
            jobs = self._search_page(url)
            
            if not jobs:
                print(f"No more results for '{query}'")
                break
            
            new_jobs = 0
            for job in jobs:
                if job.source_id not in seen_ids:
                    seen_ids.add(job.source_id)
                    new_jobs += 1
                    yield job
            
            if new_jobs == 0:
                print(f"No new jobs on page {page + 1}, stopping")
                break
            
            self._delay()
    
    def scrape_all(self, max_pages_per_query: int = 5) -> Generator[IndeedJob, None, None]:
        """
        Scrape all configured search terms.
        
        Args:
            max_pages_per_query: Max pages per search term
            
        Yields:
            IndeedJob objects
        """
        all_seen_ids = set()
        
        for query in self.SEARCH_TERMS:
            for job in self.search(query, max_pages=max_pages_per_query):
                if job.source_id not in all_seen_ids:
                    all_seen_ids.add(job.source_id)
                    yield job
            
            # Longer delay between queries
            time.sleep(random.uniform(3, 6))
    
    def fetch_job_details(self, job_url: str) -> Dict:
        """
        Fetch full job details from individual job page.
        
        Args:
            job_url: URL of job posting
            
        Returns:
            Dict with full job details
        """
        self._update_headers()
        self._delay()
        
        try:
            response = self.session.get(job_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to fetch job details: {e}")
            return {}
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        details = {}
        
        # Full description
        desc_elem = soup.find(['div', 'section'], {'id': re.compile(r'jobDescriptionText')})
        if desc_elem:
            details['description_full'] = desc_elem.get_text(separator='\n', strip=True)
        
        # Benefits section
        benefits_section = soup.find(['div', 'ul'], {'class': re.compile(r'benefits')})
        if benefits_section:
            details['benefits'] = [li.get_text(strip=True) for li in benefits_section.find_all('li')]
        
        # Company info
        company_section = soup.find(['div'], {'class': re.compile(r'companyInfo')})
        if company_section:
            details['company_info'] = company_section.get_text(separator='\n', strip=True)
        
        return details


def job_to_db_dict(job: IndeedJob, details: Dict = None) -> Dict:
    """
    Convert IndeedJob to database-ready dictionary.
    
    Args:
        job: Parsed IndeedJob
        details: Optional full job details
        
    Returns:
        Dict matching FractionalJob model fields
    """
    # Parse compensation
    comp_type, comp_min, comp_max = parse_compensation(job.compensation_raw)
    
    # Extract hours from description
    desc_text = (job.description_snippet or '') + ' ' + (details.get('description_full', '') if details else '')
    hours_min, hours_max = extract_hours(desc_text)
    
    # Normalize compensation
    normalized_comp = normalize_compensation(comp_type, comp_min, comp_max, hours_min, hours_max)
    
    # Detect location type
    loc_type, loc_restriction, state = detect_location_type(job.location_raw, desc_text)
    
    return {
        'source': 'indeed',
        'source_id': job.source_id,
        'source_url': job.source_url,
        'title': job.title,
        'company_name': job.company_name,
        'company_name_normalized': normalize_company_name(job.company_name),
        'location_raw': job.location_raw,
        'location_type': loc_type,
        'location_restriction': loc_restriction,
        'location_state': state,
        'compensation_type': comp_type,
        'compensation_min': comp_min,
        'compensation_max': comp_max,
        'hourly_rate_min': normalized_comp['hourly_min'],
        'hourly_rate_max': normalized_comp['hourly_max'],
        'monthly_retainer_min': normalized_comp['monthly_min'],
        'monthly_retainer_max': normalized_comp['monthly_max'],
        'hours_per_week_min': hours_min,
        'hours_per_week_max': hours_max,
        'job_type': job.job_type,
        'function_category': detect_function(job.title),
        'seniority_tier': detect_seniority(job.title),
        'date_posted': parse_date_posted(job.date_posted_raw),
        'date_scraped': datetime.utcnow(),
        'description_snippet': job.description_snippet,
        'description_raw': details.get('description_full') if details else None,
        'benefits_raw': ', '.join(job.benefits) if job.benefits else None,
        'easy_apply': job.easy_apply,
        'is_active': True,
        'last_seen': datetime.utcnow(),
    }


if __name__ == "__main__":
    # Test the scraper
    scraper = IndeedScraper(delay_range=(2, 4))
    
    print("Testing Indeed scraper...")
    print("-" * 50)
    
    # Test single search
    count = 0
    for job in scraper.search("fractional CFO", max_pages=1):
        print(f"\n{job.title}")
        print(f"  Company: {job.company_name}")
        print(f"  Location: {job.location_raw}")
        print(f"  Compensation: {job.compensation_raw}")
        print(f"  URL: {job.source_url}")
        
        # Convert to DB format
        db_dict = job_to_db_dict(job)
        print(f"  Function: {db_dict['function_category']}")
        print(f"  Seniority: {db_dict['seniority_tier']}")
        print(f"  Location Type: {db_dict['location_type']}")
        
        count += 1
        if count >= 3:
            break
    
    print(f"\n\nFound {count} jobs in test")
