"""
FractionalJobs.io scraper for fractional executive job listings.

Secondary data source - specialized for fractional work with:
- Structured hours/week field
- Startup-focused listings
- Clean, purpose-built data format
"""

import time
import random
import re
from datetime import datetime
from typing import List, Dict, Optional, Generator
from dataclasses import dataclass
from urllib.parse import urljoin
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
class FractionalJobsListing:
    """Parsed job listing from FractionalJobs.io."""
    source_id: str  # URL slug
    source_url: str
    title: str
    company_name: str
    company_url: Optional[str]
    location_raw: str
    hours_raw: str
    compensation_raw: str
    function_category: str
    date_added: str
    is_featured: bool


class FractionalJobsScraper:
    """Scraper for FractionalJobs.io listings."""
    
    BASE_URL = "https://www.fractionaljobs.io"
    
    # User agents to rotate
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    
    # Function category mapping from their filter names
    FUNCTION_MAP = {
        'engineering': 'engineering',
        'marketing': 'marketing',
        'design': 'product',  # Map to closest
        'sales': 'sales',
        'product': 'product',
        'finance': 'finance',
        'operations': 'operations',
        'growth': 'marketing',  # Growth usually marketing-adjacent
        'people': 'people',
        'analytics': 'data',
        'data': 'data',
        'legal': 'legal',
        'other': 'other',
    }
    
    def __init__(self, delay_range: tuple = (1, 2)):
        """
        Initialize scraper.
        
        Args:
            delay_range: Min and max seconds to wait between requests
        """
        self.delay_range = delay_range
        self.session = requests.Session()
        self._update_headers()
    
    def _update_headers(self):
        """Set request headers."""
        self.session.headers.update({
            'User-Agent': random.choice(self.USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        })
    
    def _delay(self):
        """Wait between requests to be respectful."""
        time.sleep(random.uniform(*self.delay_range))
    
    def _parse_job_card(self, card: BeautifulSoup) -> Optional[FractionalJobsListing]:
        """
        Parse a job card from the homepage listing.
        
        Based on the HTML structure observed:
        - Company name as header
        - Role title
        - Company URL in parentheses
        - Hours | Compensation | Location
        - Function category
        - Date added
        
        Args:
            card: BeautifulSoup element for job card
            
        Returns:
            FractionalJobsListing or None
        """
        try:
            # Get all text content
            text_content = card.get_text(separator=' | ', strip=True)
            
            # Look for company name (usually in h3 or first strong element)
            company_elem = card.find(['h3', 'strong'])
            company_name = company_elem.get_text(strip=True) if company_elem else "Unknown"
            
            # Look for title (usually follows company name with a dash)
            title_parts = text_content.split(' - ')
            title = title_parts[1] if len(title_parts) > 1 else "Unknown"
            
            # Clean title from extra content
            title = title.split(' | ')[0].strip()
            title = title.split('(')[0].strip()
            
            # Look for company URL
            company_link = card.find('a', href=re.compile(r'^https?://'))
            company_url = company_link.get('href') if company_link else None
            
            # Parse structured data from the listing
            # Format: "10 hrs | $2.5K - $3K / mo. + commission | Remote"
            # or: "10 - 15 hrs | $200 / hr | Remote (USA only)"
            
            hours_raw = ""
            compensation_raw = ""
            location_raw = ""
            
            # Find the metadata line (contains hrs and often $ or remote)
            meta_match = re.search(
                r'(\d+(?:\s*-\s*\d+)?\s*hrs?)[^\|]*\|([^\|]+)\|([^\|]+)',
                text_content
            )
            if meta_match:
                hours_raw = meta_match.group(1).strip()
                compensation_raw = meta_match.group(2).strip()
                location_raw = meta_match.group(3).strip()
            else:
                # Try alternative patterns
                hours_match = re.search(r'(\d+(?:\s*-\s*\d+)?\s*hrs?)', text_content)
                if hours_match:
                    hours_raw = hours_match.group(1)
                
                comp_match = re.search(r'(\$[\d,\.]+[kK]?\s*(?:-\s*\$[\d,\.]+[kK]?)?\s*(?:\/\s*(?:hr|mo|month|hour))?)', text_content)
                if comp_match:
                    compensation_raw = comp_match.group(1)
                
                if 'remote' in text_content.lower():
                    loc_match = re.search(r'(remote[^|]*)', text_content, re.I)
                    location_raw = loc_match.group(1) if loc_match else "Remote"
            
            # Function category - look for category label
            function_category = 'other'
            for func_name in self.FUNCTION_MAP.keys():
                if func_name.lower() in text_content.lower():
                    function_category = self.FUNCTION_MAP[func_name]
                    break
            
            # Date added
            date_match = re.search(r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d+,?\s*\d*)', text_content)
            date_added = date_match.group(1) if date_match else ""
            
            # Alternative: "added X days ago" pattern
            if not date_added:
                days_match = re.search(r'added\s+(\d+)\s*days?\s*ago', text_content, re.I)
                if days_match:
                    date_added = f"{days_match.group(1)} days ago"
            
            # Job URL/slug
            job_link = card.find('a', href=re.compile(r'/jobs/|fractionaljobs\.io/'))
            source_url = ""
            source_id = ""
            if job_link:
                href = job_link.get('href', '')
                source_url = urljoin(self.BASE_URL, href) if not href.startswith('http') else href
                # Extract slug as source_id
                slug_match = re.search(r'/([a-z0-9-]+)(?:\?|$)', source_url)
                source_id = slug_match.group(1) if slug_match else href
            
            if not source_id:
                # Generate ID from company + title
                source_id = f"{normalize_company_name(company_name)}-{normalize_company_name(title)}"
            
            # Check if featured
            is_featured = bool(card.find(['span', 'div'], string=re.compile(r'featured', re.I)))
            
            return FractionalJobsListing(
                source_id=source_id,
                source_url=source_url or self.BASE_URL,
                title=title,
                company_name=company_name,
                company_url=company_url,
                location_raw=location_raw,
                hours_raw=hours_raw,
                compensation_raw=compensation_raw,
                function_category=function_category,
                date_added=date_added,
                is_featured=is_featured,
            )
            
        except Exception as e:
            print(f"Error parsing job card: {e}")
            return None
    
    def scrape_homepage(self) -> List[FractionalJobsListing]:
        """
        Scrape all job listings from the homepage.
        
        Returns:
            List of parsed job listings
        """
        self._update_headers()
        
        try:
            response = self.session.get(self.BASE_URL, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to fetch FractionalJobs.io: {e}")
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find job listing container
        # Based on observed structure, jobs are in divs with specific patterns
        jobs = []
        
        # Try multiple selectors
        job_containers = (
            soup.find_all('div', {'class': re.compile(r'job|listing|card', re.I)}) or
            soup.find_all('article') or
            soup.find_all('li', {'class': re.compile(r'job|listing', re.I)})
        )
        
        # Also look for the specific pattern in the HTML we fetched earlier
        # Jobs appear as structured blocks with company name, title, metadata
        
        # Parse each job section
        for container in job_containers:
            # Check if this looks like a job listing
            text = container.get_text()
            if 'hrs' in text.lower() and ('remote' in text.lower() or '$' in text):
                job = self._parse_job_card(container)
                if job and job.title != "Unknown":
                    jobs.append(job)
        
        return jobs
    
    def scrape_all(self) -> Generator[FractionalJobsListing, None, None]:
        """
        Scrape all listings.
        
        Yields:
            FractionalJobsListing objects
        """
        print("Scraping FractionalJobs.io homepage...")
        
        jobs = self.scrape_homepage()
        
        print(f"Found {len(jobs)} listings")
        
        for job in jobs:
            yield job


def job_to_db_dict(job: FractionalJobsListing) -> Dict:
    """
    Convert FractionalJobsListing to database-ready dictionary.
    
    Args:
        job: Parsed FractionalJobsListing
        
    Returns:
        Dict matching FractionalJob model fields
    """
    # Parse hours from structured field
    hours_min, hours_max = extract_hours(job.hours_raw)
    
    # Parse compensation
    comp_type, comp_min, comp_max = parse_compensation(job.compensation_raw)
    
    # Normalize compensation
    normalized_comp = normalize_compensation(comp_type, comp_min, comp_max, hours_min, hours_max)
    
    # Detect location type
    loc_type, loc_restriction, state = detect_location_type(job.location_raw, '')
    
    return {
        'source': 'fractionaljobs',
        'source_id': job.source_id,
        'source_url': job.source_url,
        'title': job.title,
        'company_name': job.company_name,
        'company_name_normalized': normalize_company_name(job.company_name),
        'company_url': job.company_url,
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
        'function_category': job.function_category,
        'seniority_tier': detect_seniority(job.title),
        'date_posted': parse_date_posted(job.date_added),
        'date_scraped': datetime.utcnow(),
        'is_active': True,
        'last_seen': datetime.utcnow(),
    }


if __name__ == "__main__":
    # Test the scraper
    scraper = FractionalJobsScraper()
    
    print("Testing FractionalJobs.io scraper...")
    print("-" * 50)
    
    count = 0
    for job in scraper.scrape_all():
        print(f"\n{job.title}")
        print(f"  Company: {job.company_name}")
        print(f"  Hours: {job.hours_raw}")
        print(f"  Compensation: {job.compensation_raw}")
        print(f"  Location: {job.location_raw}")
        print(f"  Function: {job.function_category}")
        print(f"  URL: {job.source_url}")
        
        # Convert to DB format
        db_dict = job_to_db_dict(job)
        print(f"  Parsed Hours: {db_dict['hours_per_week_min']}-{db_dict['hours_per_week_max']}")
        print(f"  Parsed Hourly Rate: ${db_dict['hourly_rate_min']:.0f}-${db_dict['hourly_rate_max']:.0f}/hr" if db_dict['hourly_rate_min'] else "  Rate: Not disclosed")
        
        count += 1
        if count >= 5:
            break
    
    print(f"\n\nDisplayed {count} jobs")
