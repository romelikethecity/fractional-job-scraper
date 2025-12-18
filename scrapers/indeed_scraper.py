"""
Indeed.com scraper for fractional executive job listings.
Uses JobSpy library for reliable scraping.
"""

from jobspy import scrape_jobs
import pandas as pd
import time
from datetime import datetime
from typing import List, Dict, Generator

import sys
import os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.parsers import (
    detect_function, detect_seniority, detect_location_type,
    extract_hours, parse_compensation, normalize_compensation,
    normalize_company_name
)


# Search terms for fractional roles
SEARCH_TERMS = [
    "fractional CFO",
    "fractional CMO",
    "fractional CRO",
    "fractional CTO",
    "fractional COO",
    "fractional executive",
]

LOCATIONS = [
    "United States",
    "Remote",
]


class IndeedScraper:
    """Scraper for Indeed fractional job listings using JobSpy."""
    
    def __init__(self, wait_time: int = 60):
        self.wait_time = wait_time
    
    def scrape_all(self, max_results_per_search: int = 50) -> Generator[Dict, None, None]:
        total_searches = len(SEARCH_TERMS) * len(LOCATIONS)
        current_search = 0
        seen_urls = set()
        
        print(f"Starting Indeed scrape: {len(SEARCH_TERMS)} terms x {len(LOCATIONS)} locations")
        
        for location in LOCATIONS:
            for term in SEARCH_TERMS:
                current_search += 1
                print(f"\n[{current_search}/{total_searches}] {term} in {location}")
                
                try:
                    jobs_df = scrape_jobs(
                        site_name=["indeed"],
                        search_term=term,
                        location=location,
                        results_wanted=max_results_per_search,
                        hours_old=168,
                        country_indeed='USA'
                    )
                    
                    if len(jobs_df) > 0:
                        print(f"   Found {len(jobs_df)} jobs")
                        
                        for _, row in jobs_df.iterrows():
                            job_url = row.get('job_url', '')
                            if job_url in seen_urls:
                                continue
                            seen_urls.add(job_url)
                            yield row.to_dict()
                    else:
                        print(f"   No results")
                    
                    if current_search < total_searches:
                        print(f"   Waiting {self.wait_time}s...")
                        time.sleep(self.wait_time)
                        
                except Exception as e:
                    print(f"   Error: {str(e)[:100]}")
                    time.sleep(self.wait_time)
                    continue


def job_to_db_dict(job: Dict) -> Dict:
    title = job.get('title', '')
    description = job.get('description', '')
    
    min_amount = job.get('min_amount')
    max_amount = job.get('max_amount')
    interval = job.get('interval', '')
    
    if interval == 'hourly':
        comp_type = 'hourly'
    elif interval == 'monthly':
        comp_type = 'monthly'
    elif interval in ('yearly', 'annually'):
        comp_type = 'annual'
    elif min_amount or max_amount:
        if max_amount and max_amount < 500:
            comp_type = 'hourly'
        elif max_amount and max_amount < 50000:
            comp_type = 'monthly'
        else:
            comp_type = 'annual'
    else:
        comp_type = 'not_disclosed'
    
    hours_min, hours_max = extract_hours(description)
    normalized_comp = normalize_compensation(comp_type, min_amount, max_amount, hours_min, hours_max)
    
    location_raw = job.get('location', '')
    is_remote = job.get('is_remote', False)
    
    if is_remote:
        loc_type, loc_restriction, state = 'remote', 'usa_only', None
    else:
        loc_type, loc_restriction, state = detect_location_type(location_raw, description)
    
    job_url = job.get('job_url', '')
    source_id = job_url.split('jk=')[-1][:16] if 'jk=' in job_url else str(hash(job_url))[-16:]
    
    return {
        'source': 'indeed',
        'source_id': source_id,
        'source_url': job_url,
        'title': title,
        'company_name': job.get('company', ''),
        'company_name_normalized': normalize_company_name(job.get('company', '')),
        'company_url': job.get('company_url'),
        'location_raw': location_raw,
        'location_type': loc_type,
        'location_restriction': loc_restriction,
        'location_state': state,
        'compensation_type': comp_type,
        'compensation_min': min_amount,
        'compensation_max': max_amount,
        'hourly_rate_min': normalized_comp['hourly_min'],
        'hourly_rate_max': normalized_comp['hourly_max'],
        'monthly_retainer_min': normalized_comp['monthly_min'],
        'monthly_retainer_max': normalized_comp['monthly_max'],
        'hours_per_week_min': hours_min,
        'hours_per_week_max': hours_max,
        'job_type': job.get('job_type', ''),
        'function_category': detect_function(title),
        'seniority_tier': detect_seniority(title),
        'date_posted': job.get('date_posted'),
        'date_scraped': datetime.utcnow(),
        'description_raw': description,
        'description_snippet': description[:500] if description else None,
        'is_active': True,
        'last_seen': datetime.utcnow(),
    }


if __name__ == "__main__":
    scraper = IndeedScraper(wait_time=10)
    
    print("Testing Indeed scraper...")
    count = 0
    for job in scraper.scrape_all(max_results_per_search=5):
        print(f"\n{job.get('title')}")
        print(f"  Company: {job.get('company')}")
        print(f"  Location: {job.get('location')}")
        
        count += 1
        if count >= 3:
            break
    
    print(f"\nTest complete. Found {count} jobs.")
