"""
Fractional job scraper - Indeed + FractionalJobs.io (IMPROVED)
This version fetches full job details from each FractionalJobs.io listing.

Replace your existing scrape_fractional.py with this file.
"""

from jobspy import scrape_jobs
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime

print("=" * 60)
print("FRACTIONAL EXECUTIVE JOB SCRAPER (v2 - Improved)")
print("=" * 60)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# ============================================================
# PART 1: INDEED (via JobSpy) - Unchanged
# ============================================================

SEARCH_TERMS = [
    "fractional CFO",
    "fractional CMO",
    "fractional CRO",
    "fractional CTO",
    "fractional COO",
    "fractional executive",
]

LOCATIONS = ["United States", "Remote"]

indeed_jobs = []
total_searches = len(SEARCH_TERMS) * len(LOCATIONS)
current_search = 0

print("\n[INDEED]")

for location in LOCATIONS:
    for term in SEARCH_TERMS:
        current_search += 1
        print(f"  [{current_search}/{total_searches}] {term} - {location}")
        
        try:
            jobs = scrape_jobs(
                site_name=["indeed"],
                search_term=term,
                location=location,
                results_wanted=50,
                hours_old=168,  # 7 days
                country_indeed="USA"
            )
            if len(jobs) > 0:
                indeed_jobs.append(jobs)
                print(f"    ✓ Found {len(jobs)} jobs")
            time.sleep(2)
        except Exception as e:
            print(f"    ✗ Error: {str(e)[:50]}")

# Combine and dedupe Indeed results
if indeed_jobs:
    indeed_df = pd.concat(indeed_jobs, ignore_index=True)
    indeed_df = indeed_df.drop_duplicates(subset=['job_url'])
    print(f"\n  Total Indeed (deduped): {len(indeed_df)}")
else:
    indeed_df = pd.DataFrame()
    print("\n  No Indeed jobs found")


# ============================================================
# PART 2: FRACTIONALJOBS.IO (IMPROVED - Fetches full details)
# ============================================================

print("\n[FRACTIONALJOBS.IO]")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

BASE_URL = "https://www.fractionaljobs.io"
fj_jobs = []

try:
    # Step 1: Get all job listing URLs from the index page
    print("  Fetching job index...")
    response = requests.get(f"{BASE_URL}/jobs", headers=HEADERS, timeout=30)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    job_urls = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if '/jobs/' in href and href != '/jobs' and 'fractionaljobs.io/jobs/' not in href:
            full_url = f"{BASE_URL}{href}" if href.startswith('/') else href
            if full_url not in job_urls:
                job_urls.append(full_url)
    
    print(f"  Found {len(job_urls)} job listing URLs")
    
    # Step 2: Fetch full details from each job page
    for i, job_url in enumerate(job_urls, 1):
        print(f"  [{i}/{len(job_urls)}] Fetching: {job_url.split('/')[-1][:40]}...")
        
        try:
            job_response = requests.get(job_url, headers=HEADERS, timeout=30)
            job_soup = BeautifulSoup(job_response.text, 'html.parser')
            
            job_data = {
                'title': None,
                'company': None,
                'location': 'Remote',
                'hours_per_week': None,
                'compensation': None,
                'description': None,
                'job_url': job_url,
                'source': 'fractionaljobs'
            }
            
            # Extract title (usually in h1)
            h1 = job_soup.find('h1')
            if h1:
                job_data['title'] = h1.get_text(strip=True)
            
            # Try to extract from page text
            page_text = job_soup.get_text(separator=' ', strip=True)
            
            # Extract company - look for patterns or specific elements
            # FractionalJobs often has company name near the title or in specific divs
            company_patterns = [
                r'at\s+([A-Z][A-Za-z0-9\s&\-\.]+?)(?:\s+\||\s+–|\s+in\s+|$)',
                r'Company:\s*([A-Za-z0-9\s&\-\.]+)',
            ]
            for pattern in company_patterns:
                match = re.search(pattern, page_text)
                if match:
                    job_data['company'] = match.group(1).strip()[:100]
                    break
            
            # If no company found, try to extract from URL
            if not job_data['company']:
                url_parts = job_url.split('/')[-1].split('-at-')
                if len(url_parts) > 1:
                    job_data['company'] = url_parts[-1].replace('-', ' ').title()
            
            # Extract hours per week
            hours_match = re.search(r'(\d+)\s*(?:hours?\s*(?:per\s*week|/\s*week|weekly)|hrs?/wk)', page_text, re.I)
            if hours_match:
                job_data['hours_per_week'] = hours_match.group(1)
            
            # Extract compensation
            comp_patterns = [
                r'\$[\d,]+(?:\s*-\s*\$[\d,]+)?(?:\s*(?:per|/)\s*(?:hour|hr|month|mo|year|yr|annual))?',
                r'\$[\d,]+[kK]?\s*(?:-\s*\$[\d,]+[kK]?)?',
            ]
            for pattern in comp_patterns:
                comp_match = re.search(pattern, page_text)
                if comp_match:
                    job_data['compensation'] = comp_match.group(0)
                    break
            
            # Extract description (look for main content area)
            desc_elem = job_soup.find('div', class_=lambda x: x and any(
                cls in str(x).lower() for cls in ['description', 'content', 'body', 'job-details', 'prose']
            ))
            if desc_elem:
                job_data['description'] = desc_elem.get_text(separator='\n', strip=True)[:5000]
            else:
                # Fallback: get main article or body content
                main = job_soup.find('main') or job_soup.find('article')
                if main:
                    job_data['description'] = main.get_text(separator='\n', strip=True)[:5000]
            
            # Only add if we got at least a title or company
            if job_data['title'] or job_data['company']:
                fj_jobs.append(job_data)
                status = f"✓ {job_data.get('title', 'No title')[:30]}"
            else:
                status = "✗ Could not extract data"
            
            print(f"      {status}")
            time.sleep(1)  # Be polite
            
        except Exception as e:
            print(f"      ✗ Error: {str(e)[:50]}")
    
    print(f"\n  Total FractionalJobs.io: {len(fj_jobs)}")
    
except Exception as e:
    print(f"  ✗ Error fetching index: {str(e)}")

fj_df = pd.DataFrame(fj_jobs) if fj_jobs else pd.DataFrame()


# ============================================================
# SAVE RESULTS
# ============================================================

print("\n" + "=" * 60)
print("RESULTS")
print("=" * 60)

timestamp = datetime.now().strftime('%Y%m%d_%H%M')

# Save Indeed jobs
if not indeed_df.empty:
    indeed_file = f"indeed_fractional_{timestamp}.csv"
    indeed_df.to_csv(indeed_file, index=False)
    print(f"Indeed: {len(indeed_df)} jobs → {indeed_file}")
else:
    print("Indeed: No jobs found")

# Save FractionalJobs.io jobs
if not fj_df.empty:
    fj_file = f"fractionaljobs_{timestamp}.csv"
    fj_df.to_csv(fj_file, index=False)
    print(f"FractionalJobs.io: {len(fj_df)} jobs → {fj_file}")
    
    # Quick quality check
    with_title = fj_df['title'].notna().sum()
    with_comp = fj_df['compensation'].notna().sum()
    print(f"  - With titles: {with_title}/{len(fj_df)} ({with_title/len(fj_df)*100:.0f}%)")
    print(f"  - With compensation: {with_comp}/{len(fj_df)} ({with_comp/len(fj_df)*100:.0f}%)")
else:
    print("FractionalJobs.io: No jobs found")

# Combined count
total = len(indeed_df) + len(fj_df)
print(f"\nTotal: {total} jobs")
print("=" * 60)
