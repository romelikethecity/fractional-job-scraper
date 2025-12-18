"""
Fractional job scraper - Indeed + FractionalJobs.io
"""

from jobspy import scrape_jobs
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime

print("=" * 60)
print("FRACTIONAL EXECUTIVE JOB SCRAPER")
print("=" * 60)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# ============================================================
# PART 1: INDEED (via JobSpy)
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
                hours_old=168,
                country_indeed='USA'
            )
            
            if len(jobs) > 0:
                indeed_jobs.append(jobs)
                print(f"      ✓ Found {len(jobs)} jobs")
            else:
                print(f"      - No results")
            
            if current_search < total_searches:
                time.sleep(60)
                
        except Exception as e:
            print(f"      ✗ Error: {str(e)[:50]}")
            time.sleep(60)

# Combine Indeed results
if indeed_jobs:
    indeed_df = pd.concat(indeed_jobs, ignore_index=True)
    indeed_df = indeed_df.drop_duplicates(subset=['job_url'])
    indeed_df['source'] = 'indeed'
    print(f"\n  Indeed total: {len(indeed_df)} unique jobs")
else:
    indeed_df = pd.DataFrame()
    print("\n  Indeed: No jobs found")

# ============================================================
# PART 2: FRACTIONALJOBS.IO (direct scrape)
# ============================================================

print("\n[FRACTIONALJOBS.IO]")

fj_jobs = []

try:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    
    response = requests.get('https://www.fractionaljobs.io', headers=headers, timeout=30)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find job listings
    job_links = soup.find_all('a', href=re.compile(r'/jobs/'))
    
    seen_urls = set()
    for link in job_links:
        href = link.get('href', '')
        if href in seen_urls:
            continue
        seen_urls.add(href)
        
        parent = link.find_parent(['div', 'article', 'li'])
        if not parent:
            continue
        
        text = parent.get_text(separator=' | ', strip=True)
        title = link.get_text(strip=True)
        
        company = ""
        company_elem = parent.find(['h3', 'strong'])
        if company_elem:
            company = company_elem.get_text(strip=True)
        
        hours = ""
        hours_match = re.search(r'(\d+(?:\s*-\s*\d+)?\s*hrs?)', text)
        if hours_match:
            hours = hours_match.group(1)
        
        comp = ""
        comp_match = re.search(r'(\$[\d,\.]+[kK]?\s*(?:-\s*\$[\d,\.]+[kK]?)?\s*(?:\/\s*(?:hr|mo|hour|month))?)', text)
        if comp_match:
            comp = comp_match.group(1)
        
        location = "Remote"
        if 'usa' in text.lower():
            location = "Remote (USA)"
        
        job_url = f"https://www.fractionaljobs.io{href}" if href.startswith('/') else href
        
        fj_jobs.append({
            'title': title,
            'company': company,
            'location': location,
            'hours_per_week': hours,
            'compensation': comp,
            'job_url': job_url,
            'source': 'fractionaljobs'
        })
    
    print(f"  ✓ Found {len(fj_jobs)} jobs")
    
except Exception as e:
    print(f"  ✗ Error: {str(e)}")

fj_df = pd.DataFrame(fj_jobs) if fj_jobs else pd.DataFrame()

# ============================================================
# SAVE RESULTS SEPARATELY
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
else:
    print("FractionalJobs.io: No jobs found")

# Combined count
total = len(indeed_df) + len(fj_df)
print(f"\nTotal: {total} jobs")
print("=" * 60)
