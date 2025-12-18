"""
Simple fractional job scraper - based on working CRO Report script.
"""

from jobspy import scrape_jobs
import pandas as pd
import time
from datetime import datetime

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

SITES = ["indeed"]

# Scraping
all_jobs = []
total_searches = len(SEARCH_TERMS) * len(LOCATIONS)
current_search = 0

print("=" * 60)
print("FRACTIONAL EXECUTIVE JOB SCRAPER")
print("=" * 60)
print(f"Search terms: {len(SEARCH_TERMS)}")
print(f"Locations: {len(LOCATIONS)}")
print(f"Total searches: {total_searches}")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

for location in LOCATIONS:
    for term in SEARCH_TERMS:
        current_search += 1
        print(f"\n[{current_search}/{total_searches}] {term} - {location}")
        
        try:
            jobs = scrape_jobs(
                site_name=SITES,
                search_term=term,
                location=location,
                results_wanted=50,
                hours_old=168,  # 7 days
                country_indeed='USA'
            )
            
            if len(jobs) > 0:
                all_jobs.append(jobs)
                print(f"   ✓ Found {len(jobs)} jobs")
            else:
                print(f"   - No results")
            
            # Wait between searches
            if current_search < total_searches:
                wait_time = 60
                print(f"   Waiting {wait_time}s...")
                time.sleep(wait_time)
                
        except Exception as e:
            print(f"   ✗ Error: {str(e)[:100]}")
            time.sleep(60)
            continue

# Combine results
print("\n" + "=" * 60)

if all_jobs:
    combined = pd.concat(all_jobs, ignore_index=True)
    
    # Remove duplicates
    unique_jobs = combined.drop_duplicates(subset=['job_url'])
    
    # Save to CSV
    filename = f"fractional_jobs_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    unique_jobs.to_csv(filename, index=False)
    
    print(f"COMPLETE!")
    print(f"Total jobs found: {len(combined)}")
    print(f"Unique jobs: {len(unique_jobs)}")
    print(f"Saved to: {filename}")
else:
    print("No jobs found!")

print("=" * 60)
