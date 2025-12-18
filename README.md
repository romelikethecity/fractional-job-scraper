# Fractional Executive Job Scraper

A multi-source scraper for tracking fractional executive job postings, designed to power a Substack newsletter for fractional workers.

## Data Sources

### Primary: Indeed
- **Volume**: 300+ fractional CFO roles alone, 800+ total fractional executive listings
- **Data Quality**: Rich structured data (pay, location, experience level, benefits)
- **Search Terms**: "fractional CFO", "fractional CMO", "fractional CRO", "fractional CTO", "fractional COO", "fractional executive"

### Secondary: FractionalJobs.io
- **Volume**: ~40 active listings
- **Unique Value**: Structured hours/week field, startup-focused
- **Data Quality**: Clean, purpose-built for fractional work

### Tertiary: The Free Agent
- **Volume**: ~15 visible listings
- **Unique Value**: PE/VC portfolio company focus, always shows hourly rate
- **Data Quality**: Good but often anonymized company names

## Database Schema

### Core Tables

```
fractional_jobs
├── id (PRIMARY KEY)
├── source (indeed/fractionaljobs/freeagent)
├── source_id (unique ID from source)
├── title
├── company_name
├── company_url
├── location_raw
├── location_type (remote/hybrid/onsite)
├── location_restriction (worldwide/usa_only/state_specific/timezone)
├── compensation_type (hourly/monthly/annual/equity_only/not_disclosed)
├── compensation_min
├── compensation_max
├── compensation_currency (USD default)
├── hours_per_week_min
├── hours_per_week_max
├── job_type (contract/part_time/full_time)
├── experience_level (entry/mid/senior/executive)
├── function_category (finance/marketing/sales/product/engineering/ops/people/other)
├── seniority_tier (c_level/evp/svp/vp/director/head_of)
├── date_posted
├── date_scraped
├── description_raw
├── benefits_raw
├── is_active (boolean)
└── last_seen

company_enrichment
├── id (PRIMARY KEY)
├── company_name
├── company_url
├── crunchbase_url
├── pitchbook_url
├── funding_stage (seed/series_a/series_b/series_c/growth/public/pe_backed/bootstrapped)
├── total_funding
├── last_funding_date
├── employee_count
├── industry
├── hq_location
├── date_enriched
└── enrichment_source

compensation_snapshots
├── id (PRIMARY KEY)
├── snapshot_date
├── function_category
├── seniority_tier
├── location_type
├── sample_size
├── hourly_rate_min_avg
├── hourly_rate_max_avg
├── hourly_rate_median
├── monthly_retainer_min_avg
├── monthly_retainer_max_avg
└── monthly_retainer_median
```

## Compensation Normalization

### Conversion Logic

```python
# Normalize all compensation to hourly AND monthly equivalents

def normalize_compensation(comp_type, comp_min, comp_max, hours_min, hours_max):
    """
    Returns: (hourly_min, hourly_max, monthly_min, monthly_max)
    
    Assumptions:
    - 4.33 weeks per month
    - If hours not specified, assume 15 hrs/week (midpoint of 10-20)
    - Annual rates assume full-time equivalent for comparison
    """
    
    if comp_type == 'hourly':
        hourly_min, hourly_max = comp_min, comp_max
        hours_avg = (hours_min + hours_max) / 2 if hours_min else 15
        monthly_min = hourly_min * hours_avg * 4.33
        monthly_max = hourly_max * hours_avg * 4.33
        
    elif comp_type == 'monthly':
        monthly_min, monthly_max = comp_min, comp_max
        hours_avg = (hours_min + hours_max) / 2 if hours_min else 15
        hourly_min = monthly_min / (hours_avg * 4.33)
        hourly_max = monthly_max / (hours_avg * 4.33)
        
    elif comp_type == 'annual':
        # Convert to FTE-equivalent hourly for comparison
        hourly_min = comp_min / 2080  # 40 hrs * 52 weeks
        hourly_max = comp_max / 2080
        monthly_min = comp_min / 12
        monthly_max = comp_max / 12
        
    return (hourly_min, hourly_max, monthly_min, monthly_max)
```

## Function Category Detection

```python
FUNCTION_PATTERNS = {
    'finance': ['cfo', 'chief financial', 'controller', 'fp&a', 'finance'],
    'marketing': ['cmo', 'chief marketing', 'marketing', 'brand', 'growth'],
    'sales': ['cro', 'chief revenue', 'sales', 'revenue', 'business development'],
    'product': ['cpo', 'chief product', 'product', 'pm'],
    'engineering': ['cto', 'chief technology', 'engineering', 'technical', 'architect'],
    'operations': ['coo', 'chief operating', 'operations', 'ops'],
    'people': ['chro', 'chief people', 'hr', 'human resources', 'talent', 'people'],
    'data': ['cdo', 'chief data', 'data science', 'analytics', 'ml'],
}

def detect_function(title: str) -> str:
    title_lower = title.lower()
    for function, patterns in FUNCTION_PATTERNS.items():
        if any(pattern in title_lower for pattern in patterns):
            return function
    return 'other'
```

## Seniority Tier Detection

```python
SENIORITY_PATTERNS = {
    'c_level': ['chief', 'ceo', 'cfo', 'cmo', 'cro', 'cto', 'coo', 'cpo', 'chro', 'cdo'],
    'evp': ['evp', 'executive vice president'],
    'svp': ['svp', 'senior vice president', 'sr vice president'],
    'vp': ['vp', 'vice president'],
    'director': ['director'],
    'head_of': ['head of', 'head,'],
}

def detect_seniority(title: str) -> str:
    title_lower = title.lower()
    for tier, patterns in SENIORITY_PATTERNS.items():
        if any(pattern in title_lower for pattern in patterns):
            return tier
    return 'unknown'
```

## Location Type Detection

```python
def detect_location_type(location_raw: str, description: str = '') -> tuple:
    """
    Returns: (location_type, location_restriction)
    
    location_type: remote / hybrid / onsite
    location_restriction: worldwide / usa_only / state_specific / timezone / city_specific
    """
    loc_lower = location_raw.lower()
    desc_lower = description.lower()
    
    # Detect type
    if 'remote' in loc_lower:
        location_type = 'remote'
    elif 'hybrid' in loc_lower:
        location_type = 'hybrid'
    else:
        location_type = 'onsite'
    
    # Detect restriction
    if location_type == 'remote':
        if 'worldwide' in loc_lower or 'global' in loc_lower:
            restriction = 'worldwide'
        elif any(state in loc_lower for state in US_STATES):
            restriction = 'state_specific'
        elif 'usa' in loc_lower or 'united states' in loc_lower or 'us only' in loc_lower:
            restriction = 'usa_only'
        elif any(tz in desc_lower for tz in ['pst', 'est', 'cst', 'mst', 'et ', 'pt ']):
            restriction = 'timezone'
        else:
            restriction = 'worldwide'  # Default for remote
    else:
        restriction = 'city_specific'
    
    return (location_type, restriction)
```

## Hours Detection (from description)

```python
import re

def extract_hours(description: str) -> tuple:
    """
    Extract hours per week from job description.
    Returns: (hours_min, hours_max)
    """
    patterns = [
        r'(\d+)\s*-\s*(\d+)\s*(?:hrs?|hours?)\s*(?:per|\/|a)?\s*week',
        r'(\d+)\s*(?:hrs?|hours?)\s*(?:per|\/|a)?\s*week',
        r'part[\s-]?time\s*\((\d+)\s*(?:hrs?|hours?)\)',
        r'up to (\d+)\s*(?:hrs?|hours?)',
        r'approximately (\d+)\s*(?:hrs?|hours?)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, description.lower())
        if match:
            groups = match.groups()
            if len(groups) == 2:
                return (int(groups[0]), int(groups[1]))
            elif len(groups) == 1:
                hours = int(groups[0])
                return (hours, hours)
    
    return (None, None)
```

## Output Analytics

### Weekly Snapshot Metrics

1. **Total Active Listings** by source
2. **New Listings This Week** 
3. **Listings Removed** (no longer active)
4. **Compensation Trends** by function and seniority
5. **Location Distribution** (remote vs hybrid vs onsite)
6. **Hours Commitment Distribution**
7. **Top Hiring Companies**

### Chart Data Exports

Following The CRO Report pattern:

```
output/
├── charts/
│   ├── comp_by_function.png
│   ├── comp_by_seniority.png  
│   ├── comp_by_location_type.png
│   ├── comp_by_hours_commitment.png
│   ├── listings_trend_90_days.png
│   └── listings_trend_all_time.png
├── data/
│   ├── weekly_snapshot_YYYY-MM-DD.csv
│   ├── all_active_listings.csv
│   └── compensation_benchmarks.csv
└── newsletter/
    └── draft_YYYY-MM-DD.md
```

## Scraping Schedule

```
Daily (GitHub Actions):
- Indeed: Full scrape of all search terms
- FractionalJobs.io: Full scrape
- The Free Agent: Full scrape

Weekly (Sunday):
- Generate compensation snapshots
- Create chart visualizations
- Export newsletter data package
- Trigger Crunchbase enrichment for new companies
```

## Rate Limiting & Ethics

- Indeed: 2-second delay between requests, rotate user agents
- FractionalJobs.io: 1-second delay, respect robots.txt
- The Free Agent: 1-second delay, respect robots.txt
- All sources: Cache responses, don't re-scrape same listing within 24 hours

## Tech Stack

- **Scraping**: Python + BeautifulSoup + requests
- **Database**: SQLite (portable) or PostgreSQL (production)
- **Scheduling**: GitHub Actions
- **Visualization**: matplotlib/seaborn (matching CRO Report style)
- **Hosting**: GitHub Pages for job board frontend
