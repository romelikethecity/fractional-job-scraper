"""
Utility functions for parsing and normalizing job listing data.
"""

import re
from typing import Optional, Tuple, Dict, List
from datetime import datetime, timedelta

# US States for location detection
US_STATES = [
    'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado',
    'connecticut', 'delaware', 'florida', 'georgia', 'hawaii', 'idaho',
    'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana',
    'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota',
    'mississippi', 'missouri', 'montana', 'nebraska', 'nevada',
    'new hampshire', 'new jersey', 'new mexico', 'new york',
    'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon',
    'pennsylvania', 'rhode island', 'south carolina', 'south dakota',
    'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington',
    'west virginia', 'wisconsin', 'wyoming'
]

US_STATE_ABBREVS = [
    'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga', 'hi', 'id',
    'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms',
    'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok',
    'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv',
    'wi', 'wy'
]

# Function category detection patterns
FUNCTION_PATTERNS = {
    'finance': [
        'cfo', 'chief financial', 'controller', 'fp&a', 'finance director',
        'vp finance', 'head of finance', 'financial', 'accounting'
    ],
    'marketing': [
        'cmo', 'chief marketing', 'marketing', 'brand', 'growth marketing',
        'demand gen', 'content', 'vp marketing', 'head of marketing'
    ],
    'sales': [
        'cro', 'chief revenue', 'sales', 'revenue', 'business development',
        'vp sales', 'head of sales', 'commercial', 'go-to-market', 'gtm'
    ],
    'product': [
        'cpo', 'chief product', 'product', 'vp product', 'head of product'
    ],
    'engineering': [
        'cto', 'chief technology', 'engineering', 'technical', 'architect',
        'vp engineering', 'head of engineering', 'software'
    ],
    'operations': [
        'coo', 'chief operating', 'operations', 'ops', 'vp operations',
        'head of operations'
    ],
    'people': [
        'chro', 'chief people', 'chief human', 'hr', 'human resources',
        'talent', 'people ops', 'vp people', 'head of people', 'head of hr'
    ],
    'data': [
        'cdo', 'chief data', 'data science', 'analytics', 'ml', 'ai',
        'machine learning', 'head of data', 'vp data'
    ],
    'legal': [
        'general counsel', 'legal', 'chief legal', 'clo', 'compliance'
    ],
}

# Seniority detection patterns (order matters - check most specific first)
SENIORITY_PATTERNS = [
    ('c_level', ['chief', ' ceo', ' cfo', ' cmo', ' cro', ' cto', ' coo', ' cpo', ' chro', ' cdo', ' clo']),
    ('evp', ['evp', 'executive vice president']),
    ('svp', ['svp', 'senior vice president', 'sr vice president', 'sr. vice president']),
    ('vp', [' vp ', 'vice president', ' vp,', 'vp of']),
    ('director', ['director']),
    ('head_of', ['head of', 'head,']),
]


def detect_function(title: str) -> str:
    """
    Detect the function category from job title.
    
    Args:
        title: Job title string
        
    Returns:
        Function category string (finance, marketing, sales, etc.)
    """
    title_lower = title.lower()
    
    for function, patterns in FUNCTION_PATTERNS.items():
        if any(pattern in title_lower for pattern in patterns):
            return function
    
    return 'other'


def detect_seniority(title: str) -> str:
    """
    Detect seniority tier from job title.
    
    Args:
        title: Job title string
        
    Returns:
        Seniority tier string (c_level, evp, svp, vp, director, head_of, unknown)
    """
    title_lower = ' ' + title.lower() + ' '  # Pad for word boundary matching
    
    for tier, patterns in SENIORITY_PATTERNS:
        if any(pattern in title_lower for pattern in patterns):
            return tier
    
    return 'unknown'


def detect_location_type(location_raw: str, description: str = '') -> Tuple[str, str, Optional[str]]:
    """
    Detect location type and restrictions from location string and description.
    
    Args:
        location_raw: Raw location string from listing
        description: Job description text
        
    Returns:
        Tuple of (location_type, location_restriction, state_if_applicable)
    """
    loc_lower = (location_raw or '').lower()
    desc_lower = (description or '').lower()
    combined = loc_lower + ' ' + desc_lower
    
    # Detect type
    if 'remote' in loc_lower:
        location_type = 'remote'
    elif 'hybrid' in loc_lower:
        location_type = 'hybrid'
    elif any(city in loc_lower for city in ['new york', 'san francisco', 'los angeles', 'chicago', 'boston', 'seattle', 'austin', 'denver']):
        location_type = 'onsite'
    else:
        # Check description for remote indicators
        if 'fully remote' in combined or 'remote position' in combined or '100% remote' in combined:
            location_type = 'remote'
        elif 'hybrid' in combined:
            location_type = 'hybrid'
        else:
            location_type = 'onsite'
    
    # Detect restriction
    state = None
    if location_type == 'remote':
        # Check for worldwide/global
        if 'worldwide' in combined or 'global' in combined or 'anywhere' in combined:
            restriction = 'worldwide'
        # Check for US only
        elif 'usa only' in combined or 'us only' in combined or 'united states only' in combined:
            restriction = 'usa_only'
        # Check for state-specific
        else:
            for i, state_name in enumerate(US_STATES):
                if state_name in loc_lower or state_name in desc_lower[:500]:
                    restriction = 'state_specific'
                    state = US_STATE_ABBREVS[i].upper()
                    break
            else:
                # Check abbreviations
                for abbrev in US_STATE_ABBREVS:
                    pattern = rf'\b{abbrev}\b'
                    if re.search(pattern, loc_lower, re.IGNORECASE):
                        restriction = 'state_specific'
                        state = abbrev.upper()
                        break
                else:
                    # Check for timezone requirements
                    if any(tz in combined for tz in ['pst', 'est', 'cst', 'mst', ' et ', ' pt ', ' ct ', ' mt ', 'eastern time', 'pacific time']):
                        restriction = 'timezone'
                    else:
                        restriction = 'usa_only'  # Default assumption for US job boards
    else:
        restriction = 'city_specific'
    
    return (location_type, restriction, state)


def extract_hours(text: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract hours per week from job description or title.
    
    Args:
        text: Text to search for hours information
        
    Returns:
        Tuple of (hours_min, hours_max) or (None, None) if not found
    """
    if not text:
        return (None, None)
    
    text_lower = text.lower()
    
    # Patterns to match hours per week
    patterns = [
        # "10-20 hours per week" or "10 - 20 hrs/week"
        r'(\d+)\s*[-–—to]+\s*(\d+)\s*(?:hrs?|hours?)\s*(?:per|\/|a)?\s*week',
        # "10 hours per week" or "10 hrs/week"
        r'(\d+)\s*(?:hrs?|hours?)\s*(?:per|\/|a)?\s*week',
        # "part-time (10 hours)"
        r'part[\s-]?time\s*\((\d+)\s*(?:hrs?|hours?)?\)',
        # "up to 20 hours"
        r'up\s+to\s+(\d+)\s*(?:hrs?|hours?)',
        # "approximately 15 hours"
        r'approximately\s+(\d+)\s*(?:hrs?|hours?)',
        # "15-20 hrs"
        r'(\d+)\s*[-–—to]+\s*(\d+)\s*hrs?',
        # "10 hrs" standalone
        r'\b(\d+)\s*hrs?\b',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text_lower)
        if match:
            groups = match.groups()
            if len(groups) == 2 and groups[1]:
                return (float(groups[0]), float(groups[1]))
            elif len(groups) >= 1 and groups[0]:
                hours = float(groups[0])
                # Sanity check - hours should be reasonable for fractional work
                if 1 <= hours <= 50:
                    return (hours, hours)
    
    return (None, None)


def parse_compensation(comp_string: str) -> Tuple[str, Optional[float], Optional[float]]:
    """
    Parse compensation string into type and min/max values.
    
    Args:
        comp_string: Raw compensation string (e.g., "$85 - $100 an hour")
        
    Returns:
        Tuple of (comp_type, min_value, max_value)
    """
    if not comp_string:
        return ('not_disclosed', None, None)
    
    comp_lower = comp_string.lower().replace(',', '')
    
    # Remove currency symbols and clean up
    comp_clean = re.sub(r'[^\d\s\-–—\.kyKY]', '', comp_string)
    
    # Extract numbers
    numbers = re.findall(r'[\d.]+[kK]?', comp_clean)
    
    if not numbers:
        return ('not_disclosed', None, None)
    
    # Convert K notation
    def parse_num(s):
        s = s.lower()
        if 'k' in s:
            return float(s.replace('k', '')) * 1000
        return float(s)
    
    try:
        if len(numbers) >= 2:
            min_val = parse_num(numbers[0])
            max_val = parse_num(numbers[1])
        else:
            min_val = max_val = parse_num(numbers[0])
    except ValueError:
        return ('not_disclosed', None, None)
    
    # Determine type based on keywords and value ranges
    if any(word in comp_lower for word in ['hour', 'hr', '/hr', 'hourly']):
        comp_type = 'hourly'
    elif any(word in comp_lower for word in ['month', 'mo', '/mo', 'monthly']):
        comp_type = 'monthly'
    elif any(word in comp_lower for word in ['year', 'yr', '/yr', 'annual', 'salary']):
        comp_type = 'annual'
    else:
        # Infer from value range
        if max_val and max_val < 1000:
            comp_type = 'hourly'  # Likely hourly rate
        elif max_val and max_val < 50000:
            comp_type = 'monthly'  # Likely monthly retainer
        else:
            comp_type = 'annual'  # Likely annual salary
    
    return (comp_type, min_val, max_val)


def normalize_compensation(
    comp_type: str,
    comp_min: Optional[float],
    comp_max: Optional[float],
    hours_min: Optional[float] = None,
    hours_max: Optional[float] = None
) -> Dict[str, Optional[float]]:
    """
    Normalize compensation to both hourly and monthly equivalents.
    
    Args:
        comp_type: Type of compensation (hourly, monthly, annual)
        comp_min: Minimum compensation value
        comp_max: Maximum compensation value
        hours_min: Minimum hours per week
        hours_max: Maximum hours per week
        
    Returns:
        Dict with hourly_min, hourly_max, monthly_min, monthly_max
    """
    result = {
        'hourly_min': None,
        'hourly_max': None,
        'monthly_min': None,
        'monthly_max': None
    }
    
    if not comp_min and not comp_max:
        return result
    
    # Use midpoint hours or assume 15 hrs/week if not specified
    hours_avg = 15.0
    if hours_min and hours_max:
        hours_avg = (hours_min + hours_max) / 2
    elif hours_min:
        hours_avg = hours_min
    elif hours_max:
        hours_avg = hours_max
    
    WEEKS_PER_MONTH = 4.33
    HOURS_PER_YEAR = 2080  # 40 hrs * 52 weeks (FTE equivalent)
    
    if comp_type == 'hourly':
        result['hourly_min'] = comp_min
        result['hourly_max'] = comp_max
        if comp_min:
            result['monthly_min'] = comp_min * hours_avg * WEEKS_PER_MONTH
        if comp_max:
            result['monthly_max'] = comp_max * hours_avg * WEEKS_PER_MONTH
            
    elif comp_type == 'monthly':
        result['monthly_min'] = comp_min
        result['monthly_max'] = comp_max
        if comp_min and hours_avg > 0:
            result['hourly_min'] = comp_min / (hours_avg * WEEKS_PER_MONTH)
        if comp_max and hours_avg > 0:
            result['hourly_max'] = comp_max / (hours_avg * WEEKS_PER_MONTH)
            
    elif comp_type == 'annual':
        # Convert to FTE-equivalent for comparison
        if comp_min:
            result['hourly_min'] = comp_min / HOURS_PER_YEAR
            result['monthly_min'] = comp_min / 12
        if comp_max:
            result['hourly_max'] = comp_max / HOURS_PER_YEAR
            result['monthly_max'] = comp_max / 12
    
    return result


def extract_experience_years(text: str) -> Optional[int]:
    """
    Extract years of experience requirement from description.
    
    Args:
        text: Job description text
        
    Returns:
        Minimum years required or None
    """
    if not text:
        return None
    
    patterns = [
        r'(\d+)\+?\s*(?:years?|yrs?)\s*(?:of)?\s*experience',
        r'minimum\s*(?:of)?\s*(\d+)\s*(?:years?|yrs?)',
        r'at\s*least\s*(\d+)\s*(?:years?|yrs?)',
        r'(\d+)\s*(?:years?|yrs?)\s*(?:of)?\s*(?:relevant|related|professional)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text.lower())
        if match:
            years = int(match.group(1))
            if 1 <= years <= 50:  # Sanity check
                return years
    
    return None


def normalize_company_name(name: str) -> str:
    """
    Normalize company name for matching across sources.
    
    Args:
        name: Raw company name
        
    Returns:
        Normalized company name
    """
    if not name:
        return ''
    
    # Lowercase
    normalized = name.lower().strip()
    
    # Remove common suffixes
    suffixes = [
        ', inc.', ', inc', ' inc.', ' inc',
        ', llc', ' llc',
        ', ltd.', ', ltd', ' ltd.', ' ltd',
        ', corp.', ', corp', ' corp.', ' corp',
        ' corporation',
        ', co.', ', co', ' co.',
        ' company',
    ]
    for suffix in suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)]
    
    # Remove special characters except spaces and alphanumerics
    normalized = re.sub(r'[^\w\s]', '', normalized)
    
    # Collapse multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized


def calculate_hours_bucket(hours_min: Optional[float], hours_max: Optional[float]) -> str:
    """
    Categorize hours into buckets for analysis.
    
    Args:
        hours_min: Minimum hours per week
        hours_max: Maximum hours per week
        
    Returns:
        Hours bucket string
    """
    if not hours_min and not hours_max:
        return 'not_specified'
    
    hours_avg = (hours_min or 0) + (hours_max or hours_min or 0)
    hours_avg = hours_avg / 2 if hours_max else hours_min
    
    if hours_avg <= 10:
        return '1-10'
    elif hours_avg <= 20:
        return '10-20'
    elif hours_avg <= 30:
        return '20-30'
    else:
        return '30-40'


def parse_date_posted(date_string: str) -> Optional[datetime]:
    """
    Parse relative or absolute date strings into datetime.
    
    Args:
        date_string: Date string like "3 days ago", "Just posted", "2024-01-15"
        
    Returns:
        datetime object or None
    """
    if not date_string:
        return None
    
    date_lower = date_string.lower().strip()
    now = datetime.utcnow()
    
    # Relative dates
    if 'just' in date_lower or 'today' in date_lower:
        return now
    
    if 'yesterday' in date_lower:
        return now - timedelta(days=1)
    
    # "X days ago"
    match = re.search(r'(\d+)\s*days?\s*ago', date_lower)
    if match:
        days = int(match.group(1))
        return now - timedelta(days=days)
    
    # "X weeks ago"
    match = re.search(r'(\d+)\s*weeks?\s*ago', date_lower)
    if match:
        weeks = int(match.group(1))
        return now - timedelta(weeks=weeks)
    
    # "X hours ago"
    match = re.search(r'(\d+)\s*hours?\s*ago', date_lower)
    if match:
        return now
    
    # Try parsing absolute dates
    date_formats = [
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%B %d, %Y',
        '%b %d, %Y',
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    
    return None


if __name__ == "__main__":
    # Test the parsing functions
    print("Testing parsing functions...")
    
    # Test function detection
    assert detect_function("Fractional CFO") == "finance"
    assert detect_function("Chief Marketing Officer") == "marketing"
    assert detect_function("VP of Sales") == "sales"
    print("✓ Function detection works")
    
    # Test seniority detection
    assert detect_seniority("Fractional CFO") == "c_level"
    assert detect_seniority("VP of Sales") == "vp"
    assert detect_seniority("Head of Marketing") == "head_of"
    print("✓ Seniority detection works")
    
    # Test hours extraction
    assert extract_hours("10-20 hours per week") == (10.0, 20.0)
    assert extract_hours("15 hrs/week") == (15.0, 15.0)
    assert extract_hours("up to 25 hours") == (25.0, 25.0)
    print("✓ Hours extraction works")
    
    # Test compensation parsing
    assert parse_compensation("$85 - $100 an hour") == ("hourly", 85.0, 100.0)
    assert parse_compensation("$8K-$10K/month") == ("monthly", 8000.0, 10000.0)
    print("✓ Compensation parsing works")
    
    print("\nAll tests passed!")
