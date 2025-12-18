"""
Main scraper orchestrator for fractional job board.

Coordinates scraping from multiple sources, deduplication, and database updates.
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.database import (
    create_database, get_session, FractionalJob, CompanyEnrichment,
    ListingSnapshot, ScrapeLog, CompensationSnapshot
)
from scrapers.indeed_scraper import IndeedScraper, job_to_db_dict
from scrapers.fractionaljobs_scraper import FractionalJobsScraper, job_to_db_dict as fj_to_db
from utils.parsers import normalize_company_name, calculate_hours_bucket


class FractionalJobOrchestrator:
    """Orchestrates scraping from multiple sources."""
    
    def __init__(self, db_url: str = None):
        """
        Initialize orchestrator.
        
        Args:
            db_url: Database connection URL. Defaults to SQLite.
        """
        self.db_url = db_url or os.environ.get(
            'DATABASE_URL', 
            'sqlite:///fractional_jobs.db'
        )
        self.engine = create_database(self.db_url)
        self.session = get_session(self.engine)
        
        # Initialize scrapers
        self.indeed_scraper = IndeedScraper(wait_time=60)
        self.fj_scraper = FractionalJobsScraper(delay_range=(1, 2))
    
    def _log_scrape_start(self, source: str) -> ScrapeLog:
        """Create scrape log entry."""
        log = ScrapeLog(
            source=source,
            started_at=datetime.utcnow(),
            status='running'
        )
        self.session.add(log)
        self.session.commit()
        return log
    
    def _log_scrape_end(self, log: ScrapeLog, status: str, 
                        found: int, new: int, updated: int, 
                        deactivated: int, error: str = None):
        """Update scrape log entry."""
        log.completed_at = datetime.utcnow()
        log.status = status
        log.listings_found = found
        log.listings_new = new
        log.listings_updated = updated
        log.listings_deactivated = deactivated
        log.error_message = error
        self.session.commit()
    
    def _upsert_job(self, job_dict: Dict) -> tuple:
        """
        Insert or update a job listing.
        
        Args:
            job_dict: Job data dictionary
            
        Returns:
            Tuple of (is_new, job_id)
        """
        existing = self.session.query(FractionalJob).filter(
            FractionalJob.source == job_dict['source'],
            FractionalJob.source_id == job_dict['source_id']
        ).first()
        
        if existing:
            # Update existing job
            for key, value in job_dict.items():
                if key not in ('id', 'date_scraped') and value is not None:
                    setattr(existing, key, value)
            existing.last_seen = datetime.utcnow()
            existing.is_active = True
            self.session.commit()
            return (False, existing.id)
        else:
            # Insert new job
            job = FractionalJob(**job_dict)
            self.session.add(job)
            self.session.commit()
            return (True, job.id)
    
    def _deactivate_stale_jobs(self, source: str, seen_ids: set, hours: int = 48):
        """
        Mark jobs as inactive if not seen recently.
        
        Args:
            source: Source name
            seen_ids: Set of source_ids seen in this scrape
            hours: Hours threshold for staleness
        """
        threshold = datetime.utcnow() - timedelta(hours=hours)
        
        stale_jobs = self.session.query(FractionalJob).filter(
            FractionalJob.source == source,
            FractionalJob.is_active == True,
            FractionalJob.last_seen < threshold,
            ~FractionalJob.source_id.in_(seen_ids) if seen_ids else True
        ).all()
        
        count = 0
        for job in stale_jobs:
            job.is_active = False
            count += 1
        
        self.session.commit()
        return count
    
    def scrape_indeed(self, max_pages_per_query: int = 3) -> Dict:
        """
        Scrape Indeed for fractional jobs.
        
        Args:
            max_pages_per_query: Max pages per search term
            
        Returns:
            Stats dictionary
        """
        log = self._log_scrape_start('indeed')
        
        stats = {'found': 0, 'new': 0, 'updated': 0, 'errors': 0}
        seen_ids = set()
        
        try:
            for job in self.indeed_scraper.scrape_all(max_results_per_search=50):
                stats['found'] += 1
                job_url = job.get('job_url', '')
                seen_ids.add(job_url)
                
                try:
                    job_dict = job_to_db_dict(job)
                    is_new, job_id = self._upsert_job(job_dict)
                    
                    if is_new:
                        stats['new'] += 1
                    else:
                        stats['updated'] += 1
                        
                except Exception as e:
                    print(f"Error processing Indeed job: {e}")
                    stats['errors'] += 1
            
            # Deactivate stale jobs
            deactivated = self._deactivate_stale_jobs('indeed', seen_ids)
            stats['deactivated'] = deactivated
            
            self._log_scrape_end(
                log, 'success', 
                stats['found'], stats['new'], stats['updated'], deactivated
            )
            
        except Exception as e:
            self._log_scrape_end(log, 'failed', 0, 0, 0, 0, str(e))
            raise
        
        return stats
    
    def scrape_fractionaljobs(self) -> Dict:
        """
        Scrape FractionalJobs.io.
        
        Returns:
            Stats dictionary
        """
        log = self._log_scrape_start('fractionaljobs')
        
        stats = {'found': 0, 'new': 0, 'updated': 0, 'errors': 0}
        seen_ids = set()
        
        try:
            for job in self.fj_scraper.scrape_all():
                stats['found'] += 1
                seen_ids.add(job.source_id)
                
                try:
                    job_dict = fj_to_db(job)
                    is_new, job_id = self._upsert_job(job_dict)
                    
                    if is_new:
                        stats['new'] += 1
                    else:
                        stats['updated'] += 1
                        
                except Exception as e:
                    print(f"Error processing FractionalJobs listing {job.source_id}: {e}")
                    stats['errors'] += 1
            
            # Deactivate stale jobs
            deactivated = self._deactivate_stale_jobs('fractionaljobs', seen_ids)
            stats['deactivated'] = deactivated
            
            self._log_scrape_end(
                log, 'success',
                stats['found'], stats['new'], stats['updated'], deactivated
            )
            
        except Exception as e:
            self._log_scrape_end(log, 'failed', 0, 0, 0, 0, str(e))
            raise
        
        return stats
    
    def create_daily_snapshot(self) -> Dict:
        """
        Create daily statistics snapshot for trend analysis.
        
        Returns:
            Snapshot data dictionary
        """
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Get active jobs
        active_jobs = self.session.query(FractionalJob).filter(
            FractionalJob.is_active == True
        ).all()
        
        # Calculate breakdowns
        by_function = defaultdict(int)
        by_seniority = defaultdict(int)
        by_location_type = defaultdict(int)
        by_hours = defaultdict(int)
        by_source = defaultdict(int)
        
        comp_disclosed = 0
        
        for job in active_jobs:
            by_function[job.function_category or 'other'] += 1
            by_seniority[job.seniority_tier or 'unknown'] += 1
            by_location_type[job.location_type or 'unknown'] += 1
            by_source[job.source] += 1
            
            # Hours bucket
            bucket = calculate_hours_bucket(job.hours_per_week_min, job.hours_per_week_max)
            by_hours[bucket] += 1
            
            # Compensation disclosure
            if job.compensation_type and job.compensation_type != 'not_disclosed':
                comp_disclosed += 1
        
        total = len(active_jobs)
        
        # Get new today
        new_today = self.session.query(FractionalJob).filter(
            FractionalJob.date_scraped >= today
        ).count()
        
        # Get removed today (deactivated)
        removed_today = self.session.query(FractionalJob).filter(
            FractionalJob.is_active == False,
            FractionalJob.last_seen >= today - timedelta(days=1),
            FractionalJob.last_seen < today
        ).count()
        
        # Create snapshot
        snapshot = ListingSnapshot(
            snapshot_date=today,
            source='all',
            total_active=total,
            new_today=new_today,
            removed_today=removed_today,
            by_function=json.dumps(dict(by_function)),
            by_seniority=json.dumps(dict(by_seniority)),
            by_location_type=json.dumps(dict(by_location_type)),
            by_hours_bucket=json.dumps(dict(by_hours)),
            comp_disclosed_count=comp_disclosed,
            comp_disclosed_pct=comp_disclosed / total * 100 if total > 0 else 0
        )
        self.session.add(snapshot)
        self.session.commit()
        
        return {
            'date': today.isoformat(),
            'total_active': total,
            'new_today': new_today,
            'removed_today': removed_today,
            'by_function': dict(by_function),
            'by_seniority': dict(by_seniority),
            'by_location_type': dict(by_location_type),
            'by_hours': dict(by_hours),
            'comp_disclosed_pct': comp_disclosed / total * 100 if total > 0 else 0
        }
    
    def create_compensation_snapshot(self) -> List[Dict]:
        """
        Create weekly compensation statistics by dimension.
        
        Returns:
            List of compensation snapshot data
        """
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Get active jobs with compensation data
        jobs_with_comp = self.session.query(FractionalJob).filter(
            FractionalJob.is_active == True,
            FractionalJob.hourly_rate_min.isnot(None)
        ).all()
        
        snapshots = []
        
        # Group by function category
        by_function = defaultdict(list)
        for job in jobs_with_comp:
            func = job.function_category or 'other'
            by_function[func].append(job)
        
        for func, jobs in by_function.items():
            if len(jobs) >= 3:  # Minimum sample size
                hourly_rates = [j.hourly_rate_min for j in jobs if j.hourly_rate_min]
                hourly_rates_max = [j.hourly_rate_max for j in jobs if j.hourly_rate_max]
                
                snapshot = CompensationSnapshot(
                    snapshot_date=today,
                    function_category=func,
                    sample_size=len(jobs),
                    hourly_rate_min_avg=sum(hourly_rates) / len(hourly_rates),
                    hourly_rate_max_avg=sum(hourly_rates_max) / len(hourly_rates_max) if hourly_rates_max else None,
                    hourly_rate_median=sorted(hourly_rates)[len(hourly_rates) // 2],
                )
                self.session.add(snapshot)
                snapshots.append({
                    'function': func,
                    'sample_size': len(jobs),
                    'hourly_rate_avg': snapshot.hourly_rate_min_avg,
                    'hourly_rate_median': snapshot.hourly_rate_median,
                })
        
        self.session.commit()
        return snapshots
    
    def export_active_listings_csv(self, filepath: str = None) -> str:
        """
        Export all active listings to CSV.
        
        Args:
            filepath: Output file path
            
        Returns:
            Path to created file
        """
        import csv
        
        filepath = filepath or f"output/data/active_listings_{datetime.now().strftime('%Y-%m-%d')}.csv"
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        jobs = self.session.query(FractionalJob).filter(
            FractionalJob.is_active == True
        ).order_by(FractionalJob.date_posted.desc()).all()
        
        fieldnames = [
            'id', 'source', 'title', 'company_name', 'location_raw',
            'location_type', 'location_restriction', 'compensation_type',
            'compensation_min', 'compensation_max', 'hourly_rate_min',
            'hourly_rate_max', 'hours_per_week_min', 'hours_per_week_max',
            'function_category', 'seniority_tier', 'date_posted',
            'source_url'
        ]
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for job in jobs:
                row = {field: getattr(job, field, '') for field in fieldnames}
                row['date_posted'] = job.date_posted.isoformat() if job.date_posted else ''
                writer.writerow(row)
        
        print(f"Exported {len(jobs)} listings to {filepath}")
        return filepath
    
    def export_weekly_summary(self) -> Dict:
        """
        Generate weekly summary for newsletter.
        
        Returns:
            Summary data dictionary
        """
        # Get snapshots from last 7 days
        week_ago = datetime.utcnow() - timedelta(days=7)
        
        snapshots = self.session.query(ListingSnapshot).filter(
            ListingSnapshot.snapshot_date >= week_ago,
            ListingSnapshot.source == 'all'
        ).order_by(ListingSnapshot.snapshot_date).all()
        
        if not snapshots:
            return {}
        
        latest = snapshots[-1]
        earliest = snapshots[0]
        
        # Calculate week-over-week change
        wow_change = latest.total_active - earliest.total_active
        wow_pct = (wow_change / earliest.total_active * 100) if earliest.total_active else 0
        
        # Get top functions
        by_function = json.loads(latest.by_function) if latest.by_function else {}
        top_functions = sorted(by_function.items(), key=lambda x: x[1], reverse=True)[:5]
        
        return {
            'week_ending': latest.snapshot_date.isoformat(),
            'total_active': latest.total_active,
            'wow_change': wow_change,
            'wow_change_pct': wow_pct,
            'new_this_week': sum(s.new_today or 0 for s in snapshots),
            'removed_this_week': sum(s.removed_today or 0 for s in snapshots),
            'top_functions': top_functions,
            'comp_transparency_pct': latest.comp_disclosed_pct,
            'by_location_type': json.loads(latest.by_location_type) if latest.by_location_type else {},
        }
    
    def run_daily_scrape(self):
        """Run full daily scrape workflow."""
        print(f"Starting daily scrape at {datetime.utcnow().isoformat()}")
        print("=" * 60)
        
        # Scrape Indeed
        print("\n[1/3] Scraping Indeed...")
        indeed_stats = self.scrape_indeed(max_pages_per_query=3)
        print(f"Indeed: Found {indeed_stats['found']}, New {indeed_stats['new']}, "
              f"Updated {indeed_stats['updated']}, Deactivated {indeed_stats.get('deactivated', 0)}")
        
        # Scrape FractionalJobs.io
        print("\n[2/3] Scraping FractionalJobs.io...")
        fj_stats = self.scrape_fractionaljobs()
        print(f"FractionalJobs: Found {fj_stats['found']}, New {fj_stats['new']}, "
              f"Updated {fj_stats['updated']}, Deactivated {fj_stats.get('deactivated', 0)}")
        
        # Create daily snapshot
        print("\n[3/3] Creating daily snapshot...")
        snapshot = self.create_daily_snapshot()
        print(f"Snapshot: {snapshot['total_active']} active listings")
        print(f"  By function: {snapshot['by_function']}")
        print(f"  Compensation disclosed: {snapshot['comp_disclosed_pct']:.1f}%")
        
        # Export CSV
        print("\nExporting active listings CSV...")
        self.export_active_listings_csv()
        
        print("\n" + "=" * 60)
        print(f"Daily scrape completed at {datetime.utcnow().isoformat()}")
        
        return {
            'indeed': indeed_stats,
            'fractionaljobs': fj_stats,
            'snapshot': snapshot
        }


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description='Fractional Job Scraper')
    parser.add_argument('--action', choices=['scrape', 'snapshot', 'export', 'weekly'],
                        default='scrape', help='Action to perform')
    parser.add_argument('--source', choices=['indeed', 'fractionaljobs', 'all'],
                        default='all', help='Source to scrape')
    parser.add_argument('--db-url', help='Database URL')
    parser.add_argument('--max-pages', type=int, default=3,
                        help='Max pages per query for Indeed')
    
    args = parser.parse_args()
    
    orchestrator = FractionalJobOrchestrator(db_url=args.db_url)
    
    if args.action == 'scrape':
        if args.source == 'all':
            orchestrator.run_daily_scrape()
        elif args.source == 'indeed':
            stats = orchestrator.scrape_indeed(args.max_pages)
            print(f"Indeed scrape complete: {stats}")
        elif args.source == 'fractionaljobs':
            stats = orchestrator.scrape_fractionaljobs()
            print(f"FractionalJobs scrape complete: {stats}")
    
    elif args.action == 'snapshot':
        snapshot = orchestrator.create_daily_snapshot()
        print(f"Snapshot created: {json.dumps(snapshot, indent=2)}")
    
    elif args.action == 'export':
        filepath = orchestrator.export_active_listings_csv()
        print(f"Exported to: {filepath}")
    
    elif args.action == 'weekly':
        summary = orchestrator.export_weekly_summary()
        print(f"Weekly summary: {json.dumps(summary, indent=2)}")


if __name__ == "__main__":
    main()
