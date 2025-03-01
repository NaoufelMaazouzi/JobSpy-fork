from __future__ import annotations

import random
import time
import concurrent.futures
from typing import List, Optional

from bs4 import BeautifulSoup

from jobspy.model import (
    Scraper,
    ScraperInput,
    Site,
    JobPost,
    JobResponse,
    Location,
    Country,
)
from jobspy.util import (
    create_logger, 
    create_session, 
    is_valid_bayt_country, 
    is_valid_bayt_city, 
    format_bayt_location
)

log = create_logger("Bayt")


class BaytScraper(Scraper):
    base_url = "https://www.bayt.com"
    delay = 2
    band_delay = 3
    max_workers_limit = 20  # Maximum limit for worker threads

    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None
    ):
        super().__init__(Site.BAYT, proxies=proxies, ca_cert=ca_cert)
        self.scraper_input = None
        self.session = None
        self.country_enum = Country.WORLDWIDE

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        self.scraper_input = scraper_input
        self.session = create_session(
            proxies=self.proxies, ca_cert=self.ca_cert, is_tls=False, has_retry=True
        )
        job_list: list[JobPost] = []
        results_wanted = (
            scraper_input.results_wanted if scraper_input.results_wanted else 10
        )
        
        # Extract city from location if available
        city = None
        if scraper_input.location:
            city = scraper_input.location
            # Validate city
            if not is_valid_bayt_city(city):
                log.info(f"City '{city}' is not in the list of valid Bayt cities. Search results may be limited.")
            
        # Get country from scraper_input
        country = None
        if hasattr(scraper_input, 'country') and scraper_input.country:
            # Use the first part of the name (before any comma)
            country_name = scraper_input.country.name.lower().split(',')[0]
            country = country_name
            if country == "unitedarabemirates":
                country = "uae"
            # Validate country
            if not is_valid_bayt_country(country):
                log.info(f"Country '{country}' is not in the list of valid Bayt countries. Using 'international' instead.")
                country = "international"
            # Store the country enum in self.country_enum for _extract_job_info
            self.country_enum = scraper_input.country
        else:
            # Default to international if no country is specified
            country = "international"
            self.country_enum = Country.WORLDWIDE
                
        log.info(f"COUNTRY: {country}")

        # Start with fetching the first page to determine if there are results
        log.info("Fetching Bayt jobs page 1")
        first_page_result = self._fetch_jobs(self.scraper_input.search_term, country, city, 1)
        first_page_elements, total_jobs = first_page_result
        
        if not first_page_elements:
            return JobResponse(jobs=[])
        
        # Calculate total number of pages based on total jobs (20 jobs per page)
        jobs_per_page = 20
        total_pages = (total_jobs + jobs_per_page - 1) // jobs_per_page  # Ceiling division
        log.info(f"Total jobs: {total_jobs}, Total pages: {total_pages}")
        
        # Dynamically set max_workers for first page based on number of job elements
        first_page_max_workers = min(self.max_workers_limit, len(first_page_elements))
        log.info(f"Processing first page with {first_page_max_workers} workers")
            
        # Use ThreadPoolExecutor to process first page jobs in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=first_page_max_workers) as executor:
            # Process first page jobs
            first_page_futures = [
                executor.submit(self._extract_job_info, job)
                for job in first_page_elements
            ]
            
            # Add valid job posts from first page
            for future in concurrent.futures.as_completed(first_page_futures):
                try:
                    job_post = future.result()
                    if job_post:
                        job_list.append(job_post)
                        if len(job_list) >= results_wanted:
                            break
                except Exception as e:
                    log.error(f"Bayt: Error extracting job info: {str(e)}")
        
        # If we need more results, fetch additional pages in parallel
        if len(job_list) < results_wanted and total_pages > 1:
            # Calculate how many more pages we need to fetch
            # We already fetched page 1, so we start from page 2
            # We only need to fetch enough pages to get results_wanted jobs
            remaining_results_needed = results_wanted - len(job_list)
            estimated_pages_needed = (remaining_results_needed + jobs_per_page - 1) // jobs_per_page
            
            # Limit to actual available pages
            max_additional_pages = min(estimated_pages_needed, total_pages - 1)
            log.info(f"Need to fetch {max_additional_pages} more pages to get {remaining_results_needed} more results")
            
            # Generate list of pages to fetch (starting from page 2)
            pages_to_fetch = list(range(2, 2 + max_additional_pages))
            
            # Dynamically set max_workers for page fetching
            page_max_workers = min(self.max_workers_limit, len(pages_to_fetch))
            log.info(f"Fetching additional pages with {page_max_workers} workers")
            
            # Use ThreadPoolExecutor to fetch pages in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=page_max_workers) as page_executor:
                # Submit page fetching tasks
                page_futures = {
                    page_executor.submit(
                        lambda p: self._fetch_jobs(self.scraper_input.search_term, country, city, p)[0],
                        page
                    ): page
                    for page in pages_to_fetch
                }
                
                # Process results as they complete
                for future in concurrent.futures.as_completed(page_futures):
                    page = page_futures[future]
                    try:
                        job_elements = future.result()
                        if not job_elements:
                            continue
                            
                        log.info(f"Processing results from page {page}")
                        
                        # Dynamically set max_workers for job processing based on number of jobs
                        job_max_workers = min(self.max_workers_limit, len(job_elements))
                        log.info(f"Processing page {page} with {job_max_workers} workers")
                        
                        # Process jobs from this page in parallel
                        with concurrent.futures.ThreadPoolExecutor(max_workers=job_max_workers) as job_executor:
                            job_futures = [
                                job_executor.submit(self._extract_job_info, job)
                                for job in job_elements
                            ]
                            
                            # Collect results
                            for job_future in concurrent.futures.as_completed(job_futures):
                                try:
                                    job_post = job_future.result()
                                    if job_post:
                                        job_list.append(job_post)
                                        if len(job_list) >= results_wanted:
                                            break
                                except Exception as e:
                                    log.error(f"Bayt: Error extracting job info: {str(e)}")
                                    
                            if len(job_list) >= results_wanted:
                                break
                                
                    except Exception as e:
                        log.error(f"Bayt: Error fetching page {page}: {str(e)}")

        # Ensure we don't return more jobs than requested
        job_list = job_list[:results_wanted]
        return JobResponse(jobs=job_list)

    def _fetch_jobs(self, query: str, country: str, city: str, page: int) -> tuple[list | None, int]:
        """
        Grabs the job results for the given query and page number.
        
        Returns:
            tuple: (job_listings, total_jobs_count)
                - job_listings: List of job elements or None if error
                - total_jobs_count: Total number of jobs found (0 if not found)
        """
        try:
            # Format query by replacing spaces with hyphens for multi-word queries
            formatted_query = format_bayt_location(query) if query else ""
            
            # Format country and city
            formatted_country = format_bayt_location(country) if country else ""
            
            # Only use city in URL if it's valid
            formatted_city = ""
            if city and is_valid_bayt_city(city):
                formatted_city = format_bayt_location(city)
            
            # Handle all possible combinations of country and city
            if formatted_country and formatted_city:
                # Both country and city are available and city is valid
                url = f"{self.base_url}/en/{formatted_country}/jobs/{formatted_query}-jobs-in-{formatted_city}/?page={page}"
            elif formatted_country:
                # Only country is available or city is invalid
                url = f"{self.base_url}/en/{formatted_country}/jobs/{formatted_query}-jobs/?page={page}"
            elif formatted_city:
                # Only city is available (use international as default country)
                url = f"{self.base_url}/en/international/jobs/{formatted_query}-jobs-in-{formatted_city}/?page={page}"
            else:
                # Neither country nor city is available
                url = f"{self.base_url}/en/international/jobs/{formatted_query}-jobs/?page={page}"
            
            log.info(f"URLLLLLLL: {url}")
                
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Extract total number of jobs found
            total_jobs = 0
            jobs_found_element = soup.find("b", attrs={"data-automation-id": "XJobsFound"})
            if jobs_found_element:
                jobs_text = jobs_found_element.get_text(strip=True)
                # Extract the number from text like "8 jobs found"
                try:
                    total_jobs = int(jobs_text.split()[0])
                    log.info(f"Total jobs found: {total_jobs}")
                except (ValueError, IndexError):
                    log.warning(f"Could not parse total jobs count from: {jobs_text}")
            
            job_listings = soup.find_all("li", attrs={"data-js-job": ""})
            log.debug(f"Found {len(job_listings)} job listing elements on page {page}")
            return job_listings, total_jobs
        except Exception as e:
            log.error(f"Bayt: Error fetching jobs - {str(e)}")
            return None, 0

    def _extract_job_info(self, job: BeautifulSoup) -> JobPost | None:
        """
        Extracts the job information from a single job listing.
        """
        # Find the h2 element holding the title and link (no class filtering)
        job_general_information = job.find("h2")
        if not job_general_information:
            return

        job_title = job_general_information.get_text(strip=True)
        job_url = self._extract_job_url(job_general_information)
        if not job_url:
            return

        # Extract company name using the original approach:
        company_tag = job.find("div", class_="t-nowrap p10l")
        company_name = (
            company_tag.find("span").get_text(strip=True)
            if company_tag and company_tag.find("span")
            else None
        )

        # Extract location using the original approach:
        location_tag = job.find("div", class_="t-mute t-small")
        location = location_tag.get_text(strip=True) if location_tag else None

        job_id = f"bayt-{abs(hash(job_url))}"
        
        location_obj = Location(
            city=location,
            country=self.country_enum,
        )
        return JobPost(
            id=job_id,
            title=job_title,
            company_name=company_name,
            location=location_obj,
            job_url=job_url,
        )

    def _extract_job_url(self, job_general_information: BeautifulSoup) -> str | None:
        """
        Pulls the job URL from the 'a' within the h2 element.
        """
        a_tag = job_general_information.find("a")
        if a_tag and a_tag.has_attr("href"):
            return self.base_url + a_tag["href"].strip()
