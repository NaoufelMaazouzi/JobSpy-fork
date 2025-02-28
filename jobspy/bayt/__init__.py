from __future__ import annotations

import random
import time

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
from jobspy.util import create_logger, create_session

log = create_logger("Bayt")


class BaytScraper(Scraper):
    base_url = "https://www.bayt.com"
    delay = 2
    band_delay = 3

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
        page = 1
        results_wanted = (
            scraper_input.results_wanted if scraper_input.results_wanted else 10
        )
        
        # Extract city from location if available
        city = None
        if scraper_input.location:
            city = scraper_input.location
            
        # Get country from scraper_input
        country = None
        if hasattr(scraper_input, 'country') and scraper_input.country:
            # Use the first part of the name (before any comma)
            country_name = scraper_input.country.name.lower().split(',')[0]
            country = country_name
            # Store the country enum in self.country_enum for _extract_job_info
            self.country_enum = scraper_input.country
        else:
            # Default to international if no country is specified
            country = "international"
            self.country_enum = Country.WORLDWIDE
                
        log.info(f"COUNTRY: {country}")

        while len(job_list) < results_wanted:
            log.info(f"Fetching Bayt jobs page {page}")
            job_elements = self._fetch_jobs(self.scraper_input.search_term, country, city, page)
            if not job_elements:
                break

            if job_elements:
                log.debug(
                    "First job element snippet:\n" + job_elements[0].prettify()[:500]
                )

            initial_count = len(job_list)
            for job in job_elements:
                try:
                    job_post = self._extract_job_info(job)
                    if job_post:
                        job_list.append(job_post)
                        if len(job_list) >= results_wanted:
                            break
                    else:
                        log.debug(
                            "Extraction returned None. Job snippet:\n"
                            + job.prettify()[:500]
                        )
                except Exception as e:
                    log.error(f"Bayt: Error extracting job info: {str(e)}")
                    continue

            if len(job_list) == initial_count:
                log.info(f"No new jobs found on page {page}. Ending pagination.")
                break

            page += 1
            time.sleep(random.uniform(self.delay, self.delay + self.band_delay))

        job_list = job_list[: scraper_input.results_wanted]
        return JobResponse(jobs=job_list)

    def _fetch_jobs(self, query: str, country: str, city: str, page: int) -> list | None:
        """
        Grabs the job results for the given query and page number.
        """
        try:
            # Format query by replacing spaces with hyphens for multi-word queries
            formatted_query = query.replace(" ", "-") if query else ""
            
            # Handle all possible combinations of country and city
            if country and country.strip() and city and city.strip():
                # Both country and city are available
                url = f"{self.base_url}/en/{country}/jobs/{formatted_query}-jobs-in-{city}/?page={page}"
            elif country and country.strip():
                # Only country is available
                url = f"{self.base_url}/en/{country}/jobs/{formatted_query}-jobs/?page={page}"
            elif city and city.strip():
                # Only city is available (use international as default country)
                url = f"{self.base_url}/en/international/jobs/{formatted_query}-jobs-in-{city}/?page={page}"
            else:
                # Neither country nor city is available
                url = f"{self.base_url}/en/international/jobs/{formatted_query}-jobs/?page={page}"
            
            log.info(f"URLLLLLLL: {url}")
                
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            job_listings = soup.find_all("li", attrs={"data-js-job": ""})
            log.debug(f"Found {len(job_listings)} job listing elements")
            return job_listings
        except Exception as e:
            log.error(f"Bayt: Error fetching jobs - {str(e)}")
            return None

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
