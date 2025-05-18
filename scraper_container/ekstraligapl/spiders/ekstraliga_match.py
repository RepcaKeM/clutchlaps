import scrapy
import re
from pathlib import Path
from scrapy.http import HtmlResponse
import psycopg2
import os
import json
import datetime

class ScheduleSpider(scrapy.Spider):
    """
    Spider for scraping speedway match data from the Ekstraliga website.

    This spider crawls the schedule page to find match links, then visits each match page
    to extract detailed information including match results, team lineups, heat-by-heat details,
    and telemetry data for each rider (using Playwright for dynamic content).
    """
    """
            "https://ekstraliga.pl/en/se/fixtures-and-results/m2e/2025",
        "https://ekstraliga.pl/en/se/fixtures-and-results/m2e/2024",
        "https://ekstraliga.pl/en/se/fixtures-and-results/m2e/2023",
        "https://ekstraliga.pl/en/se/fixtures-and-results/m2e/2022",
        "https://ekstraliga.pl/en/se/fixtures-and-results/m2e/2021",
        "https://ekstraliga.pl/en/se/fixtures-and-results/m2e/2020",
        "https://ekstraliga.pl/en/se/fixtures-and-results/m2e/2019",
        "https://ekstraliga.pl/en/se/fixtures-and-results/m2e/2018",
        "https://ekstraliga.pl/en/se/fixtures-and-results/m2e/2017",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2010",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2011",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2012",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2013",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2014",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2015",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2016",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2017",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2018",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2019",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2020",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2021",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2022",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2023",
        "https://ekstraliga.pl/en/se/fixtures-and-results/pgee/2024","""
    name = "ekstraliga_match"
    allowed_domains = ["ekstraliga.pl"]
    # start_urls removed, will be fetched from DB in start_requests

    # Settings for quieter logging and Playwright configuration
    custom_settings = {
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': f'logs/ekstraliga_scraper_logs_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log',  # Log file name with timestamp
        'LOG_ENABLED': True,
        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
        'PLAYWRIGHT_MAX_CONTEXTS': 8,
        'PLAYWRIGHT_MAX_PAGES_PER_CONTEXT': 1,
        # 'DOWNLOAD_TIMEOUT': 120, # Default is 180s. Adjust if needed.
        # Playwright default page.goto timeout is 30s.
        # scrapy-playwright's default navigation timeout is handler's page.goto_timeout or PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT (60s)
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ScheduleSpider, cls).from_crawler(crawler, *args, **kwargs)
        from scrapy import signals
        crawler.signals.connect(spider.open_spider, signal=signals.spider_opened)
        crawler.signals.connect(spider.close_spider, signal=signals.spider_closed)
        spider.conn = None
        spider.cursor = None
        spider.urls_to_process = [] # List of (url, is_current_season) tuples
        spider.pending_match_requests = {} # {starting_url: {match_url1, match_url2, ...}}
        spider.start_url_meta = {} # {starting_url: is_current_season}
        return spider

    # Database connection parameters
    DB_HOST = os.environ.get('POSTGRES_HOST', 'db')
    DB_NAME = os.environ.get('POSTGRES_DB', 'speedway_db')
    DB_USER = os.environ.get('POSTGRES_USER', 'speedway_user')
    DB_PASSWORD = os.environ.get('PGPASSWORD', 'speedgres_password')

    def open_spider(self, spider):
        """Opens database connection when the spider starts."""
        try:
            self.conn = psycopg2.connect(host=self.DB_HOST, dbname=self.DB_NAME, user=self.DB_USER, password=self.DB_PASSWORD)
            self.cursor = self.conn.cursor()
            self.logger.info("Database connection opened.")

            # Fetch URLs to scrape
            try:
                self.cursor.execute("SELECT url, is_current_season FROM public.data_source WHERE is_scraped = false")
                self.urls_to_process = self.cursor.fetchall()
                if not self.urls_to_process:
                    self.logger.warning("No URLs found in data_source table to scrape (is_scraped=false).")
                else:
                    self.logger.info(f"Fetched {len(self.urls_to_process)} URLs to scrape from the database.")
            except psycopg2.Error as e:
                self.logger.error(f"Error fetching URLs from database: {e}")
                self.urls_to_process = [] # Ensure it's empty on error

        except psycopg2.Error as e:
            self.logger.error(f"Error connecting to PostgreSQL Database: {e}")
            self.conn = None
            self.cursor = None

    def start_requests(self):
        """Generates initial requests from URLs fetched from the database."""
        if not self.cursor:
            self.logger.error("No database connection. Cannot start requests.")
            return # Stop if DB connection failed in open_spider

        if not self.urls_to_process:
            self.logger.info("No URLs to process from database.")
            return

        for url, is_current in self.urls_to_process:
            self.logger.info(f"Queueing starting URL from DB: {url} (Is current season: {is_current})")
            # Initialize tracking for this starting URL
            self.start_url_meta[url] = is_current
            self.pending_match_requests[url] = set()
            yield scrapy.Request(url, callback=self.parse, meta={'starting_url': url})

    def close_spider(self, spider):
        """Closes database connection when the spider finishes."""
        if self.cursor:
            self.cursor.close()
            self.logger.info("Database cursor closed.")
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed.")

    def parse(self, response):
        """
        Parse the schedule page (a starting URL) to find and follow links to individual match pages.
        """
        starting_url = response.meta.get('starting_url')
        if not starting_url:
            self.logger.error(f"Missing 'starting_url' in meta for response {response.url}. Cannot track completion.")
            return

        self.logger.info(f"Parsing schedule page: {response.url} (Originating from: {starting_url})")
        match_containers_selector = 'a.relative.flex.items-center.justify-between'
        self.logger.debug(f"Using CSS selector to find match containers: {match_containers_selector}")
        match_containers = response.css(match_containers_selector)

        if not match_containers:
            self.logger.warning("No match containers found using the specified CSS selector on schedule page.")
            return

        found_count = 0
        match_url_pattern = re.compile(r'/se/mecz/\d{4}$')

        for container in match_containers:
            href = container.css('::attr(href)').get()
            match_url = None
            is_match_link = False

            if href and match_url_pattern.search(href):
                is_match_link = True
                match_url = response.urljoin(href)
                self.logger.debug(f"Found valid match link: {href}")
            elif href:
                self.logger.debug(f"Found link that is NOT a match link: {href}. Skipping.")

            if not is_match_link:
                self.logger.debug(f"Skipping container as href is missing or not a match link: {href}")
                continue

            # Check if the match_url already exists in the database
            if self.cursor:
                try:
                    self.cursor.execute("SELECT 1 FROM matches WHERE match_url = %s", (match_url,))
                    exists = self.cursor.fetchone()
                    if exists:
                        self.logger.info(f"Match URL already exists in database, skipping: {match_url}")
                        continue # Skip this URL if it's already in the database
                except psycopg2.Error as e:
                    self.logger.error(f"Database error while checking URL {match_url}: {e}. Proceeding with scraping.")
            else:
                self.logger.warning(f"Database connection not available. Cannot check for existing URLs. Proceeding with scraping {match_url}.")


            found_count += 1
            date_time_summary = container.css('div.flex.flex-1 > div:nth-child(2) span.schedule-events__label:not(.schedule-events__attendance) span::text').get()
            attendance_summary = container.css('div.flex.flex-1 > div:nth-child(2) span.schedule-events__attendance::text').get()

            date_time_summary = date_time_summary.strip() if date_time_summary else None
            attendance_summary = attendance_summary.strip() if attendance_summary else None

            # Add match_url to the tracking set for its starting_url
            if starting_url in self.pending_match_requests:
                self.pending_match_requests[starting_url].add(match_url)
                self.logger.debug(f"Added {match_url} to pending requests for {starting_url}. Current count: {len(self.pending_match_requests[starting_url])}")
            else:
                # This case should ideally not happen if start_requests initialized correctly
                self.logger.warning(f"Starting URL {starting_url} not found in pending_match_requests when adding {match_url}. Initializing.")
                self.pending_match_requests[starting_url] = {match_url}


            self.logger.info(f"Queueing match link for details: {match_url}")
            yield scrapy.Request(
                url=match_url,
                callback=self.parse_match_details,
                meta={
                    'match_url': match_url,
                    'starting_url': starting_url, # Pass starting_url along
                    'attendance_summary': attendance_summary,
                    'playwright': True,
                    'playwright_include_page': True,
                    'playwright_page_methods': [], # Interactions handled in callback
                }
            )

        self.logger.info(f"Finished parsing schedule page {response.url}. Processed {found_count} valid match entries and queued detail requests.")
        # Note: We don't check for completion here, it happens in parse_match_details' finally block

    async def parse_match_details(self, response):
        """
        Parse the details page for a specific match, including Playwright interactions for telemetry.
        Handles removing the match_url from tracking and updating the DB if it's the last one for a starting_url.
        """
        match_url = response.meta.get('match_url', response.url)
        starting_url = response.meta.get('starting_url')
        page = response.meta.get("playwright_page") # Get the Playwright page object
        attendance_summary = response.meta.get('attendance_summary')

        if not starting_url:
            self.logger.error(f"Missing 'starting_url' in meta for match details page {match_url}. Cannot track completion.")
            # Optionally close page if it exists and wasn't closed by telemetry section
            if page and not page.is_closed(): await page.close()
            return # Cannot proceed with tracking logic

        self.logger.info(f"Parsing match details page: {match_url} (Originating from: {starting_url})")

        try:
            # SECTION 1: MATCH METADATA (Extracted from initial Scrapy response)
            self.logger.info(f"Scraping match metadata from: {response.url}")
            match_info_xpath = ".//div[contains(@class, 'mt-[1px]') and contains(@class, 'flex') and contains(@class, 'w-full') and contains(@class, 'flex-col') and contains(@class, 'justify-center') and contains(@class, 'bg-[#621968cc]') and contains(@class, 'px-7') and contains(@class, 'py-4') and contains(@class, 'text-center') and contains(@class, 'text-sm') and contains(@class, 'text-white') and contains(@class, 'first:rounded-t-lg') and contains(@class, 'last:rounded-b-lg') and contains(@class, 'theme-m2e:bg-darkblue10/80')]"
            match_info_element = response.xpath(match_info_xpath)

            officials_xpath = ".//div[contains(@class, 'mt-[1px]') and contains(@class, 'flex') and contains(@class, 'w-full') and contains(@class, 'justify-center') and contains(@class, 'bg-[#3b0f3fcc]') and contains(@class, 'px-7') and contains(@class, 'py-4') and contains(@class, 'text-center') and contains(@class, 'text-sm') and contains(@class, 'text-white') and contains(@class, 'theme-m2e:bg-darkblue6/80')]"
            officials_element = response.xpath(officials_xpath)
            referee, track_commissioner = None, None
            if officials_element:
                referee = "".join(officials_element.xpath(".//div[1]/p[2]/text()").getall()).replace("  ", " ").strip()
                track_commissioner = "".join(officials_element.xpath(".//div[2]/p[2]/text()").getall()).replace("  ", " ").strip()

            arena_xpath = ".//div[contains(@class, 'mt-[1px]') and contains(@class, 'flex') and contains(@class, 'w-full') and contains(@class, 'flex-col') and contains(@class, 'justify-center') and contains(@class, 'bg-[#3b0f3fcc]') and contains(@class, 'px-7') and contains(@class, 'py-4') and contains(@class, 'text-center') and contains(@class, 'text-sm') and contains(@class, 'text-white') and contains(@class, 'theme-m2e:bg-darkblue6/80') and not(./div)]"
            arena_element = response.xpath(arena_xpath)
            arena = "".join(arena_element.xpath(".//p/text()").getall()).replace("  -  ", "").strip() if arena_element else None

            competition, round_type, round_info, match_date = None, None, None, None
            if match_info_element:
                competition = (match_info_element.xpath(".//p[@class='pb-1 font-semibold uppercase']/text()").get() or "").strip()
                round_type = (match_info_element.xpath(".//p[2]/text()").get() or "").strip()
                round_text_nodes = match_info_element.xpath(".//p[3]/text()").getall()
                match_date = (match_info_element.xpath(".//p[4]/text()").get() or "").strip()
                for text in round_text_nodes:
                    cleaned_text = text.strip()
                    if cleaned_text:
                        round_match = re.search(r'(\d+)', cleaned_text)
                        if round_match:
                            round_number = round_match.group(1)
                            round_info = f"Round {round_number}"
                            break
            
            # SECTION 2: TEAMS AND SCORES
            team_name_elements = response.css('div.text-center.font-kallisto.text-sm::text').getall()
            if len(team_name_elements) > 2:
                home_team_details = team_name_elements[0].strip()
                away_team_details = team_name_elements[2].strip()
            elif len(team_name_elements) > 1:
                home_team_details = team_name_elements[0].strip()
                away_team_details = team_name_elements[1].strip()
            else:
                home_team_details, away_team_details = None, None

            score_elements = response.css('div.my-2\\.5.box-content.w-20.rounded-lg.bg-green1::text').getall()
            if len(score_elements) > 2:
                home_score_details = score_elements[0].strip()
                away_score_details = score_elements[2].strip()
            elif len(score_elements) > 1:
                home_score_details = score_elements[0].strip()
                away_score_details = score_elements[1].strip()
            else:
                home_score_details, away_score_details = None, None

            # SECTION 3: TEAM LINEUPS
            self.logger.info(f"Extracting team lineups from: {match_url}")
            team_lineups_container = response.css('div.flex.basis-3\\/4.flex-col.flex-wrap.gap-7.xl\\:flex-row')
            team1_data = {}
            team2_data = {}

            if team_lineups_container:
                team_elements = team_lineups_container.css('div.mb-5.w-full.max-w-full.xl\\:mb-0.xl\\:max-w-\\[calc\\(50\\%_\\-_14px\\)\\]')
                if len(team_elements) >= 2:
                    team1_el, team2_el = team_elements[0], team_elements[1]

                    for team_el, team_data_dict in [(team1_el, team1_data), (team2_el, team2_data)]:
                        team_data_dict['team_name'] = (team_el.css('div.truncate.max-w-\\[calc\\(100\\%_\\-_50px\\)\\]::text').get() or "").strip()
                        
                        team_info_staff = team_el.css('div.mb-4.mt-4.flex.flex-col.justify-end.text-sm.text-white')
                        spans = team_info_staff.xpath(".//span[@class='text-right']")
                        num_spans = len(spans)
                        
                        team_data_dict['manager'] = spans[0].xpath("string()").get().split(':')[-1].strip() if num_spans > 0 and ':' in spans[0].xpath("string()").get() else None
                        if num_spans == 3:
                            team_data_dict['coach'] = spans[1].xpath("string()").get().split(':')[-1].strip() if ':' in spans[1].xpath("string()").get() else None
                            team_data_dict['head_of_team'] = spans[2].xpath("string()").get().split(':')[-1].strip() if ':' in spans[2].xpath("string()").get() else None
                        elif num_spans == 2:
                            team_data_dict['coach'] = None
                            team_data_dict['head_of_team'] = spans[1].xpath("string()").get().split(':')[-1].strip() if ':' in spans[1].xpath("string()").get() else None
                        else: # num_spans <= 1 or unexpected
                            team_data_dict['coach'] = None
                            team_data_dict['head_of_team'] = None

                        riders_list = []
                        rows = team_el.css('table.w-full.text-white tbody tr')
                        for row in rows:
                            rider_data = {}
                            rider_data['number'] = (row.css('td.text-center::text').get() or "").replace('.', '').strip()
                            name_parts = row.css('td a span.inline-block::text').getall()
                            first_name = name_parts[0].strip() if name_parts else ""
                            last_name = (row.css('td a span.inline-block span.uppercase::text').get() or "").strip()
                            rider_data['name'] = f"{first_name} {last_name}".strip()
                            rider_data['scores'] = ["".join(row.css(f'td.text-center:nth-child({i}) *::text').getall()).strip() or "" for i in range(3, 9)]
                            rider_data['sum'] = (row.css('td.text-center.text-green1 strong::text').get() or "").strip()
                            rider_data['bonus'] = (row.css('td.text-center:last-child::text').get() or "").strip()
                            riders_list.append(rider_data)
                        team_data_dict['riders'] = riders_list
                else:
                    self.logger.warning(f"Could not find two team elements for lineups on {match_url}")
                    team1_data, team2_data = None, None
            else:
                self.logger.warning(f"No team lineups container found on {match_url}")
                team1_data, team2_data = None, None

            # SECTION 4: HEAT-BY-HEAT RESULTS
            self.logger.info(f"Extracting heat-by-heat results from: {match_url}")
            all_heats_data = []
            race_blocks = response.css('div.mx-auto.mb-5.max-w-\\[520px\\]')

            if not race_blocks:
                self.logger.warning(f"No race blocks found on {match_url}. Heat details might be incomplete.")
            else:
                self.logger.info(f"Found {len(race_blocks)} race blocks on {match_url}")
                for block in race_blocks:
                    heat_data = {}
                    heat_number_xpath = './/div[contains(@class, "mb-2.5")]//text()'
                    heat_text_nodes = block.xpath(heat_number_xpath).getall()
                    heat_number = next((text.strip() for text in reversed(heat_text_nodes) if text.strip().isdigit()), None)

                    if not heat_number:
                        self.logger.warning(f"Could not extract heat number using XPath: {heat_number_xpath} in block. Skipping heat.")
                        continue
                    heat_data['heat_number'] = heat_number

                    team_scores_xpath = './/td[contains(@class, "box-content") and contains(@class, "w-6") and contains(@class, "text-center") and contains(@class, "font-kallisto") and contains(@class, "lg:w-12") and @rowspan="2"]'
                    score_elements = block.xpath(team_scores_xpath)
                    if len(score_elements) == 4:
                        heat_data['hometeam_heat_score'] = (score_elements[0].xpath('./text()').get() or "").strip()
                        heat_data['awayteam_heat_score'] = (score_elements[1].xpath('./text()').get() or "").strip()
                        heat_data['hometeam_current_match_score'] = (score_elements[2].xpath('./text()').get() or "").strip()
                        heat_data['awayteam_current_match_score'] = (score_elements[3].xpath('./text()').get() or "").strip()
                    else:
                        self.logger.warning(f"Could not extract all team scores for heat {heat_number}")
                        heat_data.update({k: None for k in ['hometeam_heat_score', 'awayteam_heat_score', 'hometeam_current_match_score', 'awayteam_current_match_score']})

                    riders_in_heat = []
                    rows = block.xpath('.//tbody/tr')
                    for row_idx, row in enumerate(rows):
                        try:
                            starting_field = (row.xpath('./td[1]/text()').get() or "").strip()
                            rider_div = row.xpath('./td[3]')
                            rider = (rider_div.xpath('normalize-space(./div[not(contains(@class, "line-through"))])').get() or "").strip()
                            substituted_rider = (rider_div.xpath('normalize-space(./div[contains(@class, "line-through")])').get() or "").strip() or None
                            
                            helmet_color_class = row.xpath('./td[1]/@class').get()
                            helmet_color = None
                            if helmet_color_class and "!bg-" in helmet_color_class:
                                color_part = helmet_color_class.split("!bg-")[-1].lower()
                                if "red" in color_part: helmet_color = "red"
                                elif "white" in color_part: helmet_color = "white"
                                elif "blue" in color_part: helmet_color = "blue"
                                elif "yellow" in color_part: helmet_color = "yellow"
                            
                            rider_score = (row.xpath('./td[4]/text()').get() or "").strip()
                            warning = (row.xpath('./td[contains(@class, "box-content") and contains(@class, "w-3") and contains(@class, "text-center")]//span/text()').get() or "").strip() or None
                            
                            riders_in_heat.append({
                                'starting_field': starting_field, 'helmet_color': helmet_color,
                                'substituted_rider': substituted_rider, 'rider': rider,
                                'rider_score': rider_score, 'warning': warning,
                            })
                        except Exception as e:
                            self.logger.error(f"Error extracting rider data in heat {heat_number}, row {row_idx}: {e}")
                    heat_data['riders'] = riders_in_heat
                    all_heats_data.append(heat_data)

            # SECTION 5: TELEMETRY DATA (requires browser automation)
            main_telemetry_data = "Telemetry data not processed (Playwright page not available or error)."
            if not page:
                self.logger.error("Playwright page object not found in meta. Cannot scrape telemetry data.")
            else:
                self.logger.info(f"Starting Playwright interaction for telemetry data on: {match_url}")
                try:
                    # *** OPTIMIZATION: Resource Blocking - Test thoroughly! ***
                    # Block images, stylesheets, and fonts to potentially speed up Playwright interactions.
                    # If telemetry interactions break, remove or refine this blocking.
                    await page.route(
                        "**/*",
                        lambda route: route.abort()
                        if route.request.resource_type in ["image", "stylesheet", "font", "media"] # Keep "script", "xhr", "fetch" etc.
                        else route.continue_()
                    )
                    self.logger.info("Applied Playwright resource blocking (images, stylesheets, fonts, media).")

                    # *** EFFICIENCY CHANGE: Removed redundant page.goto() ***
                    # The page is already at response.url due to scrapy-playwright.
                    # self.logger.info(f"Navigating page to {response.url} before telemetry interaction...")
                    # await page.goto(response.url, wait_until="domcontentloaded", timeout=60000) # REMOVED
                    # self.logger.info("Page was already navigated by scrapy-playwright.")

                    stable_element_selector = 'div.flex.basis-3\\/4.flex-col.flex-wrap.gap-7.xl\\:flex-row' # e.g., team lineups
                    self.logger.info(f"Waiting for stable element '{stable_element_selector}' to ensure page is ready for telemetry.")
                    await page.wait_for_selector(stable_element_selector, state='visible', timeout=30000)
                    self.logger.info("Stable element found.")

                    telemetry_button_selector = 'a:has(span.short-name:has-text("Telemetry"))'
                    self.logger.info(f"Waiting for telemetry tab button to be visible: {telemetry_button_selector}")
                    telemetry_button = await page.wait_for_selector(telemetry_button_selector, state='visible', timeout=30000) # Long timeout for initial visibility
                    
                    self.logger.info(f"Attempting to click telemetry tab button: {telemetry_button_selector}")
                    await telemetry_button.click(timeout=10000) # Shorter timeout for the click action itself
                    self.logger.info("Telemetry tab clicked.")

                    main_telemetry_table_selector = 'table.w-full.table-auto.overflow-x-auto'
                    # *** EFFICIENCY CHANGE: Wait for table visibility after click, instead of fixed delay ***
                    self.logger.info(f"Waiting for main telemetry table to load: {main_telemetry_table_selector}")
                    await page.wait_for_selector(main_telemetry_table_selector, state='visible', timeout=90000)
                    self.logger.info("Main telemetry table loaded successfully.")

                    rider_row_handles = await page.query_selector_all(f"{main_telemetry_table_selector} tbody tr")
                    self.logger.info(f"Found {len(rider_row_handles)} potential rider rows in telemetry table.")
                    
                    processed_telemetry_data = []

                    for i, row_handle in enumerate(rider_row_handles):
                        if await row_handle.query_selector('td[colspan="6"]'): # Skip expanded sub-table rows
                            self.logger.debug(f"Skipping row {i} as it appears to be an expanded sub-table row.")
                            continue

                        rider_name, rider_number = f'Unknown Rider Row {i}', 'N/A'
                        try:
                            rider_number_el = await row_handle.query_selector('td:nth-child(2)')
                            team_code_el = await row_handle.query_selector('td:nth-child(3)')
                            rider_name_el = await row_handle.query_selector('td:nth-child(4)')
                            best_time_el = await row_handle.query_selector('td:nth-child(5)')
                            vmax_summary_el = await row_handle.query_selector('td:nth-child(6)')

                            rider_number = (await rider_number_el.inner_text()).strip() if rider_number_el else 'N/A'
                            team_code = (await team_code_el.inner_text()).strip() if team_code_el else 'N/A'
                            rider_name = (await rider_name_el.inner_text()).strip() if rider_name_el else f'Unnamed Rider Row {i}'
                            best_time = (await best_time_el.inner_text()).strip() if best_time_el else 'N/A'
                            vmax_summary = (await vmax_summary_el.inner_text()).strip() if vmax_summary_el else 'N/A'
                        except Exception as e_basic:
                            self.logger.warning(f"Could not extract basic telemetry info for rider in row {i}: {e_basic}. Using placeholders.")
                            # Basic data entry will be created anyway with placeholders
                        
                        rider_name_for_log = rider_name if rider_name != f'Unnamed Rider Row {i}' else f"Rider at index {i}"
                        basic_data_entry = {
                            'rider_number': rider_number, 'team_code': team_code, 'rider_name': rider_name,
                            'best_time': best_time, 'vmax_summary': vmax_summary, 'detailed_telemetry': []
                        }

                        button_handle = await row_handle.query_selector('td:first-child button')
                        if not button_handle:
                            self.logger.warning(f"No expand button for rider {rider_name_for_log}. No detailed telemetry.")
                            processed_telemetry_data.append(basic_data_entry)
                            continue

                        sub_table_row_handle = None # Initialize for finally block
                        try:
                            self.logger.info(f"Expanding details for rider {rider_name_for_log} (#{rider_number})")
                            await button_handle.click(timeout=10000)

                            sub_table_row_selector = "xpath=./following-sibling::tr[1][.//table]" # Relative to row_handle
                            sub_table_row_handle = await row_handle.wait_for_selector(sub_table_row_selector, state="visible", timeout=10000)
                            
                            if sub_table_row_handle:
                                sub_table_handle = await sub_table_row_handle.query_selector('table')
                                if sub_table_handle:
                                    sub_table_html = await sub_table_handle.inner_html()
                                    sub_table_response = HtmlResponse(page.url, body=sub_table_html, encoding='utf-8')
                                    sub_table_rows_data = sub_table_response.css('tbody tr')
                                    for sub_row in sub_table_rows_data:
                                        detailed_data = {
                                            'heat_number': (sub_row.css('td:nth-child(1)::text').get() or '').strip(),
                                            'lap_time': (sub_row.css('td:nth-child(2)::text').get() or '').strip(),
                                            'distance': (sub_row.css('td:nth-child(3)::text').get() or '').strip(),
                                            'vmax_lap': (sub_row.css('td:nth-child(4)::text').get() or '').strip(),
                                            'lap1_time': (sub_row.css('td:nth-child(5)::text').get() or '').strip(),
                                            'lap2_time': (sub_row.css('td:nth-child(6)::text').get() or '').strip(),
                                            'lap3_time': (sub_row.css('td:nth-child(7)::text').get() or '').strip(),
                                            'lap4_time': (sub_row.css('td:nth-child(8)::text').get() or '').strip(),
                                        }
                                        basic_data_entry['detailed_telemetry'].append(detailed_data)
                                    self.logger.debug(f"Extracted {len(basic_data_entry['detailed_telemetry'])} detailed entries for {rider_name_for_log}")
                                else: # No inner table in sub_table_row_handle
                                    self.logger.warning(f"Could not find inner table for rider {rider_name_for_log}.")
                            else: # sub_table_row_handle not found
                                 self.logger.warning(f"Sub-table row not found for rider {rider_name_for_log} after clicking expand.")
                        
                        except Exception as e_detail:
                            self.logger.error(f"Error processing detailed telemetry for {rider_name_for_log}: {e_detail}")
                        finally:
                            # Attempt to close the sub-table if it was opened or if an error occurred
                            if button_handle and await button_handle.is_visible():
                                try:
                                    # Check if the button's state suggests it's expanded (might need specific attribute check)
                                    # For now, just click if visible, assuming it might be expanded
                                    self.logger.debug(f"Attempting to close/reset expand button for {rider_name_for_log}")
                                    await button_handle.click(timeout=5000) 
                                    if sub_table_row_handle and await sub_table_row_handle.is_visible():
                                        try:
                                            # *** EFFICIENCY CHANGE: Wait for sub-table to hide ***
                                            await sub_table_row_handle.wait_for_element_state("hidden", timeout=5000)
                                            self.logger.info(f"Closed and confirmed hidden sub-table for {rider_name_for_log}")
                                        except Exception as e_hide_wait:
                                            self.logger.warning(f"Sub-table for {rider_name_for_log} did not confirm hidden quickly: {e_hide_wait}. Using small delay.")
                                            await page.wait_for_timeout(150) # Small fallback delay
                                    else:
                                        # If sub_table_row_handle wasn't found or already hidden, just log that we clicked close
                                        self.logger.info(f"Clicked to close/reset details for {rider_name_for_log} (sub-table might not have been open or already hidden).")
                                except Exception as e_close_click:
                                    self.logger.warning(f"Error clicking to close button for {rider_name_for_log}: {e_close_click}")
                            processed_telemetry_data.append(basic_data_entry)
                    
                    main_telemetry_data = processed_telemetry_data

                except Exception as e_telemetry_main:
                    self.logger.error(f"Major error during telemetry tab processing for {match_url}: {e_telemetry_main}")
                    main_telemetry_data = f"Telemetry tab/data not accessible: {e_telemetry_main}"
                finally:
                    # Scrapy-Playwright handles page and context closure based on settings.
                    # However, if you explicitly took control (which we did by getting `page`),
                    # it's good practice to ensure it's closed if no longer needed by this request,
                    # though scrapy-playwright should still manage it.
                    if page and not page.is_closed():
                        self.logger.info("Closing Playwright page for this request as telemetry processing is done.")
                        await page.close()
                        # self.logger.info("Playwright page closed by spider.")

            # SECTION 6: COMPILE AND RETURN FINAL RESULT
            match_data = {
                'source': 'match_details_aggregated_with_telemetry', 'match_url': match_url,
                'competition': competition, 'round_type': round_type, 'round': round_info, 'match_date': match_date,
                'attendance_summary': attendance_summary, 'referee': referee, 'track_commissioner': track_commissioner, 'arena': arena,
                'home_team_details': home_team_details, 'home_score_details': home_score_details,
                'away_team_details': away_team_details, 'away_score_details': away_score_details, # Corrected away_score_details
                'team1': team1_data if team1_data else "Not available",
                'team2': team2_data if team2_data else "Not available",
                'match_details': all_heats_data,
                'telemetry_data': main_telemetry_data
            }

            self.logger.info(f"Completed scraping match: {match_url}")

            # Generate filename for match data
            home_team = match_data.get('home_team_details', 'unknown_home_team').replace(" ", "_").replace("/", "_")
            away_team = match_data.get('away_team_details', 'unknown_away_team').replace(" ", "_").replace("/", "_")
            date_time = match_data.get('match_date', 'unknown_date').replace(" ", "T").replace(":", "-").replace("/", "-")
            comptetition_name = match_data.get('competition', 'unknown_competition').replace(" ", "").replace("/", "")
            round_type_name = match_data.get('round_type', 'unknown_round_type').replace(" ", "").replace("/", "")

            filename = f"output_{comptetition_name}_{round_type_name}_{home_team}-{away_team}_{date_time}.json"
            output_path = os.path.join('output', filename) # Save in an 'output' directory

            # Ensure the output directory exists
            os.makedirs('output', exist_ok=True)

            # Save the match data to a JSON file
            try:
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(match_data, f, ensure_ascii=False, indent=4)
                self.logger.info(f"Saved match data to {output_path}")
            except Exception as e:
                self.logger.error(f"Failed to save match data to {output_path}: {e}")

        finally:
            # This block executes regardless of success or failure in the try block above
            if starting_url and match_url: # Ensure we have the keys needed
                if starting_url in self.pending_match_requests:
                    try:
                        self.pending_match_requests[starting_url].remove(match_url)
                        self.logger.debug(f"Removed completed/failed {match_url} from pending for {starting_url}. Remaining: {len(self.pending_match_requests[starting_url])}")

                        # Check if this was the last pending request for the starting_url
                        if not self.pending_match_requests[starting_url]:
                            self.logger.info(f"All match requests completed for starting URL: {starting_url}")
                            is_current = self.start_url_meta.get(starting_url, None) # Get is_current_season flag

                            if is_current is False: # Explicitly check for False, not None
                                if self.cursor and self.conn:
                                    try:
                                        self.logger.info(f"Updating database: Setting is_scraped=true for non-current season URL: {starting_url}")
                                        self.cursor.execute("UPDATE public.data_source SET is_scraped = true WHERE url = %s", (starting_url,))
                                        self.conn.commit()
                                        self.logger.info(f"Successfully marked {starting_url} as scraped in the database.")
                                    except psycopg2.Error as e:
                                        self.logger.error(f"Database error updating is_scraped for {starting_url}: {e}")
                                        # Optionally rollback if needed, though commit handles one statement
                                        # self.conn.rollback()
                                else:
                                    self.logger.error(f"Cannot update is_scraped for {starting_url}: Database connection not available.")
                            elif is_current is True:
                                self.logger.info(f"Skipping database update for {starting_url} because it is marked as current season.")
                            else: # is_current is None (shouldn't happen if start_requests worked)
                                self.logger.warning(f"Could not determine if {starting_url} is current season. Cannot update database status.")

                            # Optional: Clean up memory for completed starting_url
                            # del self.pending_match_requests[starting_url]
                            # del self.start_url_meta[starting_url]

                    except KeyError:
                        self.logger.warning(f"Match URL {match_url} was not found in the pending set for {starting_url} during finally block. Might have been processed twice or tracking error.")
                else:
                    self.logger.warning(f"Starting URL {starting_url} not found in pending requests tracking during finally block for {match_url}.")
            else:
                 self.logger.warning(f"Could not perform completion check: missing starting_url or match_url in finally block for response {response.url}")

            # Ensure Playwright page is closed if it wasn't handled in the telemetry section
            # (e.g., if telemetry failed or wasn't needed)
            if page and not page.is_closed():
                self.logger.debug(f"Closing Playwright page in finally block for {match_url}")
                await page.close()
