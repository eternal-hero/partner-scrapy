# import needed libraries
import re
import requests
from techpartners.spiders.base_spider import BaseSpider
import scrapy
import urllib.parse
from techpartners.functions import *


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'zendeskpartner'
    partner_program_link = 'https://www.zendesk.com/marketplace/partners/'
    partner_directory = 'ZenDesk Partner'
    partner_program_name = ''
    crawl_id = 1260

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name', 'product/service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'product/service_description',
                   'headquarters_street', 'headquarters_city', 'headquarters_state', 'headquarters_zipcode', 'headquarters_country',
                   'locations_street', 'locations_city', 'locations_state', 'locations_zipcode', 'locations_country',
                   'regions', 'languages', 'products', 'services', 'solutions',
                   'pricing_plan', 'pricing_model', 'pricing_plan_description',
                   'pricing', 'specializations', 'categories',
                   'features', 'account_requirements', 'product_package_name', 'year_founded', 'latest_update', 'publisher',
                   'partnership_timespan', 'partnership_founding_date', 'product_version', 'product_requirements',
                   'general_phone_number', 'general_email_address',
                   'support_phone_number', 'support_email_address', 'support_link', 'help_link', 'terms_and_conditions',
                   'license_agreement_link', 'privacy_policy_link',
                   'linkedin_link', 'twitter_link', 'facebook_link', 'youtube_link', 'instagram_link', 'xing_link',
                   'primary_contact_name', 'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified?',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0"}

    def start_requests(self):
        """
        landing function of spider class
        will get api results, create the search pages and pass it to parse function
        :return:
        """

        self.sess = requests.Session()
        self.sess.get('https://www.zendesk.com/_next/static/chunks/619-a2b73af578777282bd28.js', headers=self.headers)

        result_link = 'https://7z3rm3e33j-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.9.1)%3B%20Browser%20(lite)&x-algolia-api-key=4ad1644763bb9a01fd5d8918a11c2d77&x-algolia-application-id=7Z3RM3E33J'

        pageNumber = 0
        lastPage = 1

        while pageNumber < lastPage:
            params = f"highlightPreTag=%3Cais-highlight-0000000000%3E&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&clickAnalytics=true&query=&analyticsTags=%5B%22search_type%3Adirectory%22%5D&maxValuesPerFacet=30&page={pageNumber}&facets=%5B%22regions%22%2C%22services%22%2C%22partner_types%22%2C%22searchable_rating%22%5D&tagFilters=&analytics=false"
            payload = {"requests": [{"indexName": "organizationsIndex",
                                     "params": params}]
                       }

            self.logger.info("Page Number: " + str(pageNumber))

            response = self.sess.post(result_link, json=payload, headers=self.headers)

            try:
                if response.status_code == 200:
                    data = response.json()
                    result = data['results'][0]
                    lastPage = result['nbPages']
                    profiles = result['hits']
                    yield scrapy.Request('https://www.example.com', callback=self.parse, meta={'profiles': profiles}, dont_filter=True)

                    if 0 < len(profiles) <= 20:
                        pageNumber += 1

                else:
                    print(response.status_code)
                    print(response.text)
                    raise Exception
            except:
                continue

    def parse(self, response):
        profiles = response.meta['profiles']
        for profile in profiles:

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = self.partner_program_name

            item['partner_company_name'] = profile['name']
            item['partner_type'] = profile['partner_types']
            item['partner_tier'] = profile['partner_levels']
            item['headquarters_city'] = profile['city']
            item['headquarters_country'] = profile['country']
            item['regions'] = profile['regions']
            # item['company_domain_name'] = profile['website_url']
            url_obj = urllib.parse.urlparse(profile['website_url'] if 'website_url' in profile else '')
            item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
            x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
            if x:
                item['company_domain_name'] = x[-1]

            item['general_email_address'] = profile['email']
            # 'Short Description': profile['short_description'],
            item['company_description'] = cleanhtml(profile['long_description'])
            item['services'] = profile['services_offered']

            yield item
