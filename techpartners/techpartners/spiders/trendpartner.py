# import needed libraries
import json
import math
import re

import requests
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'trendpartner'
    partner_program_link = 'https://www.trendmicro.com/en_us/partners/find-a-partner.html'
    partner_directory = 'TrendMicro Partner Directory'
    partner_program_name = ''
    crawl_id = None

    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 8,
    }

    start_urls = [partner_program_link]
    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name', 'product_service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'product_service_description',
                   'headquarters_address',
                   'headquarters_street', 'headquarters_city', 'headquarters_state', 'headquarters_zipcode', 'headquarters_country',
                   'locations_address',
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
                   'primary_contact_name', 'primary_contact_phone_number', 'primary_contact_email',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'Connection': 'keep-alive',
        'Content-Type': 'text/plain',
        'Authority': 'www.trendmicro.com',
        'Referer': 'https://www.trendmicro.com/en_us/partners/find-a-partner.html',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'x-requested-with': 'XMLHttpRequest',
        }

    def start_requests(self):
        countries = list()
        countries_url = 'https://www.trendmicro.com/feeds/reseller/feed/countries'
        r = requests.get(countries_url)
        if r.status_code != 200:
            self.logger.info(f'ERROR COUNTRIES REQUEST STATUS: {r.status_code}')
        else:
            try:
                countries = json.loads(r.text)['results']
            except Exception as e:
                self.logger.info(f'ERROR COUNTRIES JSON: {e}')

        for country in countries:
            offset = 0
            country_code = country['value']
            country_name = country['label']
            link = f'https://www.trendmicro.com/feeds/reseller/feed/resellers?country_id={country_code}&specialization=All&sort_by=tier%2Cdescending&offset={offset}'
            yield scrapy.Request(method='GET', url=link, callback=self.parse,
                                 headers=self.headers,
                                 meta={'offset': offset,
                                       'country_code': country_code,
                                       'country_name': country_name,
                                       },
                                 dont_filter=True
                                 )

    def parse(self, response):
        offset = response.meta['offset']
        country_code = response.meta['country_code']
        country_name = response.meta['country_name']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            try:
                partners = json.loads(response.text)['results']
            except:
                partners = list()

            self.logger.info(f"Country: {country_name}, Page Number = {int(offset/10)}, Number of results = {len(partners)}")

            for partner in partners:
                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item['partner_company_name'] = partner['company_name']
                item['partner_type'] = partner['partner_types'] if 'partner_types' in partner and partner['partner_types'] else ''
                item['partner_tier'] = partner['tier'] if 'tier' in partner and partner['tier'] else ''

                item['locations_country'] = partner['country'] if 'country' in partner and partner['country'] else ''
                item['locations_state'] = partner['state'] if 'state' in partner and partner['state'] else ''
                item['locations_city'] = partner['city'] if 'city' in partner and partner['city'] else ''

                item['specializations'] = partner['specializations'] if 'specializations' in partner and partner['specializations'] else ''

                item['company_domain_name'] = partner['url'] if 'url' in partner and partner['url'] else ''
                try:
                    url_obj = urllib.parse.urlparse(item['company_domain_name'])
                    item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                    x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                    if x:
                        item['company_domain_name'] = x[-1]
                    if '/' in item['company_domain_name']:
                        item['company_domain_name'] = item['company_domain_name'][:item['company_domain_name'].find('/')]
                except Exception as e:
                    print('DOMAIN ERROR: ', e)

                yield item

            # follow next pages
            if len(partners) == 10:
                offset += 10
                link = f'https://www.trendmicro.com/feeds/reseller/feed/resellers?country_id={country_code}&specialization=All&sort_by=tier%2Cdescending&offset={offset}'
                yield scrapy.Request(method='GET', url=link, callback=self.parse,
                                     headers=self.headers,
                                     meta={'offset': offset,
                                           'country_code': country_code,
                                           'country_name': country_name,
                                           },
                                     dont_filter=True
                                     )
