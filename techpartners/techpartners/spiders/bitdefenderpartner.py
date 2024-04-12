# import needed libraries
import json
import re
import math

import requests

from techpartners.spiders.base_spider import BaseSpider
import scrapy
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse
from scrapy.http import HtmlResponse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'bitdefenderpartner'
    partner_program_link = 'https://www.bitdefender.com/partners/partner-locator.html'
    partner_directory = 'Bitdefender Partner Directory'
    partner_program_name = ''
    crawl_id = None

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
                   'primary_contact_name', 'primary_contact_phone_number',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
               'Authority': 'www.bitdefender.com',
               'Accept': '*/*',
               'Accept-Language': 'en-US,en;q=0.9',
               'Accept-Encoding': 'gzip, deflate',
               'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
               'Origin': 'https://www.bitdefender.com',
               'Referer': 'https://www.bitdefender.com/partners/partner-locator.html',
               'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
               'sec-ch-ua-mobile': '?0',
               'sec-ch-ua-platform': 'Windows',
               'Sec-Fetch-Dest': 'empty',
               'Sec-Fetch-Mode': 'cors',
               'Sec-Fetch-Site': 'same-origin',
               'X-Requested-With': 'XMLHttpRequest',
               }

    all_countries = None

    def start_requests(self):
        r = requests.get('https://www.bitdefender.com/etc.clientlibs/bitdefender/clientlibs/clientlib-site.lc-8c0f52d55b14c337feca6a54c2e21739-lc.min.js',     headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36'})
        if r.status_code == 200:
            if 'var all_countries' in r.text:
                content = r.text[r.text.find('var all_countries'):]
                content = content[1+content.find('='): 1+content.find('}')]
                self.all_countries = json.loads(content.strip())
        else:
            self.logger.info('ERROR: GET COUNTRIES DATA')

        data = 'search=&partner_type=&country_select=All&state_select=All&city_select=All&per_page=25&page=1'
        yield scrapy.Request(method='POST', url='https://www.bitdefender.com/site/Partnership/partnerLocatorAjax2019',
                             body=data, callback=self.parse, headers=self.headers)

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
            return

        data = json.loads(response.text)
        if 'success' in data and data['success'] and 'data' in data:
            curr_page = data['pagination']['page']
            last_page = data['pagination']['total_pages']

            for profile in data['data']:
                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item['partner_company_name'] = profile['p_company']
                item['partner_type'] = profile['program_type'] if 'program_type' in profile else ''
                item['partner_tier'] = profile['p_category'] if 'p_category' in profile else ''

                item['company_domain_name'] = profile['p_website'] if 'p_website' in profile and profile['p_website'] else ''
                try:
                    url_obj = urllib.parse.urlparse(item['company_domain_name'])
                    item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                    x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                    if x:
                        item['company_domain_name'] = x[-1]
                    if '/' in item['company_domain_name']:
                        item['company_domain_name'] = item['company_domain_name'][
                                                      :item['company_domain_name'].find('/')]
                except Exception as e:
                    print('DOMAIN ERROR: ', e)

                item['general_phone_number'] = profile['p_phone'] if 'p_phone' in profile else ''
                item['general_email_address'] = profile['p_email'] if 'p_email' in profile else ''

                item['certified'] = 'Yes' if 'p_cert_bus' in profile and profile['p_cert_bus'] == '1' else ''

                item['headquarters_street'] = profile['p_address'] if 'p_address' in profile else ''
                item['headquarters_city'] = profile['p_city'] if 'p_city' in profile else ''
                item['headquarters_state'] = profile['p_state'] if 'p_state' in profile else ''
                item['headquarters_zipcode'] = profile['p_zipcode'] if 'p_zipcode' in profile else ''
                item['headquarters_country'] = profile['p_country'] if 'p_country' in profile else ''

                if item['headquarters_country'] != '' and item['headquarters_country'] in self.all_countries:
                    item['headquarters_country'] = self.all_countries[item['headquarters_country']]

                yield item

            # follow next pages
            if curr_page == 1 and last_page > 1:
                for i in range(2, last_page+1):
                    data = f'search=&partner_type=&country_select=All&state_select=All&city_select=All&per_page=25&page={i}'
                    yield scrapy.Request(method='POST', url='https://www.bitdefender.com/site/Partnership/partnerLocatorAjax2019',
                                         body=data, callback=self.parse, headers=self.headers)
