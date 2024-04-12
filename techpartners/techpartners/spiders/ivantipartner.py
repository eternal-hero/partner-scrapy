# import needed libraries
import datetime
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
    name = 'ivantipartner'
    partner_program_link = 'https://www.ivanti.com/partners/find'
    partner_directory = 'Ivanti Partners Directory'
    partner_program_name = ''
    crawl_id = None

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
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/json;charset=UTF-8',
        'Authority': 'www.ptc.com',
        'Origin': 'https://www.ptc.com',
        'Referer': 'https://www.ptc.com/en/partners/partner-search',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'Connection': 'keep-alive',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }
    api_link = 'https://www.ivanti.com/data/partners/ivanti-reseller?cc=en-US'

    def parse(self, response):
        countries_dict = dict()
        type_dict = dict()
        category_dict = dict()
        region_dict = dict()

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            countries = [country.find('label') for country in soup.find_all('li', {'class': 'country-list-item'})]
            for country in countries:
                countries_dict[int(country['for'])] = country.text

            divs = soup.find_all('select', {'class': 'jplist-select select'})
            for div in divs:
                if 'Partner Type' in div.text:
                    partner_types = div.find_all('option', {'value': True})
                    for partner_type in partner_types:
                        if partner_type['value'] == '':
                            continue
                        else:
                            type_dict[int(partner_type['value'])] = partner_type.text

                elif 'Product Category' in div.text:
                    category_types = div.find_all('option', {'value': True})
                    for category_type in category_types:
                        if category_type['value'] == '':
                            continue
                        else:
                            category_dict[int(category_type['value'])] = category_type.text

                elif 'Region' in div.text:
                    region_types = div.find_all('option', {'value': True})
                    for region_type in region_types:
                        if region_type['value'] == '':
                            continue
                        else:
                            region_dict[int(region_type['value'])] = region_type.text

        r = requests.get(self.api_link)
        if r.status_code == 200:
            try:
                partners = [json.loads(line) for line in r.text.splitlines()]
            except:
                partners = list()
            print(f'Partners: {len(partners)}')
            for partner in partners:
                if 'ID' in partner:

                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = ''

                    item['partner_company_name'] = partner['Name']
                    item['company_domain_name'] = partner['WebsiteUrl'] if 'WebsiteUrl' in partner and partner['WebsiteUrl'] else ''
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

                    item['partner_type'] = list()
                    item['categories'] = list()

                    categoryIDs = partner['CategoryIDs'] if 'CategoryIDs' in partner and partner['CategoryIDs'] else list()
                    for categoryID in categoryIDs:
                        if categoryID in type_dict.keys():
                            item['partner_type'].append(type_dict[categoryID])
                        elif categoryID in category_dict.keys():
                            item['categories'].append(category_dict[categoryID])

                    item['regions'] = list()
                    item['locations_country'] = list()
                    localeIDs = partner['LocaleIDs'] if 'LocaleIDs' in partner and partner['LocaleIDs'] else list()
                    for localeID in localeIDs:
                        if localeID in region_dict.keys():
                            item['regions'].append(region_dict[localeID])
                        elif localeID in countries_dict.keys():
                            item['locations_country'].append(countries_dict[localeID])

                    yield item
