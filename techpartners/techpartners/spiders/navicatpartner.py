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
    name = 'navicatpartner'
    partner_program_link = 'https://www.navicat.com/en/company/partner-directory'
    partner_directory = 'Navicat Partner'
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
                   'general_phone_number', 'general_fax_number', 'general_email_address',
                   'support_phone_number', 'support_email_address', 'support_link', 'help_link', 'terms_and_conditions',
                   'license_agreement_link', 'privacy_policy_link',
                   'linkedin_link', 'twitter_link', 'facebook_link', 'youtube_link', 'instagram_link', 'xing_link',
                   'primary_contact_name', 'primary_contact_phone_number', 'primary_contact_email',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    api_link = 'https://www.navicat.com/includes/Navicat/locate_partner.php'

    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Cookie': 'a2331c12918fc8bc532b59f20879274e=o8ftr8lbk2job2988ihnpndooa; _ga=GA1.2.768594162.1675513789; _gid=GA1.2.756297218.1675513789; _gat=1',
        'Host': 'www.navicat.com',
        'Origin': 'https://www.navicat.com',
        'Referer': 'https://www.navicat.com/en/company/partner-directory',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }

    start_urls = [partner_program_link]

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            partner_types = [optn.text.strip()  for optn in soup.find('select', {'name': 'partner-type'}).find_all('option')]

            for partner_type in partner_types:
                payload = f'partner-type={urllib.parse.quote_plus(partner_type)}&country=All&language=en'
                yield scrapy.Request(method='POST', url=self.api_link, body=payload, headers=self.headers,
                                     callback=self.parse_type, meta={'partner_type': partner_type})

    def parse_type(self, response):
        partner_type = response.meta['partner_type']
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            if soup.find('div', {'class': 'locate-reseller-table'}):
                partners = soup.find('div', {'class': 'locate-reseller-table'}).find_all('div', recursive=False)
                self.logger.info(f'{partner_type} has {len(partners)} partners.')
                for partner in partners:

                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = ''

                    item['partner_type'] = partner_type

                    data_lines = partner.find('div', {'class': 'locate-reseller-table-content'}).find_all('div', recursive=False)
                    for line in data_lines:
                        if 'Company Name' in line.find('div', {'class': 'locate-reseller-table-label'}).text:
                            item['partner_company_name'] = line.find('div', {'class': 'locate-reseller-table-details'}).text.strip()

                        elif 'Country' in line.find('div', {'class': 'locate-reseller-table-label'}).text:
                            item['headquarters_country'] = line.find('div', {'class': 'locate-reseller-table-details'}).text.strip()

                        elif 'Email' in line.find('div', {'class': 'locate-reseller-table-label'}).text:
                            item['general_email_address'] = line.find('div', {'class': 'locate-reseller-table-details'}).text.strip()

                        elif 'URL' in line.find('div', {'class': 'locate-reseller-table-label'}).text:
                            item['company_domain_name'] = get_domain_from_url(line.find('div', {'class': 'locate-reseller-table-details'}).text.strip())

                        elif 'Phone:' in line.find('div', {'class': 'locate-reseller-table-details'}).text:
                            item['general_phone_number'] = line.find('div', {'class': 'locate-reseller-table-details'}).text.replace('Phone:', '').strip()

                        elif 'Fax:' in line.find('div', {'class': 'locate-reseller-table-details'}).text:
                            item['general_fax_number'] = line.find('div', {'class': 'locate-reseller-table-details'}).text.replace('Fax:', '').strip()

                    yield item
