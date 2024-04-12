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
    name = 'cncfpartner'
    partner_program_link = 'https://www.cncf.io/about/members/'
    partner_directory = 'cncf Partners Directory'
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
                   'primary_contact_name', 'primary_contact_phone_number', 'primary_contact_email',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes', 'market_cap']
    start_urls = ['https://landscape.cncf.io/pages/members']
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        'Authority': 'landscape.cncf.io',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9',
        'cookie': '_gid=GA1.2.405412695.1667246181; _fbp=fb.1.1667246183700.2069117441; _ga=GA1.2.406080258.1667246181; _ga_T6VMPWFRDW=GS1.1.1667246182.1.1.1667247257.0.0.0',
        'Referer': 'https://www.cncf.io/',
        'Sec-Ch-Ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'Sec-Ch-Ua-mobile': '?0',
        'Sec-Ch-Ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'iframe',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-site',
        'upgrade-insecure-requests': 1,
    }

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            partners = soup.find_all('div', {'data-id': True})
            self.logger.info(f'partners = {len(partners)}')

            for partner in partners:
                partner_link = f'https://landscape.cncf.io/data/items/info-{partner["data-id"]}.html'
                yield scrapy.Request(method='GET', url=partner_link, callback=self.parse_partner,
                                     dont_filter=True, headers=self.headers)

    def parse_partner(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            partner = BS(response.text, "html.parser")

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = ''

            item['partner_company_name'] = partner.find('div', {'class': 'product-name'}).text if partner.find('div', {'class': 'product-name'}) else ''
            if '(member)' in item['partner_company_name']:
                item['partner_company_name'] = cleanhtml(item['partner_company_name'].replace('(member)', ''))

            type_info = partner.find('div', {'class': 'product-category'}).text if partner.find('div', {'class': 'product-name'}) else ''
            item['partner_type'], item['partner_tier'] = [cleanhtml(txt) for txt in type_info.split('•')]

            item['company_description'] = cleanhtml(partner.find('div', {'class': 'product-description'}).text) if partner.find('div', {'class': 'product-name'}) else ''

            data_pairs = partner.find('div', {'class': 'product-properties'}).find_all('div', {'class': 'product-property'})

            for pair in data_pairs:
                if 'Website' in pair.text:
                    item['company_domain_name'] = get_domain_from_url(cleanhtml(pair.find_all('div', recursive=False)[-1].text))

                if 'Twitter' in pair.text:
                    item['twitter_link'] = cleanhtml(pair.find_all('div', recursive=False)[-1].text)

                if 'LinkedIn' in pair.text:
                    item['linkedin_link'] = cleanhtml(pair.find_all('div', recursive=False)[-1].text)

                if 'Headquarters' in pair.text:
                    address = pair.find_all('div', recursive=False)[-1].text
                    if ',' in address:
                        item['headquarters_city'], item['headquarters_state'] = [cleanhtml(txt) for txt in address.split(',')]
                    else:
                        item['headquarters_address'] = cleanhtml(address)

                if 'Headcount' in pair.text:
                    item['company_size'] = cleanhtml(pair.find_all('div', recursive=False)[-1].text)

                if 'Market Cap' in pair.text:
                    item['market_cap'] = cleanhtml(pair.find_all('div', recursive=False)[-1].text)

            yield item
