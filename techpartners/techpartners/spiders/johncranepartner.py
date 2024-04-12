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
    name = 'johncranepartner'
    partner_program_link = 'https://www.johncrane.com/en/contact-us/distributors'
    partner_directory = 'JohnCrane Partners Directory'
    partner_program_name = ''
    crawl_id = None

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name',
                   'product_service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description',
                   'product_service_description',
                   'headquarters_address',
                   'headquarters_street', 'headquarters_city', 'headquarters_state', 'headquarters_zipcode',
                   'headquarters_country',
                   'locations_address',
                   'locations_street', 'locations_city', 'locations_state', 'locations_zipcode', 'locations_country',
                   'regions', 'languages', 'products', 'services', 'solutions',
                   'pricing_plan', 'pricing_model', 'pricing_plan_description',
                   'pricing', 'specializations', 'categories',
                   'features', 'account_requirements', 'product_package_name', 'year_founded', 'latest_update',
                   'publisher',
                   'partnership_timespan', 'partnership_founding_date', 'product_version', 'product_requirements',
                   'general_phone_number', 'general_email_address',
                   'support_phone_number', 'support_email_address', 'support_link', 'help_link', 'terms_and_conditions',
                   'license_agreement_link', 'privacy_policy_link',
                   'linkedin_link', 'twitter_link', 'facebook_link', 'youtube_link', 'instagram_link', 'xing_link',
                   'primary_contact_name', 'primary_contact_phone_number', 'primary_contact_email',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts',
                   'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']
    start_urls = [partner_program_link]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        'Authority': 'www.johncrane.com',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Content-Type': 'application/json; charset=UTF-8',
        'Origin': 'https://www.johncrane.com',
        'Referer': 'https://www.johncrane.com/en/contact-us/distributors',
        'Sec-Ch-Ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
        'Sec-Ch-Ua-mobile': '?0',
        'Sec-Ch-Ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'X-Requested-With': 'XMLHttpRequest',
    }

    done_partners = dict()

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            regions = [{'value': rgn['value'], 'label': rgn.text} for rgn in soup.find('select', {'name': 'region'}).find_all('option')]

            # self.logger.info(f'Regions : {regions}')
            for region in regions:
                region_response = requests.request(method='POST',
                                                   url='https://www.johncrane.com/J/Johncrane_com/Services/Location.asmx/GetLocationListingFiltered',
                                                   data='{"filter":"distributors","region":"%s","language":"en"}' % region['value'],
                                                   headers=self.headers)

                if region_response.status_code != 200:
                    self.logger.info(f'ERROR REQUEST STATUS: {region_response.status_code}, URL: {region_response.url}')
                else:
                    try:
                        partners = json.loads(json.loads(region_response.text)['d'])
                    except:
                        self.logger.info(f'ERROR GET PARTNERS DATA, STATUS: {region_response.status_code}, URL: {region_response.request.url}')
                        partners = list()

                    for partner in partners:

                        partner_id = partner['Id']

                        if partner_id in self.done_partners.keys():
                            item = self.done_partners[partner_id]
                        else:
                            # Initialize item
                            item = dict()
                            for k in self.item_fields:
                                item[k] = ''

                            item['partner_program_link'] = self.partner_program_link
                            item['partner_directory'] = self.partner_directory
                            item['partner_program_name'] = ''

                            item['partner_company_name'] = partner['PlaceName']
                            item['headquarters_country'] = partner['Country']
                            item['company_description'] = cleanhtml(partner['Description'])

                            info = partner['Info']
                            partner_soup = BS(info, "html.parser")
                            rows = partner_soup.find_all('dt')
                            for row in rows:
                                if 'Website' in row.text:
                                    item['company_domain_name'] = get_domain_from_url(row.findNext('dd').text)
                                if 'Address' in row.text:
                                    item['headquarters_address'] = row.findNext('dd').text
                                if 'Phone' in row.text:
                                    item['general_phone_number'] = row.findNext('dd').text
                                if 'Email' in row.text:
                                    item['general_email_address'] = row.findNext('dd').text
                                    item['company_domain_name'] = get_domain_from_url(item['general_email_address'])

                            item['regions'] = list()

                        if region['value'] != '':
                            item['regions'].append(region['label'])
                        self.done_partners[partner_id] = item

            for item in self.done_partners.values():
                yield item
