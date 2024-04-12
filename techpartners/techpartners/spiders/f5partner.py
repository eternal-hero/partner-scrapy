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
    name = 'f5partner'
    partner_program_link = 'https://www.f5.com/partners/find-a-partner'
    partner_directory = 'F5 Partners Directory'
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
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    def start_requests(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'internal.apis.f5.com',
            'Origin': 'https://www.f5.com',
            'Referer': 'https://www.f5.com/partners/find-a-partner',
            'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': 'Windows',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'X-Apikey': 'oUaaveJYG1aRa3wyHInyAImYk28nePl6',
            }

        yield scrapy.Request(method='GET', url='https://internal.apis.f5.com/partnernet/findpartner?ctrl=ptnrs',
                             headers=headers, callback=self.parse)

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            try:
                partners = json.loads(response.text.strip())
            except:
                partners = None

            if partners:
                self.logger.info(f'Partners = {len(partners)}')
                for partner in partners:

                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = ''

                    item['partner_company_name'] = partner['name']
                    item['company_domain_name'] = get_domain_from_url(partner['website'] if 'website' in partner and partner['website'] else '').strip(',').strip()
                    item['partner_type'] = partner['partnerType'] if 'partnerType' in partner and partner['partnerType'] else ''
                    item['partner_tier'] = partner['partnerLevel'] if 'partnerLevel' in partner and partner['partnerLevel'] else ''
                    item['regions'] = partner['regionName'] if 'regionName' in partner and partner['regionName'] else ''

                    item['headquarters_country'] = partner['country'] if 'country' in partner and partner['country'] else ''
                    item['headquarters_zipcode'] = partner['postcode'] if 'postcode' in partner and partner['postcode'] else ''
                    item['headquarters_state'] = partner['countyState'] if 'countyState' in partner and partner['countyState'] else ''
                    item['headquarters_city'] = partner['city'] if 'city' in partner and partner['city'] else ''
                    item['headquarters_street'] = partner['address1'] if 'address1' in partner and partner['address1'] else ''

                    item['industries'] = partner['industry'] if 'industry' in partner and partner['industry'] else ''
                    item['general_phone_number'] = partner['main_phone'] if 'main_phone' in partner and partner['main_phone'] else ''
                    item['general_email_address'] = partner['generic_email'] if 'generic_email' in partner and partner['generic_email'] else ''

                    yield item
