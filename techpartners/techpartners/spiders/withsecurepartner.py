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
    name = 'withsecurepartner'
    partner_program_link = 'https://www.withsecure.com/en/partners/find-a-partner'
    partner_directory = 'WithSecure Partner'
    partner_program_name = ''
    crawl_id = None

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
        'Connection': 'keep-alive',
        'Host': 'web-dss.f-secure.com',
        'If-None-Match': 'W/"53237-CPhpNnOquaB4OhCfop61kVqcp5k"',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
    }

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
        api_link = 'https://web-dss.f-secure.com/api/partners'
        # api_link = 'https://www.booking.com'
        yield scrapy.Request(method='GET', headers=self.headers, url=api_link, callback=self.parse)

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            partners = json.loads(response.text.strip())
            # with open('withsecure.json', 'r', encoding='utf-8') as ifile:
            #     data = ifile.read()
            #     partners = json.loads(data)
            self.logger.info(f'WithSecure found {len(partners)} partners')

            for partner in partners:
                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = ''

                item['partner_company_name'] = partner.get('name')
                item['company_domain_name'] = get_domain_from_url(partner.get('url', ''))
                item['partner_type'] = partner.get('recordType', '')
                item['partner_tier'] = partner.get('level', '')
                item['general_phone_number'] = partner.get('phone', '')
                item['headquarters_street'] = cleanhtml(partner.get('address', ''))
                item['headquarters_city'] = partner.get('city', '')
                item['headquarters_state'] = partner.get('state', '')
                item['headquarters_zipcode'] = partner.get('postalCode', '')
                item['headquarters_country'] = partner.get('country', '')
                item['certifications'] = partner.get('certification', '')

                yield item
