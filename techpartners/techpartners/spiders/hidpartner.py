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
    name = 'hidpartner'
    partner_program_link = 'https://www.hidglobal.com/partners'
    partner_directory = 'HID Partner'
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
            'authority': 'www.hidglobal.com',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'referer': 'https://www.hidglobal.com/partners',
            'sec-ch-ua': '"Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': 'Windows',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
        }
        yield scrapy.Request(method='GET',
                             url='https://www.hidglobal.com/apps/partners/v1/assets/json/sf.json',
                             callback=self.parse,
                             headers=headers)

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            partners = json.loads(response.text)
            if partners and len(partners) > 0:
                self.logger.info(f'HID result partners: {len(partners)}')
                for itm in partners:
                    partner = itm['details']
                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = ''

                    item['partner_company_name'] = partner['Company_Display_Name__c']
                    item['partner_type'] = partner['type'] if 'type' in partner and partner['type'] else ''
                    item['partner_tier'] = partner['tier'] if 'tier' in partner and partner['tier'] else ''
                    item['company_domain_name'] = get_domain_from_url(partner['Corporate_Website__c']) if 'Corporate_Website__c' in partner and partner['Corporate_Website__c'] else ''

                    item['general_phone_number'] = partner['Phone_Number__c'] if 'Phone_Number__c' in partner and partner['Phone_Number__c'] else ''
                    item['general_email_address'] = partner['Email__c'] if 'Email__c' in partner and partner['Email__c'] else ''

                    item['company_description'] = cleanhtml(partner['Company_Description__c']) if 'Company_Description__c' in partner and partner['Company_Description__c'] else ''

                    item['headquarters_street'] = cleanhtml(partner['Street__c']) if 'Street__c' in partner and partner['Street__c'] else ''
                    item['headquarters_city'] = partner['City__c'] if 'City__c' in partner and partner['City__c'] else ''
                    item['headquarters_state'] = partner['State__c'] if 'State__c' in partner and partner['State__c'] else ''
                    item['headquarters_zipcode'] = partner['Zip_Postal_Code__c'] if 'Zip_Postal_Code__c' in partner and partner['Zip_Postal_Code__c'] else ''
                    item['headquarters_country'] = partner['Country__c'] if 'Country__c' in partner and partner['Country__c'] else ''

                    business_info = itm['segments']
                    solutions = list()
                    industries = set()
                    products = set()
                    for info in business_info:
                        if 'Business_Segment__c' in info and info['Business_Segment__c']: solutions.append(info['Business_Segment__c'])
                        industries.update(info['Industries__c'] if 'Industries__c' in info and info['Industries__c'] else set())
                        products.update(info['Products__c'] if 'Products__c' in info and info['Products__c'] else set())

                    item['solutions'] = ', '.join(solutions)
                    item['industries'] = ', '.join(industries)
                    item['products'] = ', '.join(products)

                    yield item
