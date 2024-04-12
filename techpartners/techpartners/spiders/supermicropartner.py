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
    name = 'supermicropartner'
    partner_program_link = 'https://www.supermicro.com/en/wheretobuy'
    partner_directory = 'SuperMicro Partners Directory'
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

    api_link = 'https://www.supermicro.com/en/external_db_block/wheretobuy/getResllerList'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Connection': 'keep-alive',
        'Authority': 'www.supermicro.com',
        'Origin': 'https://www.supermicro.com',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'x-requested-with': 'XMLHttpRequest',
    }
    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
    }

    def start_requests(self):
        page_number = 1
        payload = f'seed=22769&region=&country=0&keywords=&perPageRowCount=20&page={page_number}'
        yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse, body=payload,
                             headers=self.headers, dont_filter=True, meta={'page_number': page_number})

    def parse(self, response):
        page_number = response.meta['page_number']
        try:
            data = json.loads(response.text.strip())
            if 'returnData' in data:
                partners = data['returnData']['aList']
                totalPage = data['returnData']['totalPage']
            else:
                raise Exception
        except:
            partners = None
            totalPage = None

        if partners and totalPage:
            self.logger.info(f'Page: {page_number} of {totalPage}, Partners = {len(partners)}')
            for partner in partners:

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = ''

                item['partner_company_name'] = partner['company']
                item['company_domain_name'] = get_domain_from_url(partner['website'] if 'website' in partner and partner['website'] else '')

                item['regions'] = partner['regionname'] if 'regionname' in partner and partner['regionname'] else ''
                item['headquarters_country'] = partner['countryname'] if 'countryname' in partner and partner['countryname'] else ''
                contact = partner['contactinfo'] if 'contactinfo' in partner and partner['contactinfo'] else None
                if contact and len(contact) > 0:
                    item['headquarters_address'] = cleanhtml(contact[0]['address']) if 'address' in contact[0] and contact[0]['address'] else ''
                    item['general_phone_number'] = contact[0]['tel'] if 'tel' in contact[0] and contact[0]['tel'] else ''
                    item['general_email_address'] = contact[0]['email'] if 'email' in contact[0] and contact[0]['email'] else ''

                yield item

            # follow next pages
            if page_number == 1 and totalPage:
                for i in range(2, totalPage+1):
                    payload = f'seed=22769&region=&country=0&keywords=&perPageRowCount=20&page={i}'
                    yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse, body=payload,
                                         headers=self.headers, dont_filter=True, meta={'page_number': i})
