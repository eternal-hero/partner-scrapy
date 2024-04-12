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
    name = 'hashicorppartner'
    partner_program_link = 'https://www.hashicorp.com/partners/find-a-partner'
    partner_directory = 'Hashicorp Partner'
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

    start_urls = ['https://www.hashicorp.com/_next/data/RMLKy27fiSj8vOuU8zhE8/partners/find-a-partner.json']

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            try:
                jsn_data = json.loads(response.text)
            except:
                jsn_data = None

            if jsn_data and 'pageProps' in jsn_data:
                for partner_type in jsn_data['pageProps']:
                    partners = jsn_data['pageProps'][partner_type]['partners']
                    self.logger.info(f'Hashicorp {partner_type}, result partners: {len(partners)}')
                    for partner in partners:
                        # Initialize item
                        item = dict()
                        for k in self.item_fields:
                            item[k] = ''

                        item['partner_program_link'] = self.partner_program_link
                        item['partner_directory'] = self.partner_directory
                        item['partner_program_name'] = ''

                        item['partner_company_name'] = partner['name']
                        item['partner_tier'] = partner['tier'] if 'tier' in partner and partner['tier'] else ''
                        item['company_description'] = cleanhtml(partner['description']) if 'description' in partner and partner['description'] else ''

                        partner_slug = partner['href'][partner['href'].rfind('/')+1:]
                        if partner_type == 'techPartners':
                            item['partner_type'] = 'Technology Partner'
                            partner_link = f'https://www.hashicorp.com/_next/data/RMLKy27fiSj8vOuU8zhE8/partners/tech/{partner_slug}.json?company={partner_slug}'
                            referer = f'https://www.hashicorp.com/partners/tech/{partner_slug}'

                        elif partner_type == 'systemsIntegrators':
                            item['partner_type'] = 'Systems Integrator'
                            partner_link = f'https://www.hashicorp.com/_next/data/RMLKy27fiSj8vOuU8zhE8/partners/systems-integrators/{partner_slug}.json?company={partner_slug}'
                            referer = f'https://www.hashicorp.com/partners/systems-integrators/{partner_slug}'

                        elif partner_type == 'trainingPartners':
                            item['partner_type'] = 'Training Partner'
                            partner_link = f'https://www.hashicorp.com/_next/data/RMLKy27fiSj8vOuU8zhE8/partners/systems-integrators/{partner_slug}.json?company={partner_slug}'
                            referer = f'https://www.hashicorp.com/partners/systems-integrators/{partner_slug}'

                        elif partner_type == 'cloudPartners':
                            item['partner_type'] = 'Cloud Partner'
                            partner_link = f'https://www.hashicorp.com/_next/data/RMLKy27fiSj8vOuU8zhE8/partners/cloud/{partner_slug}.json?company={partner_slug}'
                            referer = f'https://www.hashicorp.com/partners/cloud/{partner_slug}'

                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
                            'Authority': 'www.hashicorp.com',
                            'Accept': '*/*',
                            'content-type': 'application/json',
                            'Accept-Language': 'en-US,en;q=0.9',
                            'Accept-Encoding': 'gzip, deflate',
                            'Cache-Control': 'no-cache',
                            'cookie': 'hc_geo=country%3DEG%2Cregion%3DGZ',
                            'Content-Type': 'application/json',
                            'Sec-Ch-Ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
                            'Sec-Ch-Ua-mobile': '?0',
                            'Sec-Ch-Ua-platform': 'Windows',
                            'Sec-Fetch-Dest': 'empty',
                            'Sec-Fetch-Mode': 'cors',
                            'Sec-Fetch-Site': 'same-origin',
                            'Referer': referer,
                            'x-nextjs-data': '1',
                        }

                        yield scrapy.Request(method='GET', url=partner_link, callback=self.parse_partner,
                                             headers=headers,
                                             dont_filter=True, meta={'item': item})

    def parse_partner(self, response):
        item = response.meta['item']

        if item['partner_type'] == 'Technology Partner' or item['partner_type'] == 'Cloud Partner':
            key = 'company'
        elif item['partner_type'] == 'Systems Integrator' or item['partner_type'] == 'Training Partner':
            key = 'systemsIntegrator'
        else:
            key = None

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            try:
                jsn_data = json.loads(response.text)
            except:
                jsn_data = None

            if key and jsn_data and 'pageProps' in jsn_data and key in jsn_data['pageProps']:
                partner = jsn_data['pageProps'][key]
                item['company_domain_name'] = get_domain_from_url(partner['websiteUrl']) if 'websiteUrl' in partner and partner['websiteUrl'] else (get_domain_from_url(partner['link']) if 'link' in partner and partner['link'] else '')
        yield item
