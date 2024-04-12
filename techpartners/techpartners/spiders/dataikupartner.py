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
    name = 'dataikupartner'
    partner_program_link = 'https://www.dataiku.com/partners/'
    partner_directory = 'Dataiku Partner'
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
    types_lst = dict()
    locations_lst = dict()

    def start_requests(self):
        try:
            r = requests.get('https://www.dataiku.com/wp-json/wp/v2/partners-type')
            if r.status_code == 200:
                data = json.loads(r.text)
                for i in data:
                    self.types_lst[i['id']] = i['name']
            else:
                raise Exception
        except:
            self.logger.info('ERROR: GET Types DATA')

        try:
            r = requests.get('https://www.dataiku.com/wp-json/wp/v2/partners-location')
            if r.status_code == 200:
                data = json.loads(r.text)
                for i in data:
                    self.locations_lst[i['id']] = i['name']
            else:
                raise Exception
        except:
            self.logger.info('ERROR: GET Types DATA')

        page = 1
        yield scrapy.Request(method='GET', url=f'https://www.dataiku.com/wp-json/wp/v2/partners?page_num={page}',
                             callback=self.parse, meta={'page': 1}, dont_filter=True)

    def parse(self, response):
        page = response.meta['page']
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            try:
                partners = json.loads(response.text)
            except:
                partners = list()

            self.logger.info(f'Dataiku partners: Page = {page}, partners = {len(partners)}')

            for partner in partners:

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = ''

                item['partner_company_name'] = partner['title']['rendered']

                item['partner_type'] = list()
                json_types = partner['partners-type']
                for partner_type in json_types:
                    if partner_type in self.types_lst.keys():
                        item['partner_type'].append(self.types_lst[partner_type])

                item['regions'] = list()
                json_regions = partner['partners-location']
                for partner_region in json_regions:
                    if partner_region in self.locations_lst.keys():
                        item['regions'].append(self.locations_lst[partner_region])

                if 'acf' in partner and 'single_popup_fields' in partner['acf'] and 'description' in partner['acf']['single_popup_fields']:

                    desc_soup = BS(partner['acf']['single_popup_fields']['description'], "html.parser")
                    item['company_description'] = cleanhtml(desc_soup.text)

                    if "website:" in desc_soup.text.lower():
                        desc_parts = desc_soup.find_all('p')
                        if len(desc_parts) > 0:
                            for desc_part in desc_parts:
                                if 'website:' in desc_part.text.lower() and desc_part.find('a', {'href': True}):
                                    item['company_domain_name'] = get_domain_from_url(desc_part.find('a', {'href': True})['href'])

                yield item

            # follow next page
            if len(partners) == 30:
                page += 1
                yield scrapy.Request(method='GET', url=f'https://www.dataiku.com/wp-json/wp/v2/partners?page_num={page}',
                                     callback=self.parse, meta={'page': page}, dont_filter=True)
