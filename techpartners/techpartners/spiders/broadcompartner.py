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
    name = 'broadcompartner'
    partner_program_link = 'https://www.broadcom.com/how-to-buy/partner-distributor-lookup'
    partner_directory = 'Broadcom Partner'
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
        response = requests.get('https://www.broadcom.com/api/getjson?url=how-to-buy/partner-distributor-lookup&locale=en-us&updated_date=2022-11-22T18:24:24.487000Z&type=general_page&id=bltc7fcc26d4cb72fc2_en-us')
        data = response.json()
        if 'partners' in data:
            partner_type_dicts = [{'label': partner_type['product_type'], 'link': partner_type['url']} for partner_type in data['partners']['product_types']]

            for partner_type_dict in partner_type_dicts:
                partner_type = partner_type_dict['label']
                partner_link = 'https://www.broadcom.com' + partner_type_dict['link']

                yield scrapy.Request(method='GET', url=partner_link,
                                     callback=self.parse,
                                     dont_filter=True,
                                     meta={'partner_type': partner_type})

    def parse(self, response):
        partner_type = response.meta['partner_type']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            try:
                if 'region' in response.text:
                    regions = json.loads(response.text)['region']
                else:
                    raise Exception
            except:
                regions = None

            if regions:
                for region in regions:
                    region_name = region['name']
                    countries = region['country']
                    for country in countries:
                        locations = country['locations']
                        for partner in locations:

                            # Initialize item
                            item = dict()
                            for k in self.item_fields:
                                item[k] = ''

                            item['partner_program_link'] = self.partner_program_link
                            item['partner_directory'] = self.partner_directory
                            item['partner_program_name'] = ''

                            item['partner_company_name'] = partner['name']
                            item['categories'] = partner_type
                            item['partner_tier'] = [txt['name'] for txt in partner['filter']] if 'filter' in partner and partner['filter'] else ''
                            item['regions'] = region_name
                            item['general_phone_number'] = partner['phone'] if 'phone' in partner and partner['phone'] else ''
                            item['general_email_address'] = partner['email'] if 'email' in partner and partner['email'] else ''

                            item['company_domain_name'] = get_domain_from_url(partner['web']['url']) if 'web' in partner and partner['web'] and 'url' in partner['web'] and partner['web']['url'] else ''

                            item['headquarters_country'] = partner['country'] if 'country' in partner and partner['country'] else ''
                            item['headquarters_address'] = ' '.join([partner['address_line1'], (partner['address_line2'] if 'address_line2' in partner and partner['address_line2'] else ''), (partner['address_line3'] if 'address_line3' in partner and partner['address_line3'] else '')]).strip() if 'address_line1' in partner and partner['address_line1'] else ''
                            item['headquarters_state'] = partner['state'] if 'state' in partner and partner['state'] else ''
                            item['headquarters_zipcode'] = partner['postal_code'] if 'postal_code' in partner and partner['postal_code'] else ''

                            yield item
