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
    name = 'hpepartner'
    partner_program_link = 'https://partnerconnect.hpe.com/partners'
    partner_directory = 'HPE Partner'
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
        page = 1
        size = 50
        api_link = f'https://api.partnerconnect.hpe.com/api/v2/searcher-site/partners?page={page}&size={size}&focus=*&geo=*'
        yield scrapy.Request(method='GET', url=api_link, callback=self.parse,
                             meta={'page': page, 'size': size})

    def parse(self, response):
        page = response.meta['page']
        size = response.meta['size']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            jsn_data = json.loads(response.text)
            if jsn_data and 'data' in jsn_data:
                partners = jsn_data['data']
                self.logger.info(f'HPE Page: {page}, Size: {size}, result partners: {len(partners)}')
                for partner in partners:
                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = ''

                    item['partner_company_name'] = partner['displayName']
                    item['partner_tier'] = partner['badge'] if 'badge' in partner else ''
                    item['company_domain_name'] = get_domain_from_url(partner['linkWebsite']) if 'linkWebsite' in partner else ''
                    item['general_email_address'] = partner['partnerMail'] if 'partnerMail' in partner else ''

                    partner_slug = partner['slug']
                    partner_link = f'https://partnerconnect.hpe.com/_next/data/cpSqdFvU9FPmOH_wo0iOf/en/partner/{partner_slug}.json?id={partner_slug}'
                    yield scrapy.Request(method='GET', url=partner_link, callback=self.parse_partner,
                                         dont_filter=True, meta={'item': item})

            # follow next pages
            if page == 1 and jsn_data and 'total' in jsn_data:
                total = jsn_data['total']
                pages = math.ceil(total / size)
                self.logger.info(f'HPE total number of pages of size: {size} = {pages}')
                for page in range(2, pages+1):
                    api_link = f'https://api.partnerconnect.hpe.com/api/v2/searcher-site/partners?page={page}&size={size}&focus=*&geo=*'
                    yield scrapy.Request(method='GET', url=api_link, callback=self.parse,
                                         meta={'page': page, 'size': size})

    def parse_partner(self, response):
        saved = False
        item = response.meta['item']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            jsn_data = json.loads(response.text)
            if jsn_data and 'pageProps' in jsn_data and 'element' in jsn_data['pageProps']:
                partner = jsn_data['pageProps']['element']
                item['partner_type'] = [itm.strip() for itm in (partner['partnerType'] if 'partnerType' in partner else list())]
                locations = partner['locations'] if 'locations' in partner else list()
                for location in locations:
                    if 'isHeadquarters' in location and location['isHeadquarters']:
                        item['headquarters_street'] = location['firstAddress'] if 'firstAddress' in location and location['firstAddress'] else ''
                        item['headquarters_city'] = location['city'] if 'city' in location and location['city'] else ''
                        item['headquarters_state'] = location['state'] if 'state' in location and location['state'] else ''
                        item['headquarters_zipcode'] = location['zip'] if 'zip' in location and location['zip'] else ''
                        item['headquarters_country'] = location['country'] if 'country' in location and location['country'] else ''

                        item['general_phone_number'] = location['phone'] if 'phone' in location and location['phone'] else ''
                        item['general_email_address'] = location['email'].strip() if 'email' in location and location['email'] and location['email'].strip() != '' else ''

                        if item['company_domain_name'] == '' and item['general_email_address'] != '':
                            item['company_domain_name'] = get_domain_from_url(item['general_email_address'])

                        item['certifications'] = location['certifications'] if 'certifications' in location and location['certifications'] else ''
                        item['competencies'] = location['competencies'] if 'competencies' in location and location['competencies'] else ''
                        item['specializations'] = location['specializations'] if 'specializations' in location and location['specializations'] else ''

                for location in locations:
                    if 'isHeadquarters' in location and not location['isHeadquarters']:
                        item['locations_street'] = location['firstAddress'] if 'firstAddress' in location and location['firstAddress'] else ''
                        item['locations_city'] = location['city'] if 'city' in location and location['city'] else ''
                        item['locations_state'] = location['state'] if 'state' in location and location['state'] else ''
                        item['locations_zipcode'] = location['zip'] if 'zip' in location and location['zip'] else ''
                        item['locations_country'] = location['country'] if 'country' in location and location['country'] else ''

                        item['general_phone_number'] = location['phone'] if 'phone' in location and location['phone'] and location['phone'].strip() != '' else ''
                        item['general_email_address'] = location['email'] if 'email' in location and location['email'] and location['email'].strip() != '' else ''

                        yield item
                        saved = True

        if not saved:
            yield item
