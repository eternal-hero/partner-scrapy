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
    name = 'tenablepartner'
    partner_program_link = 'https://www.tenable.com/partner-locator/resellers'
    partner_directory = 'Tenable Partners Directory'
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

    start_urls = [partner_program_link]

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            scrpt = soup.find('script', id='__NEXT_DATA__')
            if scrpt:
                try:
                    data = json.loads(scrpt.text.strip())
                    if 'props' in data:
                        partners = data['props']['pageProps']['partners']
                    else:
                        raise Exception
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
                    item['company_domain_name'] = get_domain_from_url(partner['website'] if 'website' in partner and partner['website'] else '')
                    item['partner_type'] = partner['type'] if 'type' in partner and partner['type'] else ''
                    item['partner_tier'] = partner['level'] if 'level' in partner and partner['level'] else ''
                    item['company_description'] = partner['description'] if 'description' in partner and partner['description'] else ''

                    item['regions'] = partner['theater'] if 'theater' in partner and partner['theater'] else ''
                    item['headquarters_country'] = partner['country'] if 'country' in partner and partner['country'] else ''
                    item['headquarters_zipcode'] = partner['zip'] if 'zip' in partner and partner['zip'] else ''
                    item['headquarters_state'] = partner['state'] if 'state' in partner and partner['state'] else ''
                    item['headquarters_city'] = partner['city'] if 'city' in partner and partner['city'] else ''
                    item['headquarters_street'] = cleanhtml(partner['address']) if 'address' in partner and partner['address'] else ''

                    yield item
