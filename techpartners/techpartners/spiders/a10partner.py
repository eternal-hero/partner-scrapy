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
    name = 'a10partner'
    partner_program_link = 'https://www.a10networks.com/partners/resellers/'
    partner_directory = 'A10 Partners Directory'
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
            regions = soup.find_all('div', {'class': 'accordion-header'})
            for region in regions:
                if region.find('button', recursive=False):
                    item_region = region.text
                    content = region.findNext('div', {'class': 'accordion-body'})
                    if content and 'Please complete the form on the right' not in content.text and content.find('ul'):
                        tags = content.find_all(recursive=False)
                        for tag in tags:
                            if tag.name == 'strong' or (tag.name == 'p' and tag.find('strong')):
                                item_country = tag.text.strip()
                            elif tag.name == 'ul':
                                for partner in tag.find_all('li', recursive=False):

                                    # Initialize item
                                    item = dict()
                                    for k in self.item_fields:
                                        item[k] = ''

                                    item['partner_program_link'] = self.partner_program_link
                                    item['partner_directory'] = self.partner_directory
                                    item['partner_program_name'] = ''

                                    item['partner_company_name'] = partner.find('a').text if partner.find('a') else partner.text
                                    item['partner_type'] = re.findall(r"\(.*?\)", partner.text)[-1] if len(re.findall(r"\(.*?\)", partner.text)) > 0 else ''

                                    if item['partner_type'] in item['partner_company_name']:
                                        item['partner_company_name'] = item['partner_company_name'].replace(item['partner_type'], '').strip()

                                    item['partner_type'] = item['partner_type'][:item['partner_type'].rfind(')')].replace('(', '', 1).strip()

                                    item['company_domain_name'] = partner.find('a', {'href': True})['href'] if partner.find('a', {'href': True}) else ''
                                    if '@' in item['company_domain_name']:
                                        item['company_domain_name'] = 'www.' + item['company_domain_name'][item['company_domain_name'].find('@')+1:]
                                    item['company_domain_name'] = get_domain_from_url(item['company_domain_name'])

                                    item['regions'] = item_region
                                    item['headquarters_country'] = item_country

                                    yield item
