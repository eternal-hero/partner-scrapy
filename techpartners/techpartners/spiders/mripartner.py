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
    name = 'mripartner'
    partner_program_link = 'https://www.mrisoftware.com/uk/partners/#FindAPartner'
    partner_directory = 'MRI Partner'
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
            partners = soup.find('div', {'class': 'post_filtering_post_wrapper'}).find_all('div', {'class': 'grid_accordion_block--content'})
            self.logger.info(f'MRI has {len(partners)} PARTNERS')
            for partner in partners:

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = ''

                item['partner_company_name'] = partner.find('span', {'class': 'h2'}).text.strip()
                item['company_description'] = cleanhtml(partner.find('p').text if partner.find('p') else '')
                item['company_domain_name'] = get_domain_from_url(partner.find('a', {'href': True})['href'] if partner.find('a', {'href': True}) else '')

                optns = partner.find_all('div', {'class': 'mri-partner-fd'})
                for optn in optns:
                    if 'Category' in optn.find('span', {'class': 'pfd-title'}).text or \
                            'Categories' in optn.find('span', {'class': 'pfd-title'}).text:
                        item['categories'] = optn.find('span', {'class': 'pfd-value'}).text.strip()

                    elif 'Region' in optn.find('span', {'class': 'pfd-title'}).text:
                        item['regions'] = optn.find('span', {'class': 'pfd-value'}).text.strip()

                    elif 'Product' in optn.find('span', {'class': 'pfd-title'}).text:
                        item['products'] = optn.find('span', {'class': 'pfd-value'}).text.strip()

                    elif 'Solution' in optn.find('span', {'class': 'pfd-title'}).text:
                        item['solutions'] = optn.find('span', {'class': 'pfd-value'}).text.strip()

                yield item
