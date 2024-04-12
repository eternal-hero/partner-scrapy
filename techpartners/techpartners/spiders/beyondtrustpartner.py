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
    name = 'beyondtrustpartner'
    partner_program_link = 'https://www.beyondtrust.com/partners/directory'
    partner_directory = 'BeyondTrust Partners Directory'
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
            regions = [{'label': region.text, 'value': region['value']} for region in soup.find_all('option', {'data-filter': 'region', 'value': True})]
            countries = [{'label': country.text, 'value': country['value']} for country in soup.find_all('option', {'data-filter': 'countries', 'value': True})]

            for partner in soup.find_all('article', {'class': 'card'}):

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = ''

                item['partner_company_name'] = partner.find('h2').text if partner.find('h2') else ''
                item['partner_type'] = partner.find('p', {'class': 'partner-type'}).text if partner.find('p', {'class': 'partner-type'}) else ''
                item['partner_tier'] = partner.find('p', {'class': 'partner-level'}).text if partner.find('p', {'class': 'partner-level'}) else ''

                # item['partner_company_name'] = partner['data-card-title']
                # item['partner_type'] = partner['data-card-type']
                # item['partner_tier'] = partner['data-card-level']

                item['company_domain_name'] = partner.find('a', {'href': True})['href'] if partner.find('a', {'href': True}) else ''
                item['company_domain_name'] = get_domain_from_url(item['company_domain_name'])

                item['regions'] = list()
                item['headquarters_country'] = list()

                partner_info = partner['data-card-countries'] if partner.has_attr('data-card-countries') else ''
                for region in regions:
                    if region['value'] != '' and region['value'] in partner_info:
                        item['regions'].append(region['label'])

                for country in countries:
                    if country['value'] != '' and country['value'] in partner_info:
                        item['headquarters_country'].append(country['label'])

                item_id = partner['data-card-id'] if partner.has_attr('data-card-id') else ''
                if item_id:
                    partner_link = f'https://www.beyondtrust.com/partner-api?id={item_id}'

                    yield scrapy.Request(partner_link, callback=self.parse_partner,
                                         meta={'item': item}, dont_filter=True)
                else:
                    yield item

    def parse_partner(self, response):
        item = response.meta['item']
        if response.status != 200:
            self.logger.info(f'ERROR PARTNER REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            try:
                data = json.loads(response.text)
            except json.decoder.JSONDecodeError:
                data = None

            if data and 'description' in data:
                item['company_description'] = data['description']

            yield item
