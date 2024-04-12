# import needed libraries
import json
import math
import re
import time

import requests
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'qualtricspartner'
    partner_program_link = 'https://www.qualtrics.com/partnerships/'
    partner_directory = 'Qualtrics Partner Directory'
    partner_program_name = ''
    crawl_id = None

    start_urls = [partner_program_link]
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

    def parse(self, response):
        industry_dict = dict()
        region_dict = dict()
        product_dict = dict()

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")

            menus = soup.find_all('div', {'class': 'dropdown-group'})
            for menu in menus:
                if 'INDUSTRY' in menu.text.upper():
                    lst = menu.find('ul', {'class': 'dropdown-menu'})
                    if lst:
                        optns = lst.find_all('a', {'data-slug': True})
                        for optn in optns:
                            if optn['data-slug'] and optn['data-slug'] != '':
                                industry_dict[optn['data-slug']] = optn.text

                elif 'REGION' in menu.text.upper():
                    lst = menu.find('ul', {'class': 'dropdown-menu'})
                    if lst:
                        optns = lst.find_all('a', {'data-slug': True})
                        for optn in optns:
                            if optn['data-slug'] and optn['data-slug'] != '':
                                region_dict[optn['data-slug']] = optn.text

                elif 'PRODUCT' in menu.text.upper():
                    lst = menu.find('ul', {'class': 'dropdown-menu'})
                    if lst:
                        optns = lst.find_all('a', {'data-slug': True})
                        for optn in optns:
                            if optn['data-slug'] and optn['data-slug'] != '':
                                product_dict[optn['data-slug']] = optn.text

            partners = soup.find_all('div', {'class': 'col-6 col-md-4 col-lg-3 mb-4 filters'})
            self.logger.info(f'Partners: {len(partners)}')
            for partner in partners:
                filter = partner['data-filter']
                partner_name = partner.find('h3', {'class': 'text-white'}).text
                partner_desc = partner.find('p', {'class': 'text-white'}).text if partner.find('p', {'class': 'text-white'}) else ''

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = ''

                item['partner_company_name'] = partner_name
                item['company_description'] = partner_desc

                item['industries'] = list()
                item['regions'] = list()
                item['products'] = list()

                for key in industry_dict.keys():
                    if key + ' ' in filter:
                        item['industries'].append(industry_dict[key])

                for key in product_dict.keys():
                    if key + ' ' in filter:
                        item['products'].append(product_dict[key])

                for key in region_dict.keys():
                    if key + ' ' in filter:
                        item['regions'].append(region_dict[key])

                partner_link = partner.find('a', {'href': True})['href'] if partner.find('a', {'href': True}) else None
                if partner_link:
                    if 'www.' in partner_link:
                        item['company_domain_name'] = get_domain_from_url(partner_link)
                        yield item
                    else:
                        yield scrapy.Request('https://www.qualtrics.com/partnerships/' + partner_link,
                                             callback=self.parse_partner,
                                             dont_filter=True,
                                             meta={'item': item})
                else:
                    yield item

    def parse_partner(self, response):
        item = response.meta['item']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
            yield item
        else:
            soup = BS(response.text, "html.parser")
            item['company_domain_name'] = soup.find('a', {'class': 'btn btn-arrow mb-6', 'href': True})['href'] if soup.find('a', {'class': 'btn btn-arrow mb-6', 'href': True}) else ''
            item['company_domain_name'] = get_domain_from_url(item['company_domain_name'])

            partner_descs = soup.find_all('p', {'class': 'spacing-xxs-top'})
            if len(partner_descs) > 0:
                item['company_description'] = ' '.join([cleanhtml(partner_desc.text) for partner_desc in partner_descs])

            yield item
