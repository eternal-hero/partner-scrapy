# import needed libraries
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
    name = 'msppartner'
    partner_program_link = 'https://sponsors.themspsummit.com/?_mc=em_cfms_x_le_x_letter-kelly_2022&_gl=1*19ofpw9*_ga*NzM5MzUyMjI2LjE2NjEyMDI1NzM.*_ga_QXRJQ6VWNF*MTY2MTIwMjU3My4xLjEuMTY2MTIwMzY5MS4wLjAuMA..&_ga=2.36640736.193041211.1661202574-739352226.1661202573'
    partner_directory = 'MSP Summit Directory'
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
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            div = soup.find('div', {'class': 'ev2 exhibitor-list'}).find('div', {'id': 'atAGlance'}).find('h3', {'class': 'sponsor-text'}).parent
            tags = div.find_all(recursive=False)
            sponsor_type = ''
            for sponsor in tags:
                if sponsor.has_attr('class'):
                    if 'clear' in sponsor['class'] or 'clearfix' in sponsor['class']:
                        continue

                    elif 'sponsor-text' in sponsor['class']:
                        sponsor_type = sponsor.text

                    else:
                        sponsor_name = sponsor.find_all('a', {'class': 'turnbuckle_plus'})[-1].text if sponsor.find('a', {'class': 'turnbuckle_plus'}) else None

                        if sponsor_name:
                            # Initialize item
                            item = dict()
                            for k in self.item_fields:
                                item[k] = ''

                            item['partner_program_link'] = self.partner_program_link
                            item['partner_directory'] = self.partner_directory
                            item['partner_program_name'] = self.partner_program_name

                            item['partner_company_name'] = sponsor_name
                            item['partner_type'] = sponsor_type

                            sponsor_data = sponsor.find('div', {'class': 'autocls'})
                            if sponsor_data:
                                divs = sponsor_data.find_all('p', {'class': True}, recursive=False)
                                for sctn in divs:
                                    if sctn.find('strong') and 'Products/Services Offered' in sctn.find('strong').text:
                                        ul = sctn.find_next('ul')
                                        item['products'] = [li.text.strip() for li in ul.find_all('li')]
                                    elif sctn.find('strong') and 'Products' in sctn.find('strong').text:
                                        ul = sctn.find_next('ul')
                                        item['product_service_name'] = [li.text for li in ul.find_all('li')]

                                divs = sponsor_data.find_all('p', {'class': False}, recursive=False)
                                for sctn in divs:
                                    if sctn.find('strong') and 'Website' in sctn.find('strong').text:
                                        item['company_domain_name'] = sctn.find('a', {'href': True})['href'] if sctn.find('a', {'href': True}) else ''
                                        try:
                                            url_obj = urllib.parse.urlparse(item['company_domain_name'])
                                            item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                                            x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                                            if x:
                                                item['company_domain_name'] = x[-1]
                                            if '/' in item['company_domain_name']:
                                                item['company_domain_name'] = item['company_domain_name'][:item['company_domain_name'].find('/')]
                                        except Exception as e:
                                            print('DOMAIN ERROR: ', e)

                                    elif sctn.find('strong') and 'Social Networks' in sctn.find('strong').text:
                                        links = sctn.find_all('a', {'href': True})
                                        for social in links:
                                            if 'linkedin' in social['title']:
                                                item['linkedin_link'] = social['href']

                                            elif 'twitter' in social['title']:
                                                item['twitter_link'] = social['href']

                                            elif 'facebook' in social['title']:
                                                item['facebook_link'] = social['href']

                                    else:
                                        item['company_description'] = cleanhtml(sctn.text)

                                address = sponsor_data.find('p', {'class': 'exhibitorListAddress'})
                                if address:
                                    item['headquarters_address'] = ' '.join([cleanhtml(line.strip()) for line in address.find_all(text=True, recursive=False)])
                                yield item
                            else:
                                yield item
