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
    name = 'riverbedpartner'
    partner_program_link = 'https://partnerlocator.riverbed.com/'
    partner_directory = 'Riverbed Partners Directory'
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
        countries = list()

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            scripts = soup.find_all('script')
            for script in scripts:
                if 'partnerData' in script.text:
                    content = script.text[script.text.find('window.partnerData'):]
                    content = content[content.find('=')+1:].strip('; ')
                    data = json.loads(content.strip())
                    if 'locations' in data:
                        countries = list(data['locations'].keys())
                    break

        for country in countries:
            page_number = 1
            country_link = self.partner_program_link + f'?country={country}&page={page_number}'
            yield scrapy.Request(country_link, callback=self.parse_country, dont_filter=True,
                                 meta={'page_number': page_number, 'country': country})

    def parse_country(self, response):
        country = response.meta['country']
        page_number = response.meta['page_number']
        if response.status != 200:
            self.logger.info(f'ERROR {country} REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            partners = soup.find('div', {'class': 'results'}).select('div[class="result"]')

            self.logger.info(f'Country: {country}, Page: {page_number}, result partners: {len(partners)}')

            for partner in partners:
                partner_name = None
                if partner.find('h2'):
                    partner_name = partner.find('h2').text if partner.find('h2') else None
                elif partner.find('img'):
                    if partner.find('img').get('alt') and partner.find('img').get('alt') != '':
                        partner_name = partner.find('img').get('alt')
                        if re.search(r"\s*logo\s*", partner_name, re.IGNORECASE):
                            partner_name = partner_name.replace(re.search(r"\s*logo\s*", partner_name, re.IGNORECASE).group(), '').strip()
                    elif partner.find('img').get('src') and partner.find('img').get('src') != '':
                        partner_name = partner.find('img').get('src').lower().replace('riverbed/logos/', '').replace('.png', '').replace('.jpg', '').strip().title()

                if partner_name:
                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = ''

                    item['partner_company_name'] = partner_name
                    item['headquarters_country'] = country

                    item['partner_tier'] = partner.find('div', {'class': re.compile(r"^level .*")}).text.replace('level ', '') if partner.find('div', {'class': re.compile(r"^level .*")}) else ''
                    item['partner_type'] = partner.find('h3').text if partner.find('h3') else ''

                    info = partner.find_all('div', {'class': 'field'})
                    for line in info:
                        if 'phone' in line.text.lower():
                            item['general_phone_number'] = line.text.lower().replace('phone', '').strip()
                        elif line.find('a', {'href': True}):
                            item['company_domain_name'] = get_domain_from_url(line.find('a', {'href': True})['href'])
                        elif re.search(r"Partner since\s\d{4}", line.text, re.IGNORECASE):
                            item['partnership_founding_date'] = re.search(r"\d{4}", line.text).group()
                        else:
                            item['headquarters_address'] = cleanhtml(line.text)

                    yield item

            # follow next pages
            if len(partners) == 10:
                page_number += 1
                country_link = self.partner_program_link + f'?country={country}&page={page_number}'
                yield scrapy.Request(country_link, callback=self.parse_country, dont_filter=True,
                                     meta={'page_number': page_number, 'country': country})
