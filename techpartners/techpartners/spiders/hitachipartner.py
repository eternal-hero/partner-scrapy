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
    name = 'hitachipartner'
    partner_program_link = 'https://www.hitachivantara.com/partnerlocator/en_us/partnerlocator.html'
    partner_directory = 'Hitachi Partner'
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
    api_link = 'https://www.hitachivantara.com/partnerlocator/hds/partnerportal/search-partner'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'authority': 'www.hitachivantara.com',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'csrf-token': 'undefined',
        'origin': 'https://www.hitachivantara.com',
        'referer': 'https://www.hitachivantara.com/partnerlocator/en_us/partnerlocator.html/',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'x-kl-saas-ajax-request': 'Ajax_Request',
        'x-requested-with': 'XMLHttpRequest'
    }

    def start_requests(self):

        countries_url = 'https://www.hitachivantara.com/partnerlocator/en_us/partnerlocator/jcr:content/contentpar/partnerlocatorsearch.country.js?m=1'
        response = requests.get(countries_url)
        if response.status_code != 200:
            self.logger.info('FAILED TO START GETTING COUNTRIES DATA')
            return

        soup = BS(response.text.strip(), 'html.parser')
        countires = [{'name': country.text, 'value': country['value']} for country in soup.find_all('option') if
                     country['value']]

        for country in countires:
            payload = f'companyName=&state=&stateName=Select State/Province&country={country["value"]}&countryName={country["name"]}&branchLocation=false&solutionArea=No Preference&partnerDeliveryType=No Preference'
            yield scrapy.Request(method='POST', url=self.api_link, headers=self.headers,
                                 body=payload, callback=self.parse, meta={'country': country['name']})

    def parse(self, response):
        country = response.meta['country']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            if 'accountId' in response.text:
                partners = json.loads(response.text)
                self.logger.info(f'REQUEST COUNTRY {country}, PARTNERS = {len(partners)}')

                for partner in partners:

                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = ''
                    item['partner_company_name'] = partner.get('companyName', '')
                    item['partner_type'] = partner.get('businessModels', '')
                    item['company_domain_name'] = get_domain_from_url(partner.get('webSite', ''))
                    item['competencies'] = cleanhtml(partner.get('competencies', ''))
                    item['headquarters_country'] = partner.get('country', '')
                    item['headquarters_state'] = partner.get('state', '')
                    item['headquarters_zipcode'] = partner.get('zipCode', '')
                    item['headquarters_city'] = partner.get('city', '')
                    item['headquarters_street'] = partner.get('addressLineOne', '')

                    item['partner_tier'] = partner.get('partnerLevel', '')
                    item['general_phone_number'] = partner.get('contact', '')
                    item['company_description'] = cleanhtml(partner.get('description', ''))

                    yield item
