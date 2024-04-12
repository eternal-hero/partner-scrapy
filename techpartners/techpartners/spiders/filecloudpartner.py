# import needed libraries
import json
import re
import math
from techpartners.spiders.base_spider import BaseSpider
import scrapy
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'filecloudpartner'
    partner_program_link = 'https://www.filecloud.com/resellers/#PartnerContactus'
    partner_directory = 'Filecloud Partner'
    partner_program_name = ''
    crawl_id = None

    start_urls = [partner_program_link]

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name',
                   'product_service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description',
                   'product_service_description',
                   'headquarters_address',
                   'headquarters_street', 'headquarters_city', 'headquarters_state', 'headquarters_zipcode',
                   'headquarters_country',
                   'locations_address',
                   'locations_street', 'locations_city', 'locations_state', 'locations_zipcode', 'locations_country',
                   'regions', 'languages', 'products', 'services', 'solutions',
                   'pricing_plan', 'pricing_model', 'pricing_plan_description',
                   'pricing', 'specializations', 'categories',
                   'features', 'account_requirements', 'product_package_name', 'year_founded', 'latest_update',
                   'publisher',
                   'partnership_timespan', 'partnership_founding_date', 'product_version', 'product_requirements',
                   'general_phone_number', 'general_email_address',
                   'support_phone_number', 'support_email_address', 'support_link', 'help_link', 'terms_and_conditions',
                   'license_agreement_link', 'privacy_policy_link',
                   'linkedin_link', 'twitter_link', 'facebook_link', 'youtube_link', 'instagram_link', 'xing_link',
                   'primary_contact_name', 'primary_contact_phone_number',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts',
                   'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    def parse(self, response):

        if response.status == 200:
            soup = BS(response.text, "html.parser")
            countries = soup.find('div', {'class': 'resellerContent'}).find_all('div', {'class': 'resellerBlock'})
            for country in countries:
                country_name = country['data-country'].title()
                print(country_name)
                partners = country.find_all('div', {'class': 'contentBox'})
                for partner in partners:
                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = self.partner_program_name

                    item['partner_company_name'] = partner.find('h5').text
                    item['headquarters_country'] = country_name

                    prefered = partner.find('div', {'class': 'preferred-partner'})
                    if prefered:
                        item['partner_tier'] = 'Preferred'

                    item['headquarters_address'] = partner.find('p').text if partner.find('p') else ''
                    data = partner.text.splitlines()
                    for l in data:
                        if l.startswith('Tel:'):
                            item['general_phone_number'] = l.strip('Tel:').strip().strip(':').strip(',').strip()
                        elif l.startswith('Website:'):
                            item['company_domain_name'] = l.strip('Website:').strip().strip(':').strip(',').strip()
                            url_obj = urllib.parse.urlparse(item['company_domain_name'])
                            item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                            x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                            if x:
                                item['company_domain_name'] = x[-1]
                            if '/' in item['company_domain_name']:
                                item['company_domain_name'] = item['company_domain_name'][:item['company_domain_name'].find('/')]

                    yield item
        else:
            print(response.status)
