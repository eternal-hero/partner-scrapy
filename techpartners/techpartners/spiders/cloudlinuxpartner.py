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
    name = 'cloudlinuxpartner'
    partner_program_link = 'https://www.cloudlinux.com/partners/'
    partner_directory = 'CloudLinux Partners Directory'
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

            links_dict = dict()
            scripts = soup.find_all('script')
            for script in scripts:
                if 'et_link_options_data' in script.text:
                    content = script.text[script.text.find('et_link_options_data'):]
                    content = content[content.find('=') + 1:]
                    content = content[: content.rfind(';')]
                    links = json.loads(content.strip())
                    for link in links:
                        links_dict[link['class']] = link['url']
                    break

            div = soup.find('div', id='partners-section')
            rows = div.find_all('div', recursive=False)
            for row in rows[1:]:
                for partner in row.find_all('div', recursive=False):
                    if partner.find('div', {'class': 'contact-box'}):
                        partner_divs = partner.find_all('div', recursive=False)

                        # Initialize item
                        item = dict()
                        for k in self.item_fields:
                            item[k] = ''

                        item['partner_program_link'] = self.partner_program_link
                        item['partner_directory'] = self.partner_directory
                        item['partner_program_name'] = ''

                        item['partner_company_name'] = partner.find('div', {'class': 'contact-box'}).text.strip()
                        item['company_description'] = cleanhtml(partner_divs[2].text.strip())
                        if item['company_description'].endswith('Read more'):
                            item['company_description'] = item['company_description'][:item['company_description'].rfind('Read more')].strip()

                        if partner_divs[3]['class'][2] in links_dict:
                            item['company_domain_name'] = get_domain_from_url(links_dict[partner_divs[3]['class'][2]])

                        yield item
