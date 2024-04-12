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
    name = 'clouderapartner'
    partner_program_link = 'https://www.cloudera.com/partners/partners-listing.html'
    partner_directory = 'Cloudera Partners Directory'
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
            partners = None
            soup = BS(response.text, "html.parser")
            scripts = soup.find_all('script')
            for script in scripts:
                if 'respObj' in script.text:
                    content = script.text[script.text.find('respObj'):]
                    content = content[content.find('=') + 1:]
                    content = content[: content.rfind(';')]
                    partners = json.loads(content.strip())
                    break

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

                    item['partner_company_name'] = partner['title']
                    item['company_domain_name'] = get_domain_from_url(partner['ctaLink'].strip()) if 'ctaLink' in partner and partner['description'] else ''

                    item['company_description'] = cleanhtml(partner['description']) if 'description' in partner and partner['description'] else ''

                    cert_list = partner['certList'] if 'certList' in partner and partner['certList'] and len(partner['certList']) > 0 else None
                    if cert_list:
                        item['certifications'] = [{'Versions': cert['clouderaVersion'],
                                                   'Partner Product Name': cert['partnerProductName'],
                                                   'Partner Product Version': cert['version'],
                                                   'Interface Components': cert['interfaceComponents'],
                                                   'Certified with Kerberos': cert['supportsKerberos'],
                                                   'Certified with Apache Sentry or Apache Ranger': cert['sentry']} for cert in cert_list]

                    if 'partnerTagMap' in partner and partner['partnerTagMap']:
                        partner = partner['partnerTagMap']
                        item['partner_type'] = partner['partner-type'] if 'partner-type' in partner and partner['partner-type'] else ''
                        item['categories'] = partner['partner-category'] if 'partner-category' in partner and partner['partner-category'] else ''
                        item['product_service_name'] = partner['partnerProductName'] if 'partnerProductName' in partner and partner['partnerProductName'] else ''
                        item['product_version'] = partner['clouderaVersion'] if 'clouderaVersion' in partner and partner['clouderaVersion'] else ''

                    yield item
