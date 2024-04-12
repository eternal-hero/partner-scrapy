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
    name = 'sailpointpartner'
    partner_program_link = 'https://www.sailpoint.com/partners/system-integrators-and-resellers/'
    partner_directory = 'Sailpoint Partners Directory'
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
        partners = list()

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            scripts = soup.find_all('script')
            for script in scripts:
                if 'channelPartnersData' in script.text:
                    content = script.text[script.text.find('partners:'):].replace('<!--fwp-loop-->', '')
                    content = content[content.find(':')+1:]
                    content = cleanhtml(content[:content.rfind(']') + 1].replace("'", '"').replace('\\"', "'").strip())
                    while ",]" in content or ",}" in content or ", " in content or re.search(r"\w\"\w", content):
                        content = cleanhtml(content.replace(", ", ",").replace(",]", "]").replace(",}", "}").strip())
                        content = re.sub(r"\w\"\w", "'", content)
                    data = json.loads(content.strip())
                    if data and type(data) is list and len(data) > 0:
                        partners = data
                    break

        self.logger.info(f'Partners = {len(partners)}')
        for partner in partners:

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = ''

            item['partner_company_name'] = partner['partnerName']
            item['company_description'] = partner['partnerDesc'] if 'partnerDesc' in partner and partner['partnerDesc'] else ''
            item['certifications'] = [certificate['name'] for certificate in partner['certifications']] if 'certifications' in partner and partner['certifications'] and len(partner['certifications']) > 0 else ''
            item['regions'] = [region.replace('-', ' ').title() for region in partner['regions']] if 'regions' in partner and partner['regions'] else ''
            item['company_domain_name'] = get_domain_from_url(partner['partnerUrl'] if 'partnerUrl' in partner and partner['partnerUrl'] else '')

            item['partner_tier'] = list()

            if 'isPOY' in partner and partner['isPOY']:
                item['partner_tier'].append('Partner of the Year')
            if 'isSaasAdmiral' in partner and partner['isSaasAdmiral']:
                item['partner_tier'].append('SaaS Delivery Admiral')
            if 'isSoftwareAdmiral' in partner and partner['isSoftwareAdmiral']:
                item['partner_tier'].append('Software Delivery Admiral')

            yield item
