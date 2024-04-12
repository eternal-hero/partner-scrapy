# import needed libraries
import json
import re
import math
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


def cfDecodeEmail(encodedString):
    r = int(encodedString[:2], 16)
    email = ''.join([chr(int(encodedString[i:i + 2], 16) ^ r) for i in range(2, len(encodedString), 2)])
    return email


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'alvariapartner'
    partner_program_link = 'https://www.alvaria.com/resources/partners/find-a-partner'
    partner_directory = 'Alvaria Partner Directory'
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
                   'primary_contact_name', 'primary_contact_phone_number',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
            return
        region = ''
        soup = BS(response.text, "html.parser")
        div = soup.find('div', {'id': 'Contentplaceholder1_C002_Col00'})
        for inner in div.find_all('div', recursive=False):
            if inner.text.strip() == '':
                continue
            elif inner.find('h2', {'class': 'orange'}):
                region = inner.find('h2', {'class': 'orange'}).text
            else:
                for partner in inner.find_all('div', {'data-sf-element': 'Row'}):
                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = self.partner_program_name

                    data_div = partner.find('div', {'data-sf-element': "Column 2"})
                    item['partner_company_name'] = data_div.find('strong').text.strip()
                    item['headquarters_address'] = cleanhtml(data_div.find('strong').find_next('p').text.strip())
                    item['regions'] = region

                    if 'Phone:' in item['headquarters_address']:
                        item['headquarters_address'] = item['headquarters_address'][:item['headquarters_address'].find('Phone:')].strip()

                    email_tag = partner.find('span', {'class': '__cf_email__'})
                    if email_tag:
                        item['general_email_address'] = cfDecodeEmail(partner.find('span', {'class': '__cf_email__'})['data-cfemail'])

                    for br in partner.find_all("br"):
                        br.replace_with("\n")

                    for p in partner.find_all('p'):
                        for line in p.text.splitlines():
                            if line.strip() == '':
                                continue
                            elif line.strip().startswith('Phone:') and item['general_phone_number'] == '':
                                item['general_phone_number'] = line.strip().replace('Phone:', '').strip()
                            elif line.strip().startswith('Web:') and item['company_domain_name'] == '':
                                item['company_domain_name'] = line.strip().replace('Web:', '').strip()
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
                            # elif line.strip().startswith('Email:') and item['general_email_address'] == '':
                            #     item['general_email_address'] = line.strip().replace('Email:', '').strip()

                    yield item
