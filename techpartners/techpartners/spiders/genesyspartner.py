# import needed libraries
import json
import re
import math
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'genesyspartner'
    partner_program_link = 'https://genesys.secure.force.com/Finderloc/'
    partner_directory = 'Genesys Partner Directory'
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

        soup = BS(response.text, "html.parser")
        div = soup.find('div', {'id': 'j_id0:frmid:frmid2'})
        partners_tier = dict()
        for partner_soup in div.find_all('b'):
            if 'Name:' in partner_soup.text:
                name = partner_soup.find('a', onclick=True).text.strip()
                if partner_soup.find('img', id=re.compile(r'^j_id0:frmid:resultdisplaysection:\d+:theImage')):
                    partner_txt = \
                    partner_soup.find('img', id=re.compile(r'^j_id0:frmid:resultdisplaysection:\d+:theImage'))['src']
                    if partner_txt.strip() == '':
                        partners_tier[name] = 'Platinum'
                    elif 'GoldPartnerDirectory' in partner_txt:
                        partners_tier[name] = 'Gold'
                    elif 'SilverPartnerDirectory' in partner_txt:
                        partners_tier[name] = 'Silver'
                    elif 'BronzePartnerDirectory' in partner_txt:
                        partners_tier[name] = 'Bronze'
                else:
                    partners_tier[name] = ''

        partners = div.text.split('Name:')
        for partner in partners:
            if 'Total Result Found' in partner:
                continue

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = self.partner_program_name

            header = partner[:partner.find('Partner Type:')]
            item['partner_company_name'] = header.splitlines()[0].strip()
            item['partner_tier'] = partners_tier[item['partner_company_name']] if item['partner_company_name'] in partners_tier else ''

            for line in header.splitlines()[1:]:
                if line.strip() == '':
                    continue
                elif 'www' in line:
                    item['company_domain_name'] = line.strip()
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

                elif re.search(r'[\d\.()\-\s]+', line.strip()):
                    item['general_phone_number'] = line.strip()

            data = partner[partner.find('Partner Type:'): partner.find('Description:')]
            for line in data.splitlines():
                if line.strip() == '':
                    continue
                elif line.strip().startswith('Partner Type:'):
                    item['partner_type'] = line.strip().replace('Partner Type:', '').strip()
                elif line.strip().startswith('Partner Offerting Type:'):
                    item['integrations'] = line.strip().replace('Partner Offerting Type:', '').strip()
                elif line.strip().startswith('Region:'):
                    item['regions'] = line.strip().replace('Region:', '').strip()
                elif line.strip().startswith('Country:'):
                    item['headquarters_country'] = line.strip().replace('Country:', '').strip()
                elif line.strip().startswith('HQ Address:'):
                    item['headquarters_address'] = line.strip().replace('HQ Address:', '').strip()
                elif line.strip().startswith('Specialization:'):
                    item['specializations'] = line.strip().replace('Specialization:', '').strip()

            desc = partner[partner.find('Description:'):]
            item['company_description'] = desc.strip().replace('Description:', '').strip()

            yield item
