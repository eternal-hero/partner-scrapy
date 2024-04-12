# import needed libraries
import json
import re
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'asanapartner'
    partner_program_link = 'https://asana.com/partners/channel/directory'
    partner_directory = 'Asana Partner Directory'
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
        div = soup.find('div', {'class': 'css-m1xr13 e10bexpg7'})
        partners = div.find_all('a', recursive=False)

        for partner in partners:

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = self.partner_program_name

            data_div = partner.find('div')
            item['partner_company_name'] = data_div.find('h3').text.strip()
            item['company_description'] = cleanhtml(data_div.find('div', {'class': 'css-1rqa14h e10bexpg2'}).text.strip())

            item['company_domain_name'] = partner['href']
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

            item['partner_type'] = data_div.find('p', {'class': 'css-15irymz e10bexpg3'}).text.strip()
            item['regions'] = data_div.find('p', {'class': 'css-1on15lp e10bexpg4'}).text.strip()

            yield item
