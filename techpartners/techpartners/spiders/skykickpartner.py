# import needed libraries
import json
import re
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'skykickpartner'
    partner_program_link = 'https://www.skykick.com/platform/distributors/'
    partner_directory = 'Skykick Partner Directory'
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
        table = soup.find('table', {'class': 'tbl-distributor'})
        if table:
            partners = table.find('tbody').find_all('tr')
            for partner in partners:
                cells = partner.find_all('td')
                if 'Register' not in partner.text:
                    continue

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item['partner_company_name'] = partner.find('a', {'href': True}).text if cells[0].find('a', {'href': True}) else ''
                item['company_domain_name'] = partner.find('a', {'href': True})['href'] if cells[0].find('a', {'href': True}) else ''
                try:
                    url_obj = urllib.parse.urlparse(item['company_domain_name'])
                    item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                    x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                    if x:
                        item['company_domain_name'] = x[-1]
                    if '/' in item['company_domain_name']:
                        item['company_domain_name'] = item['company_domain_name'][
                                                      :item['company_domain_name'].find('/')]
                except Exception as e:
                    print('DOMAIN ERROR: ', e)

                locations_countries = [country.strip() for country in cells[1].text.split(',')]
                for loc in locations_countries:
                    item['locations_country'] = loc
                    yield item
