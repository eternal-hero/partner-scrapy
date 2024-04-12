# import needed libraries
import json
import re
from techpartners.spiders.base_spider import BaseSpider
import scrapy
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'acronispartner'
    partner_program_link = 'https://www.acronis.com/en-us/partners/locator/'
    partner_directory = 'Acronis Partner Directory'
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
                   'primary_contact_name', 'primary_contact_phone_number',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    all_countries = dict()

    def start_requests(self):
        r = requests.get('https://websiteapi.acronis.com/svc/v1/partners-portal/locator-legacy/countries?section=partners',     headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36'})
        if r.status_code == 200:
            if 'success' in r.text:
                for i in json.loads(r.text)['data']:
                    self.all_countries[i['code']] = i['name']
        else:
            self.logger.info('ERROR: GET COUNTRIES DATA')

        for country_code, country_name in self.all_countries.items():
            link = f'https://websiteapi.acronis.com/svc/v1/partners-portal/locator-legacy/partners?country={country_code}'
            yield scrapy.Request(method='GET', url=link, callback=self.parse, meta={'country_code': country_code, 'country_name': country_name})

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
            return

        country_code = response.meta['country_code']
        country_name = response.meta['country_name']

        data = json.loads(response.text)
        if country_code in data and 'Partners' in data[country_code]:
            for profile in data[country_code]['Partners']:

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item['partner_company_name'] = profile['name']
                item['partner_type'] = profile['level'] if 'level' in profile else ''
                item['partner_tier'] = profile['cloudlevel'] if 'cloudlevel' in profile else ''

                item['company_domain_name'] = profile['website'] if 'website' in profile and profile['website'] else ''
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

                item['general_phone_number'] = profile['phone'] if 'phone' in profile else ''

                item['headquarters_street'] = profile['street'] if 'street' in profile else ''
                item['headquarters_city'] = profile['city'] if 'city' in profile else ''
                item['headquarters_state'] = profile['state'] if 'state' in profile else ''
                item['headquarters_zipcode'] = profile['zip code'] if 'zip code' in profile else ''
                item['headquarters_country'] = profile['country'] if 'country' in profile else ''

                if item['headquarters_country'] != '' and item['headquarters_country'] in self.all_countries:
                    item['headquarters_country'] = self.all_countries[item['headquarters_country']]

                yield item
