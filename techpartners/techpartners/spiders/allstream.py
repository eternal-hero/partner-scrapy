# import needed libraries
import json
import re
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from techpartners.spiders.base_spider import BaseSpider
import scrapy
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse
from scrapy.http import HtmlResponse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'allstreampartner'
    site_name = 'AllStream Partner Directory'
    page_link = 'https://allstream.com/partners/alliance-partner-directory/'
    start_urls = ['https://allstream.com/wp-json/wpgmza/v1/features/base64eJyrVkrLzClJLVKyUqqOUcpNLIjPTIlRsopRMopR0gEJFGeUFni6FAPFomOBAsmlxSX5uW6ZqTkpYDGgpjQQB6bNEKytLDGnNBXIzyvNyanVQVdjRIQaYxQ1MUoxSrWxtUq1AO4dOaY']

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

    def get_location(self, lat, long):
        geolocator = Nominatim(user_agent="MM_geoapi")
        location = geolocator.reverse([lat, long], language='en')
        address = location.raw['address']
        return address

    def parse(self, response):
        if response.status == 200:
            try:
                data = json.loads(response.text)
            except Exception as e:
                self.logger.info('ERROR: ', e)
                return
            if 'markers' in data:
                partners = data['markers']
                for partner in partners:

                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = 'https://allstream.com/partners/alliance-partner-directory/'
                    item['partner_directory'] = 'Allstream Partner'
                    item['partner_company_name'] = partner['title']
                    item['company_description'] = cleanhtml(partner['description'] if 'description' in partner else '')

                    # item['company_domain_name'] = partner['link'] if 'link' in partner else ''
                    url_obj = urllib.parse.urlparse(partner['link'] if 'link' in partner else '')
                    item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                    x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                    if x:
                        item['company_domain_name'] = x[-1]

                    loc = self.get_location(partner['lat'], partner['lng'])
                    item['headquarters_country'] = loc.get('country', '')
                    # item['headquarters_state'] = loc.get('state', '')
                    # item['headquarters_city'] = loc.get('city', '')
                    if 'custom_field_data' in partner:
                        for n in partner['custom_field_data']:
                            if "name" in n and "value" in n and n['name'] == "Contact Name":
                                item['primary_contact_name'] = n['value'].replace("Contact name:", '').strip()
                            if "name" in n and "value" in n and n['name'] == "Contact Number":
                                item['primary_contact_phone_number'] = n['value'].replace("Contact phone number:", '').strip()
                    # try:
                    #     x = re.search(r'\s*\d{5}\s*', partner['address'])
                    #     if x:
                    #         item['headquarters_zipcode'] = x.group()
                    # except:
                    #     pass
                    # item['headquarters_address'] = (partner['address'] if 'address' in partner else '').replace(item['headquarters_zipcode'], '').strip()
                    item['headquarters_address'] = partner['address'] if 'address' in partner else ''

                    yield item
