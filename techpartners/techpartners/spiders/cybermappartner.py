# import needed libraries
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
    name = 'cybermappartner'
    partner_program_link = 'https://www.cybermap.co/'
    partner_directory = 'CyberMap Partners'
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

    start_urls = ['https://www.cybermap.co/static/js/main.51b7bde4.chunk.js']

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            raw = response.text
            lst = raw.split('function(')
            for txt in lst:
                if 'JSON.parse' in txt:
                    try:
                        data = txt[txt.find("JSON.parse('")+len("JSON.parse('"):].strip(',')
                        data = data[:data.rfind("'")].replace('\\xb0', '').replace("\\'", "'").replace('\\"', '"')
                        jdata = json.loads(data)
                        partners = jdata['companies']
                        break
                    except Exception as e:
                        print('JSON ERROR: ', e)
            else:
                partners = list()

            for partner in partners:
                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item['partner_company_name'] = partner['name'] if 'name' in partner and partner['name'] else ''
                item['company_description'] = cleanhtml(partner['description']) if 'description' in partner and partner['description'] else ''

                item['company_domain_name'] = partner['homepage'] if 'homepage' in partner and partner['homepage'] else ''
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

                item['twitter_link'] = partner['twitter'] if 'twitter' in partner and partner['twitter'] else ''
                item['linkedin_link'] = partner['linkedin'] if 'linkedin' in partner and partner['linkedin'] else ''
                item['categories'] = partner['category'] if 'category' in partner and partner['category'] else ''

                item['funding'] = partner['total_funding'] if 'total_funding' in partner and partner['total_funding'] else ''
                item['crunchbase'] = partner['crunchbase'] if 'crunchbase' in partner and partner['crunchbase'] else ''

                yield item
