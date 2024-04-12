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
    name = 'checkpointpartner2'
    partner_program_link = 'https://partnerlocator.checkpoint.com/#/'
    partner_directory = 'Checkpoint Directory Partners'
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

    def start_requests(self):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
                   'Connection': 'keep-alive',
                   'Host': 'iapi-services-ucs.checkpoint.com',
                   'Origin': 'https://partnerlocator.checkpoint.com',
                   'Referer': 'https://partnerlocator.checkpoint.com/',
                   }
        r = requests.get(url='https://iapi-services-ucs.checkpoint.com/public/api/partner-locator-mms/api/partnerLocatorFilterOptions',
                         headers=headers)
        try:
            jdata = json.loads(r.text)
            countries = jdata['country'].keys()
        except:
            countries = list()

        api_link = 'https://iapi-services-ucs.checkpoint.com/public/api/partner-locator-mms/api/partnerLocatorInfo'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
            'Connection': 'keep-alive',
            'Host': 'iapi-services-ucs.checkpoint.com',
            'Origin': 'https://partnerlocator.checkpoint.com',
            'Referer': 'https://partnerlocator.checkpoint.com/',
            'Referrer Policy': 'strict-origin-when-cross-origin',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': 'Windows',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'x-requested-with': 'XMLHttpRequest',
            'upgrade-insecure-requests': 1,
        }
        for country in countries:
            payload = [{"fieldName": "country", "data": {"value": country}}]
            yield scrapy.Request(method='POST', url=api_link, body=json.dumps(payload),
                                 callback=self.parse, headers=headers,
                                 meta={'country': country})

    def parse(self, response):
        country = response.meta['country']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:

            try:
                jdata = json.loads(response.text)
            except:
                jdata = None

            if jdata and 'data' in jdata and 'QCEntities' in jdata['data'] and len(jdata['data']['QCEntities']) > 0:
                partners = jdata['data']['QCEntities']

                self.logger.info(f"Country: {country}, Number of results = {len(partners)}")

                for partner in partners:
                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = self.partner_program_name

                    item['partner_company_name'] = partner['name'] if 'name' in partner and partner['name'] else ''
                    item['partner_type'] = partner['partnerType'] if 'partnerType' in partner and partner['partnerType'] else ''
                    item['partner_tier'] = partner['level'] if 'level' in partner and partner['level'] else ''

                    item['company_size'] = partner['companySize'] if 'companySize' in partner and partner['companySize'] else ''

                    item['headquarters_country'] = partner['country'] if 'country' in partner and partner['country'] else ''
                    item['headquarters_state'] = partner['state'] if 'state' in partner and partner['state'] else ''
                    item['headquarters_city'] = partner['city'] if 'city' in partner and partner['city'] else ''
                    item['headquarters_street'] = partner['street'] if 'street' in partner and partner['street'] else ''
                    item['headquarters_street'] += (' ' + partner['street2']) if 'street2' in partner and partner['street2'] and partner['street2'] != '' and ('street1' in partner and partner['street2'] != partner['street1']) else ''
                    item['headquarters_zipcode'] = partner['zipCode'] if 'zipCode' in partner and partner['zipCode'] else ''

                    item['company_description'] = cleanhtml(partner['companyOverview']) if 'companyOverview' in partner and partner['companyOverview'] else ''

                    item['company_domain_name'] = partner['webSite'] if 'webSite' in partner and partner['webSite'] else ''
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

                    item['general_phone_number'] = partner['telephone'] if 'telephone' in partner and partner['telephone'] else ''
                    item['general_email_address'] = partner['email'] if 'email' in partner and partner['email'] else ''

                    item['specializations'] = partner['specialization'] if 'specialization' in partner and partner['specialization'] else ''
                    item['services'] = partner['purchasingProgram'] if 'purchasingProgram' in partner and partner['purchasingProgram'] else ''

                    locations = partner['offices'] if 'offices' in partner and partner['offices'] else list()
                    if len(locations) > 0:
                        for loc in locations:
                            location = loc.split('~')
                            item['locations_country'] = location[2]
                            item['locations_state'] = location[4]
                            item['locations_city'] = location[9]
                            item['locations_street'] = location[5]
                            item['locations_zipcode'] = location[8]
                            yield item
                    else:
                        yield item
