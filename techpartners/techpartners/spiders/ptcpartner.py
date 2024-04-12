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
    name = 'ptcpartner'
    partner_program_link = 'https://www.ptc.com/en/partners/partner-search'
    partner_directory = 'PTC Partners Directory'
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
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/json;charset=UTF-8',
        'Authority': 'www.ptc.com',
        'Origin': 'https://www.ptc.com',
        'Referer': 'https://www.ptc.com/en/partners/partner-search',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'Connection': 'keep-alive',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }
    api_link = 'https://www.ptc.com/PTCArea/LandingPage/NewPartnerSearch'

    def start_requests(self):
        page = 1
        count = 12
        data = '{"page":%d,"query":"","num":%d,"facets":[]}' % (page, count)
        yield scrapy.Request(method='POST', url=self.api_link, body=data,
                             callback=self.parse, headers=self.headers,
                             dont_filter=True,
                             meta={'page': page, 'count': count})

    def parse(self, response):
        page = response.meta['page']
        count = response.meta['count']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            try:
                j = json.loads(response.text)
            except:
                j = None

            if j and 'Partners' in j:
                partners = j['Partners']
                self.logger.info(f'Page: {page}, Partners: {len(partners)}')
                for partner in partners:
                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = ''

                    item['partner_company_name'] = partner['AccountName']
                    item['partner_type'] = partner['PartnerType'] if 'PartnerType' in partner and partner['PartnerType'] else ''
                    item['company_description'] = cleanhtml(partner['PartnerDescription']) if 'PartnerDescription' in partner and partner['PartnerDescription'] else ''
                    item['solutions'] = partner['Segment'] if 'Segment' in partner and partner['Segment'] else ''
                    item['certifications'] = [certificate.replace('/-/media/Partners/', '').replace('.svg', '').strip() for certificate in partner['PartnerBadge']] if 'PartnerBadge' in partner and partner['PartnerBadge'] else ''

                    item['company_domain_name'] = partner['Website'] if 'Website' in partner and partner['Website'] else ''
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

                    locations = partner['Locations'] if 'Locations' in partner and partner['Locations'] else None
                    if locations and len(locations) > 0:
                        for location in locations:
                            item['locations_country'] = location['Country'] if 'Country' in location and location['Country'] else ''
                            item['locations_state'] = location['SubType'] if 'SubType' in location and location['SubType'] else ''
                            yield item
                    else:
                        yield item

                # follow next pages
                if page == 1 and len(partners) == count:
                    total = j['TotalNumberOfResults']
                    for page in range(2, math.ceil(total/count)+1):
                        data = '{"page":%d,"query":"","num":%d,"facets":[]}' % (page, count)
                        yield scrapy.Request(method='POST', url=self.api_link, body=data,
                                             callback=self.parse, headers=self.headers,
                                             dont_filter=True,
                                             meta={'page': page, 'count': count})
