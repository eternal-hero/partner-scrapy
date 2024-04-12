# import needed libraries
import json
import re
import math
from techpartners.spiders.base_spider import BaseSpider
import scrapy
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse
from scrapy.http import HtmlResponse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'ciscopartner'
    partner_program_link = 'https://locatr.cloudapps.cisco.com/WWChannels/LOCATR/pf/index.jsp#/'
    partner_directory = 'Cisco Partner Locator'
    partner_program_name = ''
    crawl_id = None

    api_link = 'https://locatr.cloudapps.cisco.com/WWChannels/LOCATR/service/api/v1/getPfPartners'

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name', 'product_service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'product_service_description',
                   'headquarters_street', 'headquarters_city', 'headquarters_state', 'headquarters_zipcode', 'headquarters_country',
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

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36',
               'Accept': '*/*',
               'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
               'Accept-Encoding': 'gzip, deflate, br',
               'Content-Type': 'application/json',
               'Referer': 'https://locatr.cloudapps.cisco.com/WWChannels/LOCATR/pf/index.jsp',
               'Origin': 'https://locatr.cloudapps.cisco.com',
               'authority': 'locatr.cloudapps.cisco.com',
               'Connection': 'keep-alive',
               'sec-ch-ua': '"Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
               'sec-ch-ua-platform': 'Windows',
               'Sec-Fetch-Dest': 'empty',
               'Sec-Fetch-Mode': 'cors',
               'Sec-Fetch-Site': 'same-origin',
               }

    def start_requests(self):
        pageCount = 1
        pageSize = 100

        data = {"LOCATION_IDS":"","LEVEL_IDS":"","SPECIALIZATION_IDS":"","PARTNER_KEYWORD_IDS":"","pageSize":pageSize,"pageCount":pageCount,"SORT_BY":"badge_rank","SORT_ORDER":"ASC"}
        yield scrapy.Request(method='POST', url=self.api_link, body=json.dumps(data), callback=self.parse,
                             dont_filter=True,
                             headers=self.headers, meta={'pageCount': pageCount, 'pageSize': pageSize})

    def parse(self, response):

        pageCount = response.meta['pageCount']
        pageSize = response.meta['pageSize']

        if response.status == 200:
            data = response.json()
            if 'matches' in data:
                lastPage = math.ceil(data['matches'] / pageSize)

                partners = data['data']
                self.logger.info(f"Page: {pageCount}, Result: {len(partners)}, Totalpages: {lastPage}")

                for partner in partners:
                    partner_id = partner['site_id']
                    data = {"SITE_ID":str(partner_id),"pageSize":1,"pageCount":1}
                    yield scrapy.Request(method='POST', url=self.api_link, body=json.dumps(data), dont_filter=True,
                                         callback=self.parse_partner, headers=self.headers, meta={'data': data})

                # follow next pages
                if pageCount == 1:
                    for i in range(2, lastPage+1):
                        data = {"LOCATION_IDS": "", "LEVEL_IDS": "", "SPECIALIZATION_IDS": "",
                                "PARTNER_KEYWORD_IDS": "", "pageSize": pageSize, "pageCount": i,
                                "SORT_BY": "badge_rank", "SORT_ORDER": "ASC"}
                        yield scrapy.Request(method='POST', url=self.api_link, body=json.dumps(data),
                                             callback=self.parse, headers=self.headers, dont_filter=True,
                                             meta={'pageCount': i, 'pageSize': pageSize})
        else:
            self.logger.info(f'ERROR PAGE COUNT: {pageCount}, STATUS: {response.status}')

            data = {"LOCATION_IDS": "", "LEVEL_IDS": "", "SPECIALIZATION_IDS": "",
                    "PARTNER_KEYWORD_IDS": "", "pageSize": pageSize, "pageCount": pageCount,
                    "SORT_BY": "badge_rank", "SORT_ORDER": "ASC"}

            yield scrapy.Request(method='POST', url=response.request.url, body=json.dumps(data),
                                 callback=self.parse, headers=self.headers, dont_filter=True,
                                 meta=response.meta)

    def parse_partner(self, response):
        if response.status == 200:

            data = response.json()
            if 'data' not in data or len(data['data']) == 0:
                data = response.meta['data']
                yield scrapy.Request(method='POST', url=self.api_link, body=json.dumps(data),
                                     callback=self.parse_partner,
                                     headers=self.headers, meta=response.meta, dont_filter=True)
                return

            partner = data['data'][0]

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = self.partner_program_name

            item['partner_company_name'] = partner['partner_name'] if 'partner_name' in partner else ''
            item['company_domain_name'] = partner['web_addr'] if ('web_addr' in partner and partner['web_addr']) else ''
            url_obj = urllib.parse.urlparse(item['company_domain_name'])
            item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
            x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
            if x:
                item['company_domain_name'] = x[-1]
            if '/' in item['company_domain_name']:
                item['company_domain_name'] = item['company_domain_name'][:item['company_domain_name'].find('/')]

            item['company_description'] = partner['note_text'] if 'note_text' in partner else ''
            item['headquarters_street'] = partner['site_addr_1'] if 'site_addr_1' in partner else ''
            item['headquarters_city'] = partner['site_city'] if 'site_city' in partner else ''
            item['headquarters_state'] = partner['site_state'] if 'site_state' in partner else ''
            item['headquarters_zipcode'] = partner['site_zip'] if 'site_zip' in partner else ''
            item['headquarters_country'] = partner['site_country'] if 'site_country' in partner else ''
            item['general_phone_number'] = partner['site_phone'] if 'site_phone' in partner else ''

            item['company_size'] = partner['companysize_keywords'] if 'companysize_keywords' in partner else ''

            item['industries'] = partner['industry_keywords'] if 'industry_keywords' in partner else ''
            item['categories'] = partner['technology_keywords'] if 'technology_keywords' in partner else ''

            if 'qualifications' in partner:
                item['specializations'] = list()
                for q in partner['qualifications']:
                    if q['category_display_name'] == "Partner Specializations" :
                        item['specializations'].append(q['keyword_display_name'])

                item['integration_level'] = list()
                for q in partner['qualifications']:
                    if q['category_display_name'] == "Integrator Level" :
                        item['integration_level'].append(q['keyword_display_name'])

                item['designations'] = list()
                for q in partner['qualifications']:
                    if q['category_display_name'] == "Partner Designations" :
                        item['designations'].append(q['keyword_display_name'])

                item['services'] = list()
                for q in partner['qualifications']:
                    if q['category_display_name'] == "Provider Level and Cisco Powered Services" :
                        level = q['keyword_display_name']
                        if level.endswith('Integrator') or level.endswith('Provider') or level.endswith('Developer') or level.endswith('Advisor'):
                            item['partner_tier'] = q['keyword_display_name']
                        else:
                            item['services'].append(q['keyword_display_name'])

            yield item

        else:
            data = response.meta['data']
            yield scrapy.Request(method='POST', url=self.api_link, body=json.dumps(data), callback=self.parse_partner,
                                 headers=self.headers, meta=response.meta, dont_filter=True)
