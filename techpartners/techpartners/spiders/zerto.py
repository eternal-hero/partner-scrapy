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
    name = 'zertopartner'
    site_name = 'Zerto Partner Directory'
    page_link = 'https://www.zerto.com/partners/find-a-partner/'
    start_urls = ['https://www.zerto.com/partners/find-a-partner/']

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
               'Authority': 'www.zerto.com',
               'Accept': '*/*',
               'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
               'Accept-Encoding': 'gzip, deflate, br',
               'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
               'Referer': 'https://www.zerto.com/partners/find-a-partner/',
               'Origin': 'https://www.zerto.com',
               'Connection': 'keep-alive',
               'Sec-Fetch-Dest': 'empty',
               'Sec-Fetch-Mode': 'cors',
               'Sec-Fetch-Site': 'same-origin',
               'X-Requested-With': 'XMLHttpRequest',
               }

    def parse(self, response):
        if response.status == 200:
            cats = response.xpath('//select[@id = "partner-type-select"]//option/@value').getall()
            for cat in cats[2:]:
                offset = 0
                data = f'nonce=bd826a2366&action=get_partners&offset={offset}&partner_type={cat}&partner_area=&partner_search='
                yield scrapy.Request(method='POST', url='https://www.zerto.com/wp-admin/admin-ajax.php',
                                     body=data, callback=self.parse_category, headers=self.headers,
                                     meta={'cat': cat, 'offset': offset})

    def parse_category(self, response):
        cat = response.meta['cat']
        offset = response.meta['offset']

        if response.status != 200:
            print(response.status)
            return

        # try:
        if True:
            data = json.loads(response.text)
            if 'data' in data and len(data['data']) > 0:
                for partner in data['data']:
                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = 'https://www.zerto.com/partners/find-a-partner/?type=&area=&txt='
                    item['partner_directory'] = 'Zerto Partner'
                    item['partner_company_name'] = partner['the_title']
                    item['partner_type'] = cat.replace('-', ' ').capitalize().strip()
                    item['partner_tier'] = partner['tier_level']['name'] if 'tier_level' in partner and partner['tier_level'] and 'name' in partner['tier_level'] else ''
                    item['company_description'] = partner['the_excerpt'] if 'the_excerpt' in partner else ''
                    item['company_domain_name'] = partner['partner_website'] if ('partner_website' in partner and partner['partner_website']) else ''
                    try:
                        url_obj = urllib.parse.urlparse(item['company_domain_name'])
                        item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                        x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                        if x:
                            item['company_domain_name'] = x[-1]
                    except Exception as e:
                        print('DOMAIN ERROR: ', e)

                    item['regions'] = partner['partner_location'] if 'partner_location' in partner else ''
                    item['general_phone_number'] = partner['partner_contact'] if 'partner_contact' in partner and '@' not in partner['partner_contact'] and 'https' not in partner['partner_contact'] else ''
                    item['general_email_address'] = partner['partner_contact'] if 'partner_contact' in partner and '@' in partner['partner_contact'] else ''
                    yield item

            # follow next page
            if len(data['data']) == 12:
                offset += 12
                data = f'nonce=bd826a2366&action=get_partners&offset={offset}&partner_type={cat}&partner_area=&partner_search='
                yield scrapy.Request(method='POST', url='https://www.zerto.com/wp-admin/admin-ajax.php',
                                     body=data, callback=self.parse_category, headers=self.headers,
                                     meta={'cat': cat, 'offset': offset}, dont_filter=True)

        # except Exception as e:
        #     print(e)
