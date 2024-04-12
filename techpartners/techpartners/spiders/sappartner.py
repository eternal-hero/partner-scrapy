# import needed libraries
import json
import random
import re
import time

import requests
from urllib3 import HTTPSConnectionPool

from techpartners.spiders.base_spider import BaseSpider
import scrapy
import urllib.parse
from techpartners.functions import *
with open('proxy_25000.txt', 'r') as ifile:
    Ips = [x.strip() for x in ifile.readlines()]


def get_proxy():
    proxy_index = random.randint(0, len(Ips) - 1)
    return {"http": "http://" + Ips[proxy_index], "https": "http://" + Ips[proxy_index]}


headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"}


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'sappartner'
    partner_program_link = 'https://pf-prod-sapit-partner-prod.cfapps.eu10.hana.ondemand.com/partnerNavigator'
    partner_directory = 'SAP Partners'
    partner_program_name = ''
    crawl_id = 1256

    custom_settings = {
        # 'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS': 16,
        'CONCURRENT_REQUESTS_PER_IP': 1,
        'DOWNLOADER_MIDDLEWARES': {
        'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
        'rotating_proxies.middlewares.BanDetectionMiddleware': 620},
        'ROTATING_PROXY_LIST_PATH': 'proxy_25000.txt',
        'ROTATING_PROXY_PAGE_RETRY_TIMES': 10,
    }

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name', 'product/service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'product/service_description',
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
                   'primary_contact_name', 'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified?',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    def start_requests(self):
        """
        landing function of spider class
        will create the partners list from the search page and pass it to parse function
        :return:
        """

        base_url = "https://pf-prod-sapit-partner-prod.cfapps.eu10.hana.ondemand.com/sap/search/api/search/results?q=&pageSize=12&sort=TITLE_STR:asc&pageNumber="
        partner_url = "https://pf-prod-sapit-partner-prod.cfapps.eu10.hana.ondemand.com/sap/details/api/partnerProfile/findByPartnerProfileId/"

        pageNumber = 0
        # iterate through search pages
        while True:
            try:
                response = requests.get(base_url + str(pageNumber), headers=headers, proxies=get_proxy())
            except HTTPSConnectionPool:
                self.logger.info(f'ERROR HTTPSConnectionPool EXCEPTION: {base_url + str(pageNumber)}')
                continue
            except Exception as e:
                self.logger.info('ERROR EXCEPTION: ', e)
                continue

            if response.status_code == 200 and '"error"' not in response.text:
                DATA = json.loads(response.text)
                # get search results items
                Data = DATA['results']
                self.logger.info(f'REQUEST PAGE {base_url + str(pageNumber)}, RESULTS = {len(Data)}')
                # iterate through search results and create / add partner page link
                for row in Data:
                    yield scrapy.Request(partner_url + row['profileId'], callback=self.parse,
                                         meta={'Partner_id': row['profileId'],
                                               'title': row['title'],
                                               'description': row['description']})
                if len(Data) >= 12:
                    pageNumber += 1
                else:
                    break
            else:
                # self.crawler.engine.pause()
                # print('Engine Paused')
                # time.sleep(60)
                # self.crawler.engine.unpause()
                continue

    def parse(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """

        if response.status != 200 or '"error"' in response.text:
            self.logger.info(f'ERROR: PARSE STATUS {response.status}, {response.text}')
            # self.crawler.engine.pause()
            # print('Engine Paused')
            # time.sleep(60)
            # self.crawler.engine.unpause()
            # yield scrapy.Request(response.request.url, callback=self.parse, dont_filter=True,
            #                      meta=response.meta)
        else:
            yield_check = False

            data = json.loads(response.text)

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = self.partner_program_name

            partner_id = response.meta['Partner_id']
            # item['partner_company_name'] = data['name']
            item['partner_company_name'] = data['name'].replace('«', '').replace('»', '').strip()
            item['company_description'] = cleanhtml(data['valuePreposition'])

            item['company_domain_name'] = get_domain_from_url(data['webSiteUrl'].strip()) if 'webSiteUrl' in data else ''

            item['partner_tier'] = data['level'] if 'level' in data else ''
            item['general_email_address'] = data['contactEmail'] if 'contactEmail' in data else ''
            item['services'] = [item['title'] for item in data['businessModels']] if 'businessModels' in data else []
            item['industries'] = [item['title'] for item in data['industries']] if 'industries' in data else []
            item['solutions'] = [item['solutionName'] for item in data['solutions']] if 'solutions' in data else []
            item['competencies'] = data['competencies'] if 'competencies' in data else []
            item['languages'] = [item['text'] for item in data['languages']] if 'languages' in data else []

            if 'profileSocialMedias' in data and len(data['profileSocialMedias']) > 0:
                for prof in data['profileSocialMedias']:
                    if prof['socialMediaType'] == 'LINKEDIN':
                        item['linkedin_link'] = prof['link']
                    elif prof['socialMediaType'] == 'FACEBOOK':
                        item['facebook_link'] = prof['link']
                    elif prof['socialMediaType'] == 'TWITTER':
                        item['twitter_link'] = prof['link']
                    elif prof['socialMediaType'] == 'INSTAGRAM':
                        item['instagram_link'] = prof['link']
                    elif prof['socialMediaType'] == 'YOUTUBE':
                        item['youtube_link'] = prof['link']
                    elif prof['socialMediaType'] == 'XING':
                        item['xing_link'] = prof['link']
                    elif prof['socialMediaType'] == 'WECHAT':
                        # item[''] = prof['link']
                        continue
                    elif prof['socialMediaType'] == 'WEBSITE':
                        if item['company_domain_name'] == '':
                            item['company_domain_name'] = get_domain_from_url(prof['link'].strip()) if 'link' in prof and prof['link'] else ''
                    else:
                        self.logger.info('New socialMediaType: ' + prof['socialMediaType'])

            if 'hqAddress' in data and data['hqAddress'] is not None:
                if item['general_phone_number'] is None or item['general_phone_number'] == '':
                    item['general_phone_number'] = data['hqAddress']['phone'] if 'phone' in data['hqAddress'] else ''

                if item['general_email_address'] is None or item['general_email_address'] == '':
                    item['general_email_address'] = data['hqAddress']['email'] if 'email' in data['hqAddress'] else ''

                item['headquarters_street'] = data['hqAddress']['addressName'] if 'addressName' in data['hqAddress'] else ''
                item['headquarters_city'] = data['hqAddress']['city'] if 'city' in data['hqAddress'] else ''
                item['headquarters_state'] = data['hqAddress']['regionText'] if 'regionText' in data['hqAddress'] else ''
                item['headquarters_country'] = data['hqAddress']['countryCodeText'] if 'countryCodeText' in data['hqAddress'] else ''
                x = re.search(r'\d{5}', data['hqAddress']['fullAddress']) if 'fullAddress' in data['hqAddress'] and data['hqAddress']['fullAddress'] else None
                item['headquarters_zipcode'] = x.group() if x else ''

            if 'localProfiles' in data and len(data['localProfiles']) > 0:
                for region in data['localProfiles']:
                    if 'addresses' in region and region['addresses'] and len(region['addresses']) > 0:
                        for location in region['addresses']:
                            if item['general_email_address'] is None or item['general_email_address'] == '':
                                item['general_email_address'] = location['email'] if 'email' in location else ''
                            item['general_phone_number'] = location['phone'] if 'phone' in location else ''
                            item['locations_street'] = location['addressName'] if 'addressName' in location else ''
                            item['locations_city'] = location['city'] if 'city' in location else ''
                            item['locations_state'] = location['regionText'] if 'regionText' in location else ''
                            item['locations_country'] = location['countryCodeText'] if 'countryCodeText' in location else ''
                            x = re.search(r'\d{5}', location['fullAddress']) if 'fullAddress' in location and location['fullAddress'] else None
                            item['locations_zipcode'] = x.group() if x else ''

                            yield item
                            yield_check = True

            if not yield_check:
                yield item
