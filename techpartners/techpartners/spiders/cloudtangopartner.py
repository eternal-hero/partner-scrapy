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
    name = 'cloudtangopartner'
    partner_program_link = 'https://www.cloudtango.net'
    partner_directory = 'Cloudtango Partner'
    partner_program_name = ''
    crawl_id = None

    custom_settings = {
        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_IP': 1,
        'DOWNLOADER_MIDDLEWARES': {
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620},
        'ROTATING_PROXY_LIST_PATH': 'proxy_25000.txt',
        'ROTATING_PROXY_PAGE_RETRY_TIMES': 10,
    }

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
                   'primary_contact_name', 'primary_contact_phone_number', 'primary_contact_email',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'Connection': 'keep-alive',
        'Authority': 'www.cloudtango.net',
        'Accept': 'text/html, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.cloudtango.net/',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'x-requested-with': 'XMLHttpRequest',
        }

    def start_requests(self):
        page_number = 1
        link = self.partner_program_link + f'/list/?page={page_number}'
        yield scrapy.Request(method='GET', url=link, callback=self.parse,
                             headers=self.headers,
                             meta={'page_number': page_number})

    def parse(self, response):
        page_number = response.meta['page_number']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
        else:
            soup = BS(response.text, "html.parser")
            tbl = soup.find('tbody').find_all('tr')
            self.logger.info(f"Page Number = {page_number}, Number of results = {len(tbl)}")

            for tr in tbl:
                lbl = tr.find('td', {'class': 'company'}).find('em') if tr.find('td', {'class': 'company'}) else None
                if lbl:
                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = self.partner_program_name

                    item['partner_company_name'] = lbl.text

                    partner_ref = tr.find('td', {'class': 'company'}).find('a', {'href': True})
                    if partner_ref:
                        partner_link = self.partner_program_link + partner_ref['href']
                        yield scrapy.Request(method='GET', url=partner_link, headers=self.headers,
                                             callback=self.parse_partner,
                                             meta={'item': item})
                    else:
                        yield item

            # follow next pages
            more = soup.find('a', {'class': 'next-selector'})
            if page_number == 1 or (more and more.text == 'Load more'):
                page_number += 1
                link = self.partner_program_link + f'/list/?page={page_number}'
                yield scrapy.Request(link, callback=self.parse,
                                     headers=self.headers,
                                     meta={'page_number': page_number})

    def parse_partner(self, response):
        item = response.meta['item']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST PARTNER, COMPANY: {item["partner_company_name"]}, URL: {response.request.url}')
        else:
            soup = BS(response.text, "html.parser")

            item['company_description'] = cleanhtml(soup.find('span', {'class': 'description'}).text if soup.find('span', {'class': 'description'}) else '')

            item['company_domain_name'] = get_domain_from_url(soup.find('a', {'itemprop': 'url', 'href': True}).text if soup.find('a', {'itemprop': 'url', 'href': True}) else '')
            # try:
            #     url_obj = urllib.parse.urlparse(item['company_domain_name'])
            #     item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
            #     x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
            #     if x:
            #         item['company_domain_name'] = x[-1]
            #     if '/' in item['company_domain_name']:
            #         item['company_domain_name'] = item['company_domain_name'][:item['company_domain_name'].find('/')]
            # except Exception as e:
            #     print('DOMAIN ERROR: ', e)

            item['general_phone_number'] = soup.find('a', {'itemprop': 'telephone', 'href': True}).text if soup.find('a', {'itemprop': 'telephone', 'href': True}) else ''
            address = soup.find('span', {'itemprop': 'addressLocality'}).text if soup.find('span', {'itemprop': 'addressLocality'}) else ''
            if address and address != '':
                if ',' in address:
                    item['headquarters_city'] = address[:address.find(',')].strip()
                    item['headquarters_country'] = address[address.find(',')+1:].strip()
                else:
                    item['headquarters_address'] = address

            dls = soup.find_all('dl')
            for dl in dls:
                if dl.find('dt') and 'Services:' in dl.find('dt').text:
                    item['services'] = dl.find('dd').text
                elif dl.find('dt') and 'Partnerships:' in dl.find('dt').text:
                    item['partner_clients'] = cleanhtml(' '.join(dl.find('dd').find_all(text=True, recursive=False)))

            yield item
