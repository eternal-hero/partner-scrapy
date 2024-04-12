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
    name = 'entrustpartner'
    partner_program_link = 'https://www.entrust.com/partner-directory'
    partner_directory = 'Entrust Partner'
    partner_program_name = ''
    crawl_id = None

    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 4
    }

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name',
                   'product_service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description',
                   'product_service_description',
                   'headquarters_address',
                   'headquarters_street', 'headquarters_city', 'headquarters_state', 'headquarters_zipcode',
                   'headquarters_country',
                   'locations_address',
                   'locations_street', 'locations_city', 'locations_state', 'locations_zipcode', 'locations_country',
                   'regions', 'languages', 'products', 'services', 'solutions',
                   'pricing_plan', 'pricing_model', 'pricing_plan_description',
                   'pricing', 'specializations', 'categories',
                   'features', 'account_requirements', 'product_package_name', 'year_founded', 'latest_update',
                   'publisher',
                   'partnership_timespan', 'partnership_founding_date', 'product_version', 'product_requirements',
                   'general_phone_number', 'general_email_address',
                   'support_phone_number', 'support_email_address', 'support_link', 'help_link', 'terms_and_conditions',
                   'license_agreement_link', 'privacy_policy_link',
                   'linkedin_link', 'twitter_link', 'facebook_link', 'youtube_link', 'instagram_link', 'xing_link',
                   'primary_contact_name', 'primary_contact_phone_number', 'primary_contact_email',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts',
                   'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'cookie': 'izcid=1668320723846; _cs_mk_ga=0.36480896504378957_1668320625771; utm_source=direct; utm_medium=direct; _gid=GA1.2.1027566689.1668320626; _cs_c=0; ln_or=d; OptanonAlertBoxClosed=2022-11-13T06:23:50.394Z; fabBubble=closed; nmstat=c12db61d-1f01-4506-c4c0-930e17e95267; OptanonConsent=isGpcEnabled=0&datestamp=Sun+Nov+13+2022+08%3A24%3A31+GMT%2B0200+(Eastern+European+Standard+Time)&version=6.33.0&isIABGlobal=false&hosts=&consentId=5242493d-2490-443b-87b7-adc67dd6b87e&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0004%3A1%2CC0003%3A1%2CC0002%3A1&geolocation=EG%3BC&AwaitingReconsent=false; iztid=1668320765837; _ga_6QRW66BW5T=GS1.1.1668320626.1.1.1668320672.0.0.0; _ga=GA1.1.679117041.1668320626; _cs_id=b9ced215-0606-a762-cfbd-6f499ff4509f.1668320626.1.1668320672.1668320626.1.1702484626343; _cs_s=2.5.0.1668322472222',
        'Authority': 'www.entrust.com',
        'Referer': 'https://www.entrust.com/partner-directory/search?tiers=["strategic","technology","inbound-reseller","nfinity-hsm","technology-partner"]',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'upgrade-insecure-requests': '1',
    }

    def start_requests(self):

        pageNumber = 1
        link = f'https://www.entrust.com/partner-directory/search?tiers=["strategic","technology","inbound-reseller","nfinity-hsm","technology-partner","reseller-platinum-plus","reseller-platinum","reseller-gold","reseller-authorized","reseller","reseller-indirect","distributor-platinum-plus","distributor-platinum","distributor-gold","distributor"]&pageNumber={pageNumber}'
        yield scrapy.Request(method='GET', url=link, callback=self.parse, headers=self.headers, meta={'pageNumber': pageNumber})

    def parse(self, response):
        pageNumber = response.meta['pageNumber']
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            partners = soup.find_all('div', {'class': 'result-card__partner'})

            self.logger.info(f'Entrust Partners page: {pageNumber}, result: {len(partners)}')

            for partner in partners:
                partner_name = partner.find('h3', {'class': 'card-title'})
                if partner_name:
                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = ''

                    item['partner_company_name'] = partner_name.text.strip()

                    details_lst = partner.find('div', {'class': 'partner-card__details'}).find_all('div',
                                                                                                   recursive=False)
                    for div in details_lst:
                        if 'Solutions Offered' in div.text:
                            item['solutions'] = [li.text.strip() for li in div.find_all('li')]
                        elif 'Member Type' in div.text:
                            item['partner_type'] = [li.text.strip() for li in div.find_all('li')]

                    partner_link = partner.find('a', {'href': True})
                    if partner_link:
                        partner_link = 'https://www.entrust.com' + partner_link['href']
                        yield scrapy.Request(method='GET', url=partner_link, callback=self.parse_partner,
                                             headers=self.headers, meta={'item': item}, dont_filter=True)
                    else:
                        yield item

            # follow next pages
            if pageNumber == 1:
                last_url = soup.find('div', {'class': 'pagination'}).find('a', {'class': 'last', 'href': True}) if soup.find('div', {'class': 'pagination'}) else None
                if last_url:
                    last = int(re.search(r"pageNumber=(\d+)", last_url['href']).group(1))
                    for pageNumber in range(2, last+1):
                        link = f'https://www.entrust.com/partner-directory/search?tiers=["strategic","technology","inbound-reseller","nfinity-hsm","technology-partner","reseller-platinum-plus","reseller-platinum","reseller-gold","reseller-authorized","reseller","reseller-indirect","distributor-platinum-plus","distributor-platinum","distributor-gold","distributor"]&pageNumber={pageNumber}'
                        yield scrapy.Request(method='GET', url=link, callback=self.parse, headers=self.headers,
                                             meta={'pageNumber': pageNumber})

    def parse_partner(self, response):
        item = response.meta['item']
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")

            item['company_description'] = cleanhtml(soup.find('div', {'class': 'partner-detail__description'}).text.strip())
            if item['company_description'].startswith('Company Description'):
                item['company_description'] = item['company_description'].replace('Company Description', '', 1)

            item['company_domain_name'] = get_domain_from_url(soup.find('a', {'href': True, 'data-label': 'Visit Website'})['href']) if soup.find('a', {'href': True, 'data-label': 'Visit Website'}) else ''

            item['partner_tier'] = list()
            for ul in soup.find_all('ul', {'class': 'partner-detail__membership'}):
                if len(ul.find_all('li')) > 0:
                    for li in ul.find_all('li'):
                        if 'Program:' in li.text:
                            continue
                        item['partner_tier'].append(li.text.strip())

            sctns = soup.find_all('p')
            for sctn in sctns:
                if sctn.find('strong'):
                    if 'Address:' in sctn.find('strong').text:
                        address = sctn.text.replace('Address:', '', 1).strip().splitlines()
                        if len(address) > 1:
                            item['headquarters_address'] = ' '.join(address[:-1])
                            if item['headquarters_address'].strip() == ',':
                                item['headquarters_address'] = ''
                            item['headquarters_country'] = address[-1]
                        else:
                            item['headquarters_address'] = ' '.join(address)
                    if 'Contact Information:' in sctn.find('strong').text:
                        contact_links = sctn.find_all('a', {'href': True})
                        for contact_link in contact_links:
                            if 'tel:' in contact_link['href']:
                                item['general_phone_number'] = contact_link['href'].replace('tel:', '').strip()
                            if 'email-protection' in contact_link['href']:
                                item['general_email_address'] = contact_link['data-label'].strip()

            social = soup.find('ul', {'class': 'partner-detail__social'})
            if social:
                social_links = social.find_all('a', {'href': True})
                for social_link in social_links:
                    if 'linkedin' in social_link['href'].lower():
                        item['linkedin_link'] = social_link['href']
                    elif 'facebook' in social_link['href'].lower():
                        item['facebook_link'] = social_link['href']
                    elif 'twitter' in social_link['href'].lower():
                        item['twitter_link'] = social_link['href']
                    elif 'youtube' in social_link['href'].lower():
                        item['youtube_link'] = social_link['href']
                    elif 'instagram' in social_link['href'].lower():
                        item['instagram_link'] = social_link['href']
                    elif 'xing' in social_link['href'].lower():
                        item['xing_link'] = social_link['href']

        yield item
