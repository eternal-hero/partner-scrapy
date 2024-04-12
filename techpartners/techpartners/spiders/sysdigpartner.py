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


partners_dict = dict()


def cfDecodeEmail(encodedString):
    r = int(encodedString[:2], 16)
    email = ''.join([chr(int(encodedString[i:i + 2], 16) ^ r) for i in range(2, len(encodedString), 2)])
    return email


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'sysdigpartner'
    partner_program_link = 'https://sysdig.com/partner-locator/'
    partner_directory = 'Sysdig Partners Directory'
    partner_program_name = ''
    crawl_id = None

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

    def parse(self, response):

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Authority': 'sysdig.com',
            'Referer': 'https://sysdig.com/partner-locator/',
            'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': 'Windows',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'upgrade-insecure-requests': '1',
        }

        def parse_country(page_number, country):
            country_link = self.partner_program_link + f'page/{page_number}/?sd-tax-filter-partner-location[0]={country["value"]}'
            response = requests.get(country_link, headers=headers)

            if response.status_code != 200:
                self.logger.info(
                    f'ERROR {country["label"]}, page {page_number}, REQUEST STATUS: {response.status_code}, URL: {response.url}')
            else:
                soup = BS(response.text, "html.parser")
                partners = soup.find_all('article')

                for partner in partners:
                    if 'partner-locator__legend' in partner['class']:
                        partners.remove(partner)

                print(f'Country: {country["label"]}, Page: {page_number}, result partners: {len(partners)}')

                for partner in partners:
                    partner_name = partner.find('div', {'class': 'partner-locator__company-name'}).find('h2').text if partner.find('div', {'class': 'partner-locator__company-name'}) and partner.find('div', {'class': 'partner-locator__company-name'}).find('h2') else None
                    if partner_name:
                        if partner_name in partners_dict:
                            item = partners_dict[partner_name]
                        else:
                            # Initialize item
                            item = dict()
                            for k in self.item_fields:
                                item[k] = ''

                            item['partner_program_link'] = self.partner_program_link
                            item['partner_directory'] = self.partner_directory
                            item['partner_program_name'] = ''

                            item['partner_company_name'] = partner_name
                            item['headquarters_country'] = list()

                            item['integrations'] = partner.find('div', {'class': 'partner-locator__company-name'}).find(
                                'p').text.replace('Platforms:', '').strip() if partner.find('div', {
                                'class': 'partner-locator__company-name'}) and partner.find('div', {
                                'class': 'partner-locator__company-name'}).find('p') else ''

                            item['regions'] = partner.find('div', {
                                'class': 'partner-locator__territories'}).text.strip() if partner.find('div', {
                                'class': 'partner-locator__territories'}) else ''

                            links = partner.find_all('a', {'href': True})
                            for link in links:
                                if 'Email' in link.text:
                                    item['general_email_address'] = cfDecodeEmail(
                                        link['href'][link['href'].find('#') + 1:].strip())
                                elif 'website' in link.text.lower():
                                    item['company_domain_name'] = get_domain_from_url(link['href'])

                        if country['value'] != 'all' and country['label'] not in item['headquarters_country']:
                            item['headquarters_country'].append(country['label'])
                        partners_dict[partner_name] = item

                # follow next pages
                if soup.find('a', {'class': 'page larger', 'href': True}):
                    page_number += 1
                    parse_country(page_number, country)

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            countries = [{'value': country['value'], 'label': country.text} for country in soup.find('select', {'id': 'by_location'}).find_all('option')]
            self.logger.info(f'Site Countries = {len(countries)}')
            for country in countries:
                page_number = 1
                parse_country(page_number, country)

        for item in partners_dict.values():
            yield item
