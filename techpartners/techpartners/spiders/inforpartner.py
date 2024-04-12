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
    name = 'inforpartner'
    partner_program_link = 'https://partners.infor.com/'
    partner_directory = 'Infor Partner'
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

    main_headers = {
        'authority': 'partners.infor.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-encoding': 'gzip, deflate',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
    }

    headers = {
        'authority': 'partners.infor.com',
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'referer': 'https://partners.infor.com/',
        'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'x-kl-saas-ajax-request': 'Ajax_Request',
        'x-requested-with': 'XMLHttpRequest'
    }

    start_urls = [partner_program_link]

    type_dict = dict()
    region_dict = dict()
    country_dict = dict()

    def start_requests(self):
        response = requests.get(self.partner_program_link, headers=self.main_headers)
        soup = BS(response.text, "html.parser")

        search_fields = soup.find('ol', id='searchFields').find_all('li', recursive=False)
        for search_field in search_fields:
            if 'Partner Type' in search_field.find('b').text:
                type_list_options = {optn.find('input', {'name': True})['name']: optn.text.strip() for optn in search_field.find('ul', {'class': 'bonsai'}).find_all('li', recursive=False)}
            elif 'Region' in search_field.find('b').text:
                region_list_options = {optn.find('input', {'name': True})['name']: optn.text.strip() for optn in search_field.find('ul', {'class': 'bonsai'}).find_all('li', recursive=False)}
            elif 'Country' in search_field.find('b').text:
                country_list_options = {optn['value']: optn['value'].strip() for optn in search_field.find('ul', {'class': 'bonsai'}).find_all('input', {'value': True})}

        # # get list of country partners
        # for country_value, country_text in country_list_options.items():
        #     self.country_dict[country_text] = set()
        #     page = 1
        #     while True:
        #         country_link = f'https://partners.infor.com/?co={country_value}&page={page}'
        #         response = requests.get(country_link, headers=self.main_headers)
        #         soup = BS(response.text, "html.parser")
        #         try:
        #             partners = soup.find('div', id='resultGrid').find_all('div', recursive=False)
        #         except AttributeError:
        #             partners = list()
        #         self.logger.info(f'Infor country {country_text}, PAGE {page}, has {len(partners)} PARTNERS')
        #         for partner in partners:
        #             partner_id = partner.find('h2')['data-id']
        #             self.country_dict[country_text].add(partner_id)
        #
        #         if len(partners) >= 10:
        #             page += 1
        #             continue
        #         else:
        #             break

        # get list of type partners
        for type_value, type_text in type_list_options.items():
            self.type_dict[type_text] = set()
            page = 1
            while True:
                type_link = f'https://partners.infor.com/?{type_value}=true&page={page}'
                response = requests.get(type_link, headers=self.main_headers)
                soup = BS(response.text, "html.parser")
                try:
                    partners = soup.find('div', id='resultGrid').find_all('div', recursive=False)
                except AttributeError:
                    partners = list()
                self.logger.info(f'Infor  Type {type_text}, PAGE {page}, has {len(partners)} PARTNERS')
                for partner in partners:
                    partner_id = partner.find('h2')['data-id']
                    self.type_dict[type_text].add(partner_id)

                if len(partners) >= 10:
                    page += 1
                    continue
                else:
                    break

        # get list of region partners
        for region_value, region_text in region_list_options.items():
            self.region_dict[region_text] = set()
            page = 1
            while True:
                region_link = f'https://partners.infor.com/?{region_value}=true&page={page}'
                response = requests.get(region_link, headers=self.main_headers)
                soup = BS(response.text, "html.parser")
                try:
                    partners = soup.find('div', id='resultGrid').find_all('div', recursive=False)
                except AttributeError:
                    partners = list()
                self.logger.info(f'Infor Region {region_text}, PAGE {page}, has {len(partners)} PARTNERS')
                for partner in partners:
                    partner_id = partner.find('h2')['data-id']
                    self.region_dict[region_text].add(partner_id)

                if len(partners) >= 10:
                    page += 1
                    continue
                else:
                    break

        yield scrapy.Request(url='https://partners.infor.com/?page=1', headers=self.main_headers,
                             callback=self.parse, meta={'page': 1})

    def parse(self, response):
        page = response.meta['page']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            try:
                partners = soup.find('div', id='resultGrid').find_all('div', recursive=False)
            except AttributeError:
                partners = list()
            self.logger.info(f'Infor, PAGE {page}, has {len(partners)} PARTNERS')

            for partner in partners:
                partner_id = partner.find('h2')['data-id']
                partner_link = f'https://partners.infor.com/Search/PartnerDetails/{partner_id}'
                yield scrapy.Request(url=partner_link, callback=self.parse_partner,
                                     headers=self.headers, meta={'partner_id': partner_id})

            # follow next page
            if len(partners) >= 10:
                page += 1
                page_link = f'https://partners.infor.com/?page={page}'
                yield scrapy.Request(url=page_link, headers=self.main_headers, callback=self.parse, meta={'page': page})

    def parse_partner(self, response):
        partner_id = response.meta['partner_id']

        if response.status != 200:
            self.logger.info(f'ERROR PARTNER REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            partner = BS(response.text, "html.parser").find('div', {'class': 'partnerDetails'})

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = ''

            item['partner_company_name'] = partner.find('h1').text.strip()
            item['company_description'] = cleanhtml(partner.find('section').text.strip())

            if partner.find('div', {'class': 'certifiedPartnerHeader'}):
                item['partner_tier'] = partner.find('div', {'class': 'certifiedPartnerHeader'}).text.strip()
            elif partner.find('div', {'class': 'birstHeader'}):
                item['partner_tier'] = partner.find('div', {'class': 'birstHeader'}).text.strip()

            item['company_domain_name'] = get_domain_from_url(partner.find('a', {'href': True}).text.strip())

            divs = partner.find_all('div', {'class': 'featureBox'})
            for div in divs:
                if 'Infor Product' in div.text:
                    item['products'] = [li.text.strip() for li in div.find_next('div', {'class': 'featureBoxAlign'}).find_all('li')]

                elif 'Area of Interest' in div.text:
                    item['solutions'] = [li.text.strip() for li in div.find_next('div', {'class': 'featureBoxAlign'}).find_all('li')]

                elif 'Industry' in div.text:
                    item['industries'] = [li.text.strip() for li in div.find_next('div', {'class': 'featureBoxAlign'}).find_all('li')]

                elif 'Microverticals' in div.text:
                    item['specializations'] = [li.text.strip() for li in div.find_next('div', {'class': 'featureBoxAlign'}).find_all('li')]

            item['partner_type'] = list()
            for type_text in self.type_dict:
                if partner_id in self.type_dict[type_text]:
                    item['partner_type'].append(type_text)

            item['regions'] = list()
            for region_text in self.region_dict:
                if partner_id in self.region_dict[region_text]:
                    item['regions'].append(region_text)

            # item['headquarters_country'] = list()
            # for country_text in self.country_dict:
            #     if partner_id in self.country_dict[country_text]:
            #         item['headquarters_country'].append(country_text)

            for br in partner.find_all("br"):
                br.replace_with("\n")

            address = [line.strip() for line in partner.find('h2').text.splitlines() if line.strip()]
            item['headquarters_country'] = address[-1]
            item['headquarters_address'] = ' '.join(address)

            # try:
            #     city_state_postal = address[-2]
            #     x = re.search(r"(.*?),(.*)\s([-\s\d]+)?", city_state_postal)
            #     if x:
            #         item['headquarters_street'] = ' '.join(address[0:-2])
            #         item['headquarters_city'] = x.group(1).strip()
            #         item['headquarters_state'] = x.group(2).strip()
            #         item['headquarters_zipcode'] = x.group(3).strip()
            #     else:
            #         raise Exception
            # except Exception:
            #     item['headquarters_address'] = ' '.join(address[:-1])

            yield item
