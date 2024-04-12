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
    name = 'rosslarepartner'
    partner_program_link = 'https://rosslaresecurity.com/find-distributor-integrators/'
    partner_directory = 'Rosslaresecurity Partners Directory'
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

    start_urls = [partner_program_link]

    region_dict = dict()

    api_link = 'https://rosslaresecurity.com/wp-admin/admin-ajax.php'

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR PARSE REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            # get regions and countries data
            scripts = soup.find_all('script')
            for script in scripts:
                if 'var countries' in script.text:
                    content = script.text[script.text.find('var countries'):]
                    content = content[content.find('countries'):]
                    content = content[: content.find('function')]
                    countries = re.findall(r"[^/]countries\[.*?]\s?=.*?;", content)
                    for txt in countries:
                        country = re.search(r"countries\[(.*?)]\s?=(.*?);", txt)
                        self.region_dict[country.group(1).strip()] = country.group(2).strip()
                    break

            headers = {
                'authority': 'rosslaresecurity.com',
                'Origin': 'https://rosslaresecurity.com',
                'Referer': 'https://rosslaresecurity.com/find-distributor-integrators/',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'en-US,en;q=0.9',
                'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': 'Windows',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'x-requested-with': 'XMLHttpRequest',
            }

            # get locations pin data
            locations_response = requests.request(method='POST', url=self.api_link, data='action=distributor_filter', headers=headers)
            if locations_response.status_code == 200:
                locations = re.findall(r'markersID\.push\(\"(.+?)\"\);markersTitle\.push\(\"(\d+?)\"\);markersLat\.push\(([\d.]+?)\);markersLng\.push\(([\d.]+?)\);', locations_response.text)
                self.logger.info(f'Number of locations = {len(locations)}')
                for location in locations:
                    location_id = location[1]
                    location_lat = location[2]
                    location_lng = location[3]

                    yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse_location,
                                         headers=headers, body=f'action=distributor_cluster_filter&locations%5B0%5D%5BmarkerIds%5D={location_id}&locations%5B0%5D%5Bmarkerlats%5D={location_lat}&locations%5B0%5D%5Bmarkerlngs%5D={location_lng}')

    def parse_location(self, response):

        if response.status != 200:
            self.logger.info(f'ERROR LOCATION REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            item_div = soup.find('div', {'class': 'item'})

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = ''

            item['partner_company_name'] = item_div.find('h2').text.strip() if item_div.find('h2') else ''
            item['company_domain_name'] = item_div.find('a', {'href': True})['href'] if item_div.find('a', {'href': True}) else ''
            item['company_domain_name'] = get_domain_from_url(item['company_domain_name'])
            item['headquarters_address'] = item_div.find('p').text.strip() if item_div.find('p') else ''
            item['headquarters_country'] = item['headquarters_address'][item['headquarters_address'].rfind(',')+1:].strip()

            for region in self.region_dict.keys():
                if item['headquarters_country'] in self.region_dict[region]:
                    item['regions'] = region
                    break

            yield item
