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
    name = 'magentopartner'
    partner_program_link = 'https://magento.com/tech-resources/directory/'
    partner_directory = 'Magento Partner Directory'
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

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'Connection': 'keep-alive',
        'Authority': 'magento.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://magento.com/tech-resources/directory/',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'upgrade-insecure-requests': '1',
        }

    def start_requests(self):
        type_optns = {1: 'Solution Partner', 5: 'Associate Solution Partner'}
        region_optns = {'africa': 'Africa',
                        'asia_pacific': 'Asia-Pacific',
                        'north_america': 'North America',
                        'latin_america': 'Latin America',
                        'middle_east': 'Middle East',
                        'europe': 'Europe',
                        }
        partners_dict = dict()

        for t_val, type_optn in type_optns.items():
            for r_val, region_optn in region_optns.items():
                n = 1
                while True:
                    link = f'https://magento.com/tech-resources/directory/?p={n}&partner_type={t_val}&partner_region={r_val}'
                    r = requests.get(link, headers=self.headers)
                    if r.status_code == 200:
                        soup = BS(r.text, "html.parser")
                        partners = soup.find('div', {'id': f'directory-page-{n}'}).find('div').find_all('div', recursive=False)

                        if n == 1 and len(partners) == 10:
                            # follow next pages
                            try:
                                pages = math.ceil(int(soup.find('div', {'class': 'pd_totals'}).text.replace('Results', '').strip())/10)
                            except:
                                pages = None

                            if pages:
                                n = pages
                                continue

                        for partner in partners:
                            partner_logo = partner.find('a', {'class': 'partner_logo'})
                            if partner_logo:
                                partner_link = partner_logo['href']

                                if partner_link in partners_dict:
                                    partners_dict[partner_link].append(region_optn)
                                else:
                                    partners_dict[partner_link] = [region_optn]

                    break

        for link, regions in partners_dict.items():
            yield scrapy.Request(method='GET', url=link, callback=self.parse_partner,
                                 headers=self.headers,
                                 meta={'regions': regions})

    def parse_partner(self, response):

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
        else:

            soup = BS(response.text, "html.parser")

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = self.partner_program_name

            item['partner_company_name'] = soup.find('div', {'class': 'company_name'}).find('div', {'class': 'name'}).find('a', {'href': True}).text
            item['company_domain_name'] = soup.find('div', {'class': 'company_name'}).find('div', {'class': 'name'}).find('a', {'href': True})['href']
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

            item['regions'] = response.meta['regions']

            try:
                badge = soup.find('div', {'class': 'badge'}).find('img', {'title': True})['title']
            except:
                badge = None
            if badge:
                if badge.find(',') != -1:
                    item['partner_type'] = badge[:badge.find(',')].strip().replace('Adobe', '').strip()
                    item['partner_tier'] = badge[badge.find(',')+1:].strip()
                else:
                    item['partner_type'] = badge.strip().replace('Adobe', '').strip()

            if soup.find('li', {'id': 'company-tab'}):
                item['company_description'] = cleanhtml(soup.find('li', {'id': 'company-tab'}).find('div', {'class': 'tab_text'}).text)

            if soup.find('li', {'id': 'developers-tab'}):
                certifications = soup.find('li', {'id': 'developers-tab'}).find_all('h4', {'class': 'badge_title'})
                item['certifications'] = [cleanhtml(cert.text) for cert in certifications]

            if soup.find('li', {'id': 'clients-tab'}):
                clients = soup.find('li', {'id': 'clients-tab'}).find('div', {'class': 'clients'}).find_all('a')
                item['partner_clients'] = [{'name': client['title'], 'url': client['href']} for client in clients]

            if soup.find('div', {'class': 'sp_list'}):
                lsts = soup.find('div', {'class': 'sp_list'}).find_all('li', {'class': 'fields'})
                for lst in lsts:
                    if 'Industry' in lst.text:
                        item['industries'] = [li.text for li in lst.find_all('li')]

            if soup.find('li', {'id': 'products-services-tab'}):
                divs = soup.find('li', {'id': 'products-services-tab'}).find_all('div', recursive=False)
                for div in divs:
                    if 'Products' in div.find('h2', {'class': 'tab_name'}).text:
                        item['products'] = list()
                        for txt in div.find_all('h4'):
                            item['products'].append(txt.text)

                    elif 'Services' in div.find('h2', {'class': 'tab_name'}).text:
                        item['services'] = list()
                        for txt in div.find_all('h4'):
                            item['services'].append(txt.text)

            addresses = soup.find_all('address')
            if len(addresses) > 0:
                lines = addresses[0].text.splitlines()
                item['headquarters_address'] = cleanhtml(' '.join(lines[2:-1]).strip())
                item['headquarters_country'] = lines[-1].strip()

                if len(addresses) > 1:
                    for address in addresses[1:]:
                        lines = address.text.splitlines()
                        item['locations_address'] = cleanhtml(' '.join(lines[2:-1]).strip())
                        item['locations_country'] = lines[-1].strip()

                        yield item
                else:
                    yield item
            else:
                yield item
