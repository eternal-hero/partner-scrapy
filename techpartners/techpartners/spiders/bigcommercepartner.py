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
    name = 'bigcommercepartner'
    partner_program_link = 'https://partners.bigcommerce.com/directory/'
    partner_directory = 'Bigcommerce Partner Directory'
    partner_program_name = ''
    crawl_id = None

    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 4,
    }

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
        'Authority': 'partners.bigcommerce.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'upgrade-insecure-requests': 1,
        }

    def start_requests(self):
        yield scrapy.Request(method='GET', url=self.partner_program_link, callback=self.parse, headers=self.headers)

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            articles = soup.find('div', {'id': 'Locator_BodyContent_FacetsContainer'}).find_all('article', {'class': 'mktAvailableConstraintsContainer'})
            for article in articles:
                if 'Regions Served' in article.text:
                    regions = article.find_all('input', {'name': 'Regions_Served__cf'})
                    for region in regions:
                        region_name = region['value']
                        region_link = 'https://partners.bigcommerce.com/directory/search?f0=Regions+Served&f0v0=' + region_name
                        yield scrapy.Request(method='GET', url=region_link, callback=self.parse_region,
                                             headers=self.headers,
                                             meta={'region': region_name,
                                                   'page_number': 0})
                    break

    def parse_region(self, response):
        region = response.meta['region']
        page_number = response.meta['page_number']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            partners = soup.find('div', {'id': 'partnerCards'}).find_all('div', recursive=False)
            self.logger.info(f"Region = {region}, Page Number = {page_number}, Number of results = {len(partners)}")

            if len(partners) == 0:
                yield scrapy.Request(method='GET', url=response.request.url, callback=self.parse_region,
                                     headers=self.headers, dont_filter=True,
                                     meta=response.meta)
                return

            for partner in partners:
                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item['partner_company_name'] = partner.find('h3').text
                item['regions'] = region
                try:
                    partner_link = partner.find('a', {'href': True})['href']
                except:
                    partner_link = None

                if partner_link:
                    yield scrapy.Request(method='GET', url='https://partners.bigcommerce.com'+partner_link,
                                         headers=self.headers, callback=self.parse_partner, dont_filter=True,
                                         meta={'item': item})
                else:
                    yield item

            # follow next pages
            if page_number == 0:
                try:
                    pagination_div = soup.find('div', {'id': 'Locator_BodyContent_PaginationTop'}).find('b').text
                    last_page = math.ceil(int(pagination_div)/15)
                except Exception as e:
                    last_page = None
                    print(f"{region} & Page number {page_number}")
                    print(e)

                if last_page:
                    for page_number in range(1, last_page):
                        link = response.request.url + f'&p={page_number}'
                        yield scrapy.Request(method='GET', url=link, callback=self.parse_region,
                                             headers=self.headers,
                                             meta={'region': region,
                                                   'page_number': page_number})

    def parse_partner(self, response):
        item = response.meta['item']
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST PARTNER, COMPANY: {item["partner_company_name"]}, URL: {response.request.url}')
        else:
            soup = BS(response.text, "html.parser")

            type_divs = soup.find_all('p', {'class': 'details-title'})
            for type_data in type_divs:
                if 'Partner type:' in type_data.text:
                    item['partner_type'] = type_data.find_next('p').text
                    break

            tier_data = soup.find('div', {'class': 'partner-tier-badge'})
            if tier_data:
                tier_type = tier_data.find('img', {'id': 'Locator_BodyContent_TierBadgeImage'})
                if tier_type:
                    if 'elite.svg' in tier_type['src']:
                        item['partner_tier'] = 'Elite'
                    elif 'preferred.svg' in tier_type['src']:
                        item['partner_tier'] = 'Preferred'
                    elif 'partner.svg' in tier_type['src']:
                        item['partner_tier'] = 'Partner'
                else:
                    item['partner_tier'] = 'Enrolled'

            item['company_description'] = cleanhtml(soup.find('p', {'id': 'Locator_BodyContent_Overview_LongDescription'}).text if soup.find('p', {'id': 'Locator_BodyContent_Overview_LongDescription'}) else '')

            item['company_domain_name'] = soup.find('p', {'class': 'details-website'}).find('a', {'href': True})['href'] if soup.find('p', {'class': 'details-website'}) and soup.find('p', {'class': 'details-website'}).find('a', {'href': True}) and soup.find('p', {'class': 'details-website'}).find('a', {'href': True})['href'] else ''
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

            services_data = soup.find('div', {'id': 'ServicesTab'})
            if services_data:
                item['services'] = [{'name': itm.find('h5').text,'desc': itm.find('p').text} for itm in services_data.find_all('div', {'class': 'service-info'})]

            industries_data = services_data.find('div', {'class': 'row'}) if services_data else None
            if industries_data:
                item['industries'] = [itm.text.replace('• ', '') for itm in industries_data.find_all('div', {'class': 'vertical-item'})]

            clients_data = soup.find('div', {'id': 'ClientsTab'})
            if clients_data:
                item['partner_clients'] = list()
                clients = clients_data.find('div', {'class': 'row'}).find_all('a')
                for client in clients:
                    client_name = client['title'] if client['title'] else client['href'] if  client['href'] else ''
                    if client_name != '':
                        item['partner_clients'].append(client_name)

            cert_data = soup.find('img', {'id': 'Locator_BodyContent_CertificationBadgeImage', 'src': True})
            if cert_data:
                src = cert_data['src']
                if 'bc-certified.svg' in src:
                    item['certifications'] = 'BigCommerce Certified'
                else:
                    item['certifications'] = 'OmniChannel Certified'

            branches = False
            scripts = soup.find('head').find_all('script')
            for script in scripts:
                if '[{"Headquarters":' in script.text:
                    content = script.text[script.text.find('[{"Headquarters":'):]
                    content = content[: content.find(']')+1]
                    json_object = json.loads(content.strip())

                    locations = json_object
                    for location in locations:
                        if 'Headquarters' in location and location['Headquarters']:
                            item['headquarters_street'] = location['MailingSuite'] + ' ' if 'MailingSuite' in location and location['MailingSuite'] else ''
                            item['headquarters_street'] += location['MailingStreet'] if 'MailingStreet' in location and location['MailingStreet'] else ''
                            item['headquarters_city'] = location['MailingCity'] if 'MailingCity' in location and location['MailingCity'] else ''
                            item['headquarters_state'] = location['MailingState'] if 'MailingState' in location and location['MailingState'] else ''
                            item['headquarters_zipcode'] = location['MailingPostalCode'] if 'MailingPostalCode' in location and location['MailingPostalCode'] else ''
                            item['headquarters_country'] = location['MailingCountry'] if 'MailingCountry' in location and location['MailingCountry'] else ''
                            item['general_phone_number'] = location['Phone'] if 'Phone' in location and location['Phone'] else ''
                            item['general_email_address'] = location['Email'] if 'Email' in location and location['Email'] else ''
                            break

                    for location in locations:
                        if 'Headquarters' in location and not location['Headquarters']:
                            item['locations_street'] = location['MailingSuite'] + ' ' if 'MailingSuite' in location and location['MailingSuite'] else ''
                            item['locations_street'] += location['MailingStreet'] if 'MailingStreet' in location and location['MailingStreet'] else ''
                            item['locations_city'] = location['MailingCity'] if 'MailingCity' in location and location['MailingCity'] else ''
                            item['locations_state'] = location['MailingState'] if 'MailingState' in location and location['MailingState'] else ''
                            item['locations_zipcode'] = location['MailingPostalCode'] if 'MailingPostalCode' in location and location['MailingPostalCode'] else ''
                            item['locations_country'] = location['MailingCountry'] if 'MailingCountry' in location and location['MailingCountry'] else ''
                            item['general_phone_number'] = location['Phone'] if 'Phone' in location and location['Phone'] else ''
                            item['general_email_address'] = location['Email'] if 'Email' in location and location['Email'] else ''
                            branches = True
                            yield item
                    break

            if not branches:
                yield item
