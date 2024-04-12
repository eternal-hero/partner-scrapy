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
    name = 'sonicwallpartner'
    partner_program_link = 'https://www.sonicwall.com/partner-locator/'
    partner_directory = 'Sonicwall Partner'
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
                   'primary_contact_name', 'primary_contact_phone_number',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36',
               'accept': 'application/json, text/javascript, */*; q=0.01',
               'accept-language': 'en-US,en;q=0.9,ar;q=0.8',
               'accept-encoding': 'gzip, deflate, br',
               'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
               'origin': 'https://www.sonicwall.com',
               'referer': 'https://www.sonicwall.com/partner-locator/results/',
               'authority': 'www.sonicwall.com',
               'connection': 'keep-alive',
               'sec-ch-ua': '"Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
               'sec-ch-ua-platform': 'Windows',
               'Sec-Fetch-Dest': 'document',
               'Sec-Fetch-Mode': 'navigate',
               'Sec-Fetch-Site': 'none',
               'sec-ch-ua-mobile': '?0',
               'sec-fetch-user': '?1',
               'x-requested-with': 'XMLHttpRequest',
               }

    def parse(self, response):
        categories = list()
        countries = list()

        if response.status == 200:
            data = response.text

            if 'ROBOTS' in data:
                yield scrapy.Request(self.partner_program_link, callback=self.parse, dont_filter=True)
                return

            soup = BS(data, "html.parser")
            for optn in soup.find('select', {'id': 'search_partner_type'}).find_all("option"):
                if optn['value'] != 'all':
                    categories.append(optn['value'])

            for optn in soup.find('select', {'id': 'search_country'}).find_all("option"):
                if optn['value'] != 'all':
                    countries.append([optn['data-region'], optn['value']])

            for partner_type in categories:
                for [region, country] in countries:
                    link = f'https://www.sonicwall.com/wp-admin/admin-ajax.php?action=sw_partners&specializations=all&partner_type={partner_type}&region=all&country={country}'
                    yield scrapy.Request(link, callback=self.get_partner_ids, dont_filter=True,
                                         meta={'partner_type': partner_type, 'region': region, 'country': country})

    def get_partner_ids(self, response):
        partner_type = response.meta['partner_type']
        region = response.meta['region']
        country = response.meta['country']

        if response.status == 200:
            if 'ROBOTS' in response.text:
                yield scrapy.Request(response.request.url, callback=self.get_partner_ids, meta=response.meta, dont_filter=True)
                return

            data = response.json()
            if 'features' in data and len(data['features']) > 0:
                partner_ids = list()
                for i in data['features']:
                    partner_ids.append(i['properties']['id'])
                msg = 'partner_ids=' + '%2C'.join(partner_ids)
                long = data['features'][0]['geometry']['coordinates'][0]
                lat = data['features'][0]['geometry']['coordinates'][1]

                for pageCount in range(1, 1+math.ceil(len(data['features'])/20)):
                    link = f'https://www.sonicwall.com/wp-admin/admin-ajax.php?action=sw_partner_details&page={pageCount}&lat={lat}&lng={long}&home_url=https://www.sonicwall.com/'
                    yield scrapy.Request(method='POST', url=link, body=msg, callback=self.parse_cat,
                                         headers=self.headers, dont_filter=True,
                                         meta={'partner_type': partner_type, 'region': region, 'country': country, 'pageCount': pageCount, 'msg': msg})
        else:
            yield scrapy.Request(response.request.url, callback=self.get_partner_ids, dont_filter=True, meta=response.meta)

    def parse_cat(self, response):
        partner_type = response.meta['partner_type']
        region = response.meta['region']
        country = response.meta['country']
        pageCount = response.meta['pageCount']
        msg = response.meta['msg']

        if response.status == 200:
            if 'ROBOTS' in response.text:
                yield scrapy.Request(method='POST', url=response.request.url, body=msg, callback=self.parse_cat,
                                     headers=self.headers, dont_filter=True,
                                     meta={'partner_type': partner_type, 'region': region, 'country': country,
                                           'pageCount': pageCount, 'msg': msg})
                return

            data = response.json()
            if 'total' in data and data['total'] > 0:
                soup = BS(data['content'], "html.parser")

                partners = soup.find_all("div", {'class': 'search-result-item'})
                self.logger.info(f"partner_type': {partner_type}, 'country': {country}, Total: {data['total']}, pageCount: {pageCount}, results: {len(partners)}")

                for partner in partners:
                    if partner.find("a", href=True):
                        partner_link = 'https://www.sonicwall.com' + partner.find("a", href=True).get('href')

                        yield scrapy.Request(partner_link, callback=self.parse_partner, dont_filter=True,
                                             meta={'partner_type': partner_type, 'region': region, 'country': country})

            else:
                self.logger.info(f"partner_type': {partner_type}, 'country': {country}, NO DATA")

        else:
            yield scrapy.Request(method='POST', url=response.request.url, body=msg, callback=self.parse_cat,
                                 headers=self.headers, dont_filter=True,
                                 meta={'partner_type': partner_type, 'region': region, 'country': country,
                                       'pageCount': pageCount, 'msg': msg})

    def parse_partner(self, response):
        partner_type = response.meta['partner_type']
        region = response.meta['region']
        country = response.meta['country']

        if response.status == 200:

            if 'ROBOTS' in response.text:
                yield scrapy.Request(response.request.url, callback=self.parse_partner, meta=response.meta,
                                     dont_filter=True)
                return

            soup = BS(response.text, "html.parser")
            partner = soup.find("section", {'class': 'partner-detail'})

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = self.partner_program_name

            if partner_type == 'Reseller':
                item['partner_type'] = 'Partner & Reseller'
            elif partner_type == 'Distributor':
                item['partner_type'] = 'Authorized Distributor'
            elif partner_type == 'mssp_tier':
                item['partner_type'] = 'Managed Services'
            elif partner_type == 'professional_services_tier':
                item['partner_type'] = 'Professional Services Partner'

            item['partner_company_name'] = partner.find('h1').text if partner.find('h1') else ''
            item['headquarters_address'] = partner.find('address').text if partner.find('address') else ''

            partner_links = partner.find_all('a', href=True)
            for pl in partner_links:
                if pl['href'].strip().startswith('http') or 'www.' in pl['href'].lower():
                    url_obj = urllib.parse.urlparse(pl['href'])
                    item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                    x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                    if x:
                        item['company_domain_name'] = x[-1]
                    # if '/' in item['company_domain_name']:
                    #     item['company_domain_name'] = item['company_domain_name'][:item['company_domain_name'].find('/')]

                elif pl['href'].startswith('tel:'):
                    item['general_phone_number'] = pl.text

            for lst in partner.find_all('div', {'class': 'inner-list'}):
                if lst.find('h4') and lst.find('h4').text == 'Specializations':
                    item['specializations'] = [li.text for li in lst.find_all('li')]
                    break

            item['headquarters_country'] = country
            item['regions'] = region

            yield item

        else:
            yield scrapy.Request(response.request.url, callback=self.parse_partner, meta=response.meta, dont_filter=True)
