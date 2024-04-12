# import needed libraries
import json
import re
import math
from techpartners.spiders.base_spider import BaseSpider
import scrapy
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'dickerpartner'
    partner_program_link = 'https://www.dickerdata.com.au/our-vendors-dicker-data'
    partner_directory = 'Dicker Partner Vendors'
    partner_program_name = ''
    crawl_id = None

    start_urls = [partner_program_link]

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

    geolocator = Nominatim(user_agent="MM_Geocoder")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=1)

    def parse(self, response):

        if response.status == 200:
            soup = BS(response.text, "html.parser")
            vendors = soup.find_all('div', {'class': 'dd-vendors__item'})
            for vendor in vendors:

                partner_link = vendor.find('a', href=True)['href'] if vendor.find('a', href=True) else ''

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item['partner_type'] = 'Vendor'
                item['partner_company_name'] = vendor.find('h3').text if vendor.find('h3') else ''
                txts = vendor.find_all('p')
                address = list()
                for txt in txts:
                    data = txt.text.strip()
                    if data.startswith('W:'):
                        item['company_domain_name'] = data.strip('W:').strip()
                        url_obj = urllib.parse.urlparse(item['company_domain_name'])
                        item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                        x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                        if x:
                            item['company_domain_name'] = x[-1]
                        if '/' in item['company_domain_name']:
                            item['company_domain_name'] = item['company_domain_name'][:item['company_domain_name'].find('/')]
                    elif data.startswith('P:'):
                        item['general_phone_number'] = data.strip('P:').strip()
                    elif data.strip() != '':
                        address.append(data.strip())

                item['headquarters_street'] = ' '.join(address[:-1])

                try:
                    x = re.search(r'\s*\d{5}\s*', address[-1])
                    if x:
                        item['headquarters_zipcode'] = x.group()
                    else:
                        x = re.search(r'\s*\d{4}\s*', address[-1])
                        if x:
                            item['headquarters_zipcode'] = x.group()

                    location = self.geocode(' '.join(address), language='en')
                    if not location:
                        location = self.geocode(' '.join(address[1:]), language='en')
                    if location:
                        location = self.reverse([location.latitude, location.longitude], language='en')
                        item["headquarters_city"] = location.raw['address']['city'] if location and 'address' in location.raw and 'city' in location.raw['address'] else ''
                        item["headquarters_state"] = location.raw['address']['state'] if location and 'address' in location.raw and 'state' in location.raw['address'] else ''
                        item["headquarters_country"] = location.raw['address']['country'] if location and 'address' in location.raw and 'country' in location.raw['address'] else ''
                        item["headquarters_zipcode"] = location.raw['address']['zipcode'] if location and 'address' in location.raw and 'zipcode' in location.raw['address'] and item["headquarters_zipcode"] == '' else ''
                    else:
                        item['headquarters_city'] = address[-1].replace(item["headquarters_zipcode"], '')

                except Exception as e:
                    print('ERROR', e)

                if partner_link != '':
                    yield scrapy.Request(url=partner_link, callback=self.parse_partner,
                                         meta={'item': item}, dont_filter=True)
                else:
                    yield item

        else:
            print(response.status)

    def parse_partner(self, response):
        item = response.meta['item']

        if response.status == 200:
            soup = BS(response.text, "html.parser")
            pargs = soup.find_all('div', {'class': 'row'})
            for p in pargs:
                if p.find('div', {'class': 'dd-heading-content__rt'}):
                    item['company_description'] = cleanhtml(p.find('div', {'class': 'dd-heading-content__rt'}).text)
                    break

            saved = False
            products = soup.find_all('div', {'class': 'media'})
            for product in products:
                if product.find('h2', {'class': 'media__title'}):
                    item['product_service_name'] = cleanhtml(product.find('h2', {'class': 'media__title'}).text)
                    item['product_service_description'] = ' '.join([cleanhtml(p.text) for p in product.find('div', {'class': 'media_content'}).find_all('p')])
                    saved = True
                    yield item

            if not saved:
                yield item

        else:
            yield scrapy.Request(url=response.request.url, callback=self.parse_partner,
                                 meta=response.meta, dont_filter=True)
            return
