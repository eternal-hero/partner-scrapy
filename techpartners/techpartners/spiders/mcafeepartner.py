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
    name = 'mcafeepartner'
    partner_program_link = 'https://mcafeepartners.mcafee.com/en-us/directory/search'
    partner_directory = 'Mcafee Partner Directory'
    partner_program_name = ''
    crawl_id = None

    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 8,
        'DOWNLOAD_FAIL_ON_DATALOSS': False,
    }

    start_urls = [partner_program_link]

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
                   'primary_contact_name', 'primary_contact_phone_number',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts',
                   'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    def parse(self, response):
        if response.status == 200:
            soup = BS(response.text, "html.parser")
            countries = list()
            countries_lst = soup.find('select', {'id': 'l0'}).find_all('option', value=True)
            for optn in countries_lst:
                if optn['value'].strip() != '':
                    countries.append(optn['value'].strip())

            for country in countries:
                link = f'https://mcafeepartners.mcafee.com/en-us/directory/search?l={urllib.parse.quote_plus(country)}&p=0'
                r = requests.get(link)
                if r.status_code == 200:
                    state_soup = BS(r.text, "html.parser")
                    if state_soup.find('select', {'id': 'l1'}):
                        states = list()
                        states_lst = state_soup.find('select', {'id': 'l1'}).find_all('option', value=True)
                        for optn in states_lst:
                            if optn['value'].strip() != '' and 'Select State' not in optn['value'].strip():
                                states.append(optn['value'].strip())

                        for state in states:
                            link = f'https://mcafeepartners.mcafee.com/en-us/directory/search?l={urllib.parse.quote_plus(country + " - " + state)}&p=0'

                            yield scrapy.Request(method='GET', url=link, callback=self.parse_category,
                                                 meta={'country': country, 'page': f'search?l={urllib.parse.quote_plus(country + " - " + state)}&p=0'})
                    else:
                        yield scrapy.Request(method='GET', url=link, callback=self.parse_category,
                                             meta={'country': country, 'page': f'search?l={urllib.parse.quote_plus(country)}&p=0'})
        else:
            self.logger.info(f'FAILED TO OBTAIN SCRAPING OPTIONS, STATUS: {response.status}')

    def parse_category(self, response):
        country = response.meta['country']
        page = response.meta['page']

        if response.status != 200:
            print(f'PARSE CATEGORY ERROR RESPONSE, STATUS: {response.status}')
            yield scrapy.Request(method='GET', url=response.request.url, callback=self.parse_category, dont_filter=True,
                                 meta=response.meta)
            return

        if 'dataloss' in response.flags:
            print('**************************** Data Loss retried ****************************')
            yield scrapy.Request(method='GET', url=response.request.url, callback=self.parse_category, dont_filter=True,
                                 meta=response.meta)
            return

        soup = BS(response.text, "html.parser")
        partners = soup.find('div', {'class': 'row row-results'}).find_all('div', {'class': 'panel panel-default'})
        if len(partners) == 0:
            self.logger.info(f'RETRY Country & Page: {page}, results: {len(partners)}')
            yield scrapy.Request(method='GET', url=response.request.url, callback=self.parse_category, dont_filter=True,
                                 meta=response.meta)
            return

        self.logger.info(f'SUCCESS Country & Page: {page}, results: {len(partners)}')
        for partner in partners:
            partner_company_name = partner.find('h3').text if partner.find('h3') else ''
            if partner_company_name == '':
                continue

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = self.partner_program_name
            item['partner_company_name'] = partner_company_name

            item['company_domain_name'] = partner.find('a', href=True).text if partner.find('a', href=True) and 'Partner Details' not in partner.find('a', href=True).text else ''
            try:
                url_obj = urllib.parse.urlparse(item['company_domain_name'])
                item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                if x:
                    item['company_domain_name'] = x[-1]
            except Exception as e:
                print('DOMAIN ERROR: ', e)

            partner_lines = partner.text.splitlines()
            for l in partner_lines:
                data = l.strip()
                if data != '':
                    if data.startswith('Partnership Level:'):
                        item['partner_tier'] = data[data.find(':')+1:].strip()
                    if data.startswith('Phone:'):
                        item['general_phone_number'] = data[data.find(':')+1:].strip()

            item['certifications'] = list()
            partner_images = partner.find_all('img')
            for img in partner_images:
                data = img['src']
                if data != '':
                    if 'McAFEE_SOLUTION_PROVIDER' in data:
                        item['partner_type'] = 'Solution Provider'
                    elif 'McAFEE_AUTHORIZED_DISTRIBUTOR' in data:
                        item['partner_type'] = 'Distributor'
                    elif 'McAFEE_PRODUCT_SPECIALIST' in data:
                        item['certifications'].append('McAfee Certified Product Specialist')
                    elif 'M-HERO' in data:
                        item['certifications'].append('M-HERO')

            partner_link = None
            if partner.find('p', {'class': 'detailsLink'}):
                if partner.find('p', {'class': 'detailsLink'}).find('a', href=True):
                    partner_link = 'https://mcafeepartners.mcafee.com' + partner.find('p', {'class': 'detailsLink'}).find('a', href=True)['href']

            if partner_link:
                yield scrapy.Request(method='GET', url=partner_link, callback=self.parse_partner,
                                     dont_filter=True, meta={'country': country, 'item': item})
            else:
                yield item

        # follow next pages
        nav = soup.find('nav', {'aria-label': 'Solutions'})
        if nav:
            next_page = nav.find('a', {'aria-label': 'Next', 'href': True})
            if next_page:
                yield scrapy.Request(method='GET', url='https://mcafeepartners.mcafee.com/en-us/directory/' + next_page['href'], callback=self.parse_category,
                                     dont_filter=True, meta={'country': country, 'page': next_page['href']})

    def parse_partner(self, response):
        item = response.meta['item']
        country = response.meta['country']

        if response.status != 200:
            print(response.status)
            print(response.text)
            return

        soup = BS(response.text, "html.parser")

        address = soup.find('address')
        if address.find('span', {'itemprop': "addressCountry"}):
            item['headquarters_street'] = address.find('span', {'itemprop': "streetAddress"}).text if address.find('span', {'itemprop': "streetAddress"}) else ''
            item['headquarters_city'] = address.find('span', {'itemprop': "addressLocality"}).text if address.find('span', {'itemprop': "addressLocality"}) else ''
            item['headquarters_state'] = address.find('span', {'itemprop': "addressRegion"}).text if address.find('span', {'itemprop': "addressRegion"}) else ''
            item['headquarters_zipcode'] = address.find('span', {'itemprop': "postalCode"}).text if address.find('span', {'itemprop': "postalCode"}) else ''
            item['headquarters_country'] = address.find('span', {'itemprop': "addressCountry"}).text if address.find('span', {'itemprop': "addressCountry"}) else ''
        else:
            item['headquarters_country'] = country

        specializations_div = soup.find('section', {'id': 'specializations'})
        if specializations_div:
            item['specializations'] = list()
            for itm in specializations_div.find_all('p'):
                if itm.text.strip() != '':
                    item['specializations'].append(itm.text.strip())

        certifications_div = soup.find('section', {'id': 'certifications'})
        if certifications_div and certifications_div.find('p'):
            item['products'] = list()
            for itm in certifications_div.find('p').text.splitlines():
                if itm.strip() != '':
                    item['products'].append(itm[:itm.rfind(':')].strip())

        yield item
