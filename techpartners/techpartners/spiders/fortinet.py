# import needed libraries
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
    name = 'fortinetpartner'
    site_name = 'fortinet Partner Directory'
    page_link = 'https://partnerportal.fortinet.com/directory/'
    start_urls = ['https://partnerportal.fortinet.com/directory/']

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
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
               'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
               'Accept-Encoding': 'gzip, deflate, br',
               'Referer': 'https://partnerportal.fortinet.com',
               'Origin': 'https://partnerportal.fortinet.com',
               'Connection': 'keep-alive',
               'Sec-Fetch-Dest': 'document',
               'Sec-Fetch-Mode': 'navigate',
               'Sec-Fetch-Site': 'none',
               }

    done_links = set()

    def parse(self, response):
        if response.status == 200:
            links = response.xpath('//div[@id = "Locator_BodyContent_RegionButtons"]//a/@id').getall()
            for _id in links:
                prod_link = 'https://partnerportal.fortinet.com/directory/search?l=' + _id.replace(' ', '+').replace(',', '%2C')
                r = requests.get(prod_link, headers=self.headers)
                if r.status_code == 200:
                    response = HtmlResponse(url="HTML string", body=r.content, encoding='utf-8')
                    countries = response.xpath('//select[@id = "l"]//option/@value').getall()
                    categories = ['Reseller', 'Distributor']
                    for country in countries:
                        for category in categories:
                            country_link = f"https://partnerportal.fortinet.com/directory/search?f0=Line+of+Business&f0v0={category}&l={country.replace(' ', '+').replace(',', '%2C')}"
                            # country_link = 'https://partnerportal.fortinet.com/directory/search?l=' + country.replace(' ', '+').replace(',', '%2C')
                            yield scrapy.Request(country_link, callback=self.parse_country, headers=self.headers,
                                                 meta={'region': _id, 'country': country, 'category': category})

    def parse_country(self, response):
        region = response.meta['region']
        country = response.meta['country']
        category = response.meta['category']
        country_link = response.request.url
        soup = BS(response.text, 'html.parser')
        offers = soup.find_all('div', {'class': "panel-header"})
        self.logger.info(f'Country: {country}, Category: {category}, offers: {len(offers)} - {country_link}')
        if len(offers) == 0:
            for i in range(5):
                sess = requests.Session()
                sess.get(self.page_link, headers=self.headers)
                r = sess.get(country_link, headers=self.headers)
                if r.status_code != 200:
                    continue

                soup = BS(r.content, 'html.parser')
                offers = soup.find_all('div', {'class': "panel-header"})
                self.logger.info(f'Country: {country}, Category: {category}, offers: {len(offers)} - {country_link}')
                if len(offers) > 0:
                    break
            # else:
            #     self.logger.info(f'ERROR PAGE: {country_link}')
            #     return

        self.done_links.add(country_link)

        divs = soup.find_all('div', {'class': 'col-sm-12'})
        for div in divs:
            if not div.find('div', {'class': "panel-header"}):
                continue

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.page_link
            item['partner_directory'] = 'Fortinet Partner'
            item['partner_company_name'] = cleanhtml(
                div.find('div', {'class': "panel-header"}).find('div', {'class': "col-sm-10"}).get_text())
            item['regions'] = region
            item['partner_type'] = category
            item['partner_tier'] = cleanhtml(div.find('div', {'class': "panel-header"}).find('div', {'class': "col-sm-2 partner-type"}).get_text())
            item['company_domain_name'] = div.find('a', {'class': "locator-parter-site"})['href'] if div.find('a', {'class': "locator-parter-site"}) else ''
            try:
                url_obj = urllib.parse.urlparse(item['company_domain_name'])
                item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                if x:
                    item['company_domain_name'] = x[-1]
            except Exception as e:
                print('DOMAIN ERROR: ', e)

            item['headquarters_country'] = country
            blocks = div.find_all('div', {'class': 'partner-info-box'})
            if len(blocks) > 0:
                info1 = cleanhtml(blocks[0].find('p', {'class': "locator-partner-info"})).replace(
                    item['company_domain_name'], '')
                phone = info1[info1.find('Phone:'):info1.find('http')].replace('Phone:', '').strip()
                address = info1[:info1.find('Phone')].strip()
                item['headquarters_address'] = address
                item['general_phone_number'] = phone

            services_blocks = div.find_all('div', {'class': 'services-box'})
            for services_block in services_blocks:
                if 'Service(s)' in services_block.text:
                    if services_block:
                        data = services_block.find('ul').find_all('li')
                        item['services'] = [cleanhtml(tag.text) for tag in data]
                if 'Specialization(s)' in services_block.text:
                    if services_block:
                        data = services_block.find('ul').find_all('li')
                        item['specializations'] = [cleanhtml(tag.text) for tag in data]

            yield item

        # follow next pages
        links = soup.find('nav', {'aria-label': "Solutions"}).find_all('a') if soup.find('nav', {'aria-label': "Solutions"}) else list()
        if len(links) > 0:
            for link in links:
                try:
                    if 'p=' in link['href'] and 'p=0' not in link['href']:
                        country_link = 'https://partnerportal.fortinet.com/directory/' + link['href']
                        if country_link in self.done_links:
                            continue
                        yield scrapy.Request(country_link, callback=self.parse_country, headers=self.headers,
                                             meta={'region': region, 'country': country, 'category': category})
                except Exception as e:
                    print('ERROR NEXT: ', e)
