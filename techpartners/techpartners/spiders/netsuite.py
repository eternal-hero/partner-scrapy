# import needed libraries
import re

import requests
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from urllib.parse import urlparse


def parse_link(link):
    url_obj = urlparse(link)
    url = url_obj.netloc if url_obj.netloc != '' else url_obj.path

    x = re.split(r'www\.', url, flags=re.IGNORECASE)
    if x:
        url = x[-1]

    if '/' in url:
        url = url[:url.find('/')]
    return url


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'netsuitepartner'
    partner_program_link = 'https://www.netsuite.com/portal/partners/alliance-partner-program.shtml'
    partner_directory = 'Oracle Netsuite Partner'
    partner_program_name = ''
    crawl_id = 1273

    domain = 'https://www.netsuite.com'

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'headquarters_street',
                   'headquarters_city', 'headquarters_state', 'headquarters_zipcode', 'headquarters_country',
                   'locations_street', 'locations_city', 'locations_state', 'locations_zipcode', 'locations_country',
                   'regions', 'languages', 'services', 'solutions', 'products', 'pricing', 'specializations',
                   'categories', 'year_founded', 'partnership_timespan', 'general_phone_number', 'general_email_address',
                   'use_case', 'integrations', 'contract_vehicles', 'company_characteristics', 'linkedin_link',
                   'twitter_link', 'facebook_link', 'primary_contact_name', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'certified?', 'industries', 'partner_clients', 'notes']

    start_urls = [partner_program_link]

    def parse(self, response):
        """
        parse search page and get results json data
        :param response:
        :return:
        """
        n = 1
        divs = response.xpath('//div[@class="container"]').extract()

        for div in divs:
            soup = BS(div, "html.parser")
            label = soup.find("h3")
            if label and 'NetSuite Global Systems Integrators' in label.text:
                global_div = soup
            elif label and 'NetSuite Regional Systems Integrators' in label.text:
                regional_div = soup

        # get all partners images
        global_partners = global_div.find_all('img', {"class": "img-fluid"})
        regional_partners = regional_div.find_all('img', {"class": "img-fluid"})

        if global_partners and len(global_partners) > 0:

            for partner in global_partners:
                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                try:
                    item['partner_company_name'] = partner.parent['data-tracklinktext']
                except:
                    try:
                        item['partner_company_name'] = partner['alt']
                    except:
                        item['partner_company_name'] = ''

                if partner.parent.name == 'a' and 'portal/partners' in partner.parent['href']:
                    link = ''
                    url = ''
                    netsuite_link = self.domain + partner.parent['href'] if self.domain not in partner.parent['href'] else partner.parent['href']

                    if item['partner_company_name'] != '':
                        res = requests.get(netsuite_link)
                        if res.status_code == 200:
                            partner_soup = BS(res.text, "html.parser")
                            sub_links = partner_soup.find_all('a', href=True)
                            for l in sub_links:
                                url_obj = urlparse(l['href'])
                                sub_domain = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                                if item['partner_company_name'] in sub_domain:
                                    link = sub_domain
                                    break

                            divs = partner_soup.find_all('div', {'class': "row"})
                            for div in divs:
                                try:
                                    div_header = div.find('h3').text
                                    if 'overview' in div_header.lower():
                                        item['company_description'] = div.find('p').text
                                    elif 'service offerings' in div_header.lower() or 'services' in div_header.lower():
                                        item['services'] = [li.text for li in div.find_all('li')]
                                    elif 'industries' in div_header.lower():
                                        item['industries'] = [li.text for li in div.find_all('li')]
                                except:
                                    continue

                        if link != '':
                            url = parse_link(link)

                elif partner.parent.name == 'a' and 'portal/partners' not in partner.parent['href']:
                    netsuite_link = ''
                    link = partner.parent['href']
                    url = parse_link(link)
                else:
                    netsuite_link = ''
                    link = ''
                    url = ''

                item['partner_type'] = 'Global Partner'
                item['company_domain_name'] = url
                yield item

        if regional_partners and len(regional_partners) > 0:

            for partner in regional_partners:
                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                if partner.parent.name == 'a':
                    partner = partner.parent
                    link = partner['href']
                    url = parse_link(link)

                    try:
                        alt = partner['data-tracklinktext']
                    except:
                        try:
                            alt = partner.find('img')['alt']
                        except:
                            alt = link.domain.strip().capitalize()
                else:
                    link = ''
                    url = ''
                    try:
                        alt = partner['alt']
                    except:
                        alt = ''

                if url.startswith('/portal/partners'):
                    # url = 'https://www.netsuite.com/' + url
                    url = ''

                item['partner_type'] = 'Regional Partner',
                item['partner_company_name'] = alt,
                item['company_domain_name'] = url

                yield item
