# import needed libraries
import json
import re
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


def cfDecodeEmail(encodedString):
    r = int(encodedString[:2], 16)
    email = ''.join([chr(int(encodedString[i:i + 2], 16) ^ r) for i in range(2, len(encodedString), 2)])
    return email


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'iotevolutionpartner'
    partner_program_link = 'https://www.iotevolutionexpo.com/east/exhibitor-list.aspx'
    partner_directory = 'IOT-Evolution Partner Directory'
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
        'Authority': 'www.iotevolutionexpo.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Referer': 'https://www.iotevolutionexpo.com/east/exhibitor-list.aspx',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'upgrade-insecure-requests': '1',
        }

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
            return

        soup = BS(response.text, "html.parser")
        table = None
        tables = soup.find_all('table')
        for t in tables:
            if 'Company Name' in t.text:
                table = t
                break

        if table:
            partners = table.find('tbody').find_all('tr')
            for partner in partners:
                cells = partner.find_all('td')
                if len(cells) != 2 or 'Company Name' in cells[0].text:
                    continue

                link_tag = partner.find('a', {'href': True})
                if link_tag:
                    link = 'https://www.iotevolutionexpo.com' + link_tag['href']
                    yield scrapy.Request(method='GET', url=link, callback=self.parse_partner,
                                         headers=self.headers, dont_filter=True)

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

            item['partner_company_name'] = soup.find('span', {'id': 'company2'}).text if soup.find('span', {'id': 'company2'}) else ''
            item['partner_tier'] = soup.find('span', {'id': 'sponsor'}).text if soup.find('span', {'id': 'sponsor'}) else ''
            item['company_description'] = soup.find('span', {'id': 'profile'}).text if soup.find('span', {'id': 'profile'}) else ''
            item['categories'] = soup.find('span', {'id': 'categories'}).text if soup.find('span', {'id': 'categories'}) and soup.find('span', {'id': 'categories'}).text != 'N/A' else ''

            contact = soup.find('span', {'id': 'contact'})
            contact_name = contact.find('b', {'style': True})
            if contact_name:
                item['primary_contact_name'] = contact_name.text
                contact.find('b', {'style': True}).decompose()

            email_code = contact.find('a', {'class': '__cf_email__', 'data-cfemail': True})
            if email_code:
                item['primary_contact_email'] = cfDecodeEmail(email_code['data-cfemail'])
                contact.find('a', {'class': '__cf_email__', 'data-cfemail': True}).decompose()

            address_lst = list()
            if contact and contact.text != '':
                for br in contact.find_all('br'):
                    br.append('\n')
                contact_lines = contact.text.splitlines()
                for line in contact_lines:
                    if line.strip() == '':
                        continue
                    if re.search(r"^[ext\+\:\d\-\.\s()]*\-?[\-RABITJUMP]*$", line.strip(), re.IGNORECASE):
                        item['general_phone_number'] = line.strip()
                    else:
                        address_lst.append(line.strip())

            if len(address_lst) >= 2:
                item['headquarters_country'] = address_lst[-1]
                headquarters_address = ' '.join(address_lst[:-1]).strip()
                zipcode = re.search(r"[\d\-]+$", headquarters_address)
                if zipcode and len(zipcode.group()) >= 3:
                    item['headquarters_zipcode'] = zipcode.group()
                    headquarters_address = headquarters_address.replace(item['headquarters_zipcode'], '')
                else:
                    zipcode = re.search(r"\w{3}\s+\w{3}$", headquarters_address)
                    if zipcode:
                        item['headquarters_zipcode'] = zipcode.group()
                        headquarters_address = headquarters_address.replace(item['headquarters_zipcode'], '')

                if headquarters_address.rfind(',') != -1:
                    item['headquarters_street'] = headquarters_address[: headquarters_address.rfind(',')]
                    item['headquarters_city'] = headquarters_address[headquarters_address.rfind(',')+1:]
                else:
                    item['headquarters_address'] = headquarters_address

            connects = soup.find_all('div', {'class': 'socialtxtwrp'})
            for connect in connects:
                connect_link = connect.find('i', {'class': True})['class'] if connect.find('i', {'class': True}) else ''
                connect_txt = connect.find('div', {'class': 'socialtxt'}).find('a', {'href': True})['href']
                if connect_txt.startswith('^'):
                    continue
                if 'fa-twitter' in connect_link:
                    item['twitter_link'] = connect_txt

                if 'fa-facebook' in connect_link:
                    item['facebook_link'] = connect_txt

                if 'fa-linkedin' in connect_link:
                    item['linkedin_link'] = connect_txt

                if 'fa-instagram' in connect_link:
                    item['instagram_link'] = connect_txt

                if 'fa-globe' in connect_link:
                    item['company_domain_name'] = connect_txt
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

            yield item
