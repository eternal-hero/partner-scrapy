# import needed libraries
import json
import math
import re
import time

import requests
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse

partners = dict()


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'nuixpartner'
    partner_program_link = 'https://www.nuix.com/partners'
    partner_directory = 'Nuix Partner Directory'
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
        'Content-Type': 'text/plain',
        'Authority': 'www.nuix.com',
        'Referer': 'https://www.nuix.com/partners',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'x-requested-with': 'XMLHttpRequest',
        }

    def parse(self, response):

        def parse_category(filter_dict):
            global partners

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjM0MDAzMTMiLCJhcCI6IjExMDMxMDA4MzYiLCJpZCI6IjA0NDE3ZGQzYzY5NDVhMTgiLCJ0ciI6IjQ2NzJlYWM1MGQyYzg1OTU5MTg5ZTVjOWE5YjA4NjA4IiwidGkiOjE2NjI1NDg1ODEzMTcsInRrIjoiNjY2ODYifX0=',
                'x-newrelic-id': 'VwIHUVVSCxAJVVhRBgQDUF0=',
                'origin': 'https://www.nuix.com',
                'Authority': 'www.nuix.com',
                'Referer': 'https://www.nuix.com/partners',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
                'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': 'Windows',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'x-requested-with': 'XMLHttpRequest',
            }
            payload = f'field_continent_value={"All" if filter_dict["filter_type"] != "regions" else filter_dict["filter_value"]}&field_industry_focus_value={"All" if filter_dict["filter_type"] != "solutions" else filter_dict["filter_value"]}&partner-services={"All" if filter_dict["filter_type"] != "services" else filter_dict["filter_value"]}&view_name=partner_locator&view_display_id=block_1&view_args=&view_path=%2Fnode%2F460&view_base_path=&view_dom_id=727f84fbb7e479865a483a045d2924d6f5bc47a8aa7c474386cc43eeaacb064b&pager_element=0&_drupal_ajax=1&ajax_page_state%5Btheme%5D=gavias_enzio&ajax_page_state%5Btheme_token%5D=&ajax_page_state%5Blibraries%5D=addtoany%2Faddtoany%2Cbetter_exposed_filters%2Fauto_submit%2Cbetter_exposed_filters%2Fgeneral%2Cckeditor_accordion%2Faccordion_style%2Ccore%2Fdrupal.dropbutton%2Ccore%2Fhtml5shiv%2Cdropdown_language%2Fdropdown-language-selector%2Cextlink%2Fdrupal.extlink%2Cfontawesome%2Ffontawesome.svg.shim%2Cgavias_content_builder%2Fgavias_content_builder.assets.frontend%2Cgavias_enzio%2Fgavias_enzio.skin.turquoise%2Cgavias_enzio%2Fglobal-styling%2Cgavias_sliderlayer%2Fgavias_sliderlayer.assets.frontend%2Cgeofield_map%2Fgeofield_google_map%2Cgeofield_map%2Fgeojson%2Cgeofield_map%2Fintersection_observer%2Cgeofield_map%2Foverlappingmarkerspiderfier%2Clayout_discovery%2Fonecol%2Cparagraphs%2Fdrupal.paragraphs.unpublished%2Csystem%2Fbase%2Cviews%2Fviews.ajax%2Cviews%2Fviews.module'
            api_link = 'https://www.nuix.com/views/ajax?_wrapper_format=drupal_ajax'

            for i in range(3):
                try:
                    category_response = requests.post(url=api_link, headers=headers, data=payload)
                    if category_response.status_code != 200:
                        self.logger.info(f'ERROR PARTNER REQUEST, STATUS: {category_response.status_code}')
                        raise Exception
                    else:
                        break
                except:
                    self.logger.info(f'ERROR CATEGORY REQUEST, retry = {i}')
                    time.sleep(2)
                    continue
            else:
                category_response = None

            if category_response:
                try:
                    jdata = json.loads(category_response.text)
                    if len(jdata) > 0 and 'settings' in jdata[0] and 'geofield_google_map' in jdata[0]['settings']:
                        page_partners = jdata[0]['settings']['geofield_google_map'][list(jdata[0]['settings']['geofield_google_map'].keys())[0]]['data']['features']
                        self.logger.info(f"Category {filter_dict['filter_type']}: {filter_dict['filter_label']}, Number of results = {len(page_partners)}")
                    else:
                        raise Exception
                except:
                    page_partners = list()

                for partner in page_partners:
                    if 'properties' in partner:
                        partner_data = BS(partner['properties']['description'], "html.parser").find('h2')
                        partner_link = partner_data.find('a', {'href': True})['href']
                        parse_partner(partner_link, filter_dict)

        def parse_partner(link, filter_dict=None):
            global partners

            if link in partners.keys():
                item = partners[link]
            else:
                for i in range(3):
                    try:
                        partner_response = requests.get(url='https://www.nuix.com' + link, headers=self.headers)
                        if partner_response.status_code != 200:
                            self.logger.info(f'ERROR PARTNER REQUEST, STATUS: {partner_response.status_code}, URL: https://www.nuix.com{link}')
                            raise Exception
                        else:
                            break
                    except:
                        self.logger.info(f'ERROR PARTNER REQUEST, retry = {i}, URL: {link}')
                        time.sleep(2)
                        continue
                else:
                    partner_response = None

                if not partner_response:
                    item = None
                else:
                    soup = BS(partner_response.text, "html.parser")

                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = self.partner_program_name

                    item['partner_company_name'] = soup.find('h2', {'class': 'page-title'}).text.strip()

                    partner_data = soup.find('div', {'class': 'item'})
                    divs = partner_data.find_all('div', recursive=False)
                    for div in divs:
                        if 'views-field' in div['class'] and 'views-field-body' in div['class']:
                            item['company_description'] = cleanhtml(div.text)
                        elif 'views-field' in div['class'] and 'views-field-field-partner-website-link' in div['class']:
                            item['company_domain_name'] = div.find('a', {'href': True})['href'] if div.find('a', {
                                'href': True}) else ''
                            try:
                                url_obj = urllib.parse.urlparse(item['company_domain_name'])
                                item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                                x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                                if x:
                                    item['company_domain_name'] = x[-1]
                                if '/' in item['company_domain_name']:
                                    item['company_domain_name'] = item['company_domain_name'][
                                                                  :item['company_domain_name'].find('/')]
                            except Exception as e:
                                print('DOMAIN ERROR: ', e)

                    divs = soup.find_all('div', {'class': 'item-image'})
                    for div in divs:
                        if div.find('img', {'src': True, 'alt': True}) and 'badges' in \
                                div.find('img', {'src': True, 'alt': True})['src']:
                            item['partner_tier'] = div.find('img', {'src': True, 'alt': True})['alt']
                            break

                    locations = soup.find('div', {'class': 'partner-address item'}).find_all('div', recursive=False)
                    if len(locations) > 0:
                        item['headquarters_address'] = cleanhtml(' '.join([txt.text.strip() for txt in locations[0].find_all('p')]))

                    if len(locations) > 1:
                        item['locations_address'] = [cleanhtml(' '.join([txt.text.strip() for txt in location.find_all('p')])) for location in locations[1:]]

                    item['regions'] = list()
                    item['services'] = list()
                    item['solutions'] = list()

                    partners[partner_link] = item

            if item and filter_dict:
                    if filter_dict['filter_type'] == 'regions':
                        item['regions'].append(filter_dict['filter_label'])
                    if filter_dict['filter_type'] == 'services':
                        item['services'].append(filter_dict['filter_label'])
                    if filter_dict['filter_type'] == 'solutions':
                        item['solutions'].append(filter_dict['filter_label'])

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            partner_filters = list()
            soup = BS(response.text, "html.parser")

            # get regions options
            region_slct = soup.find('select', {'name': 'field_continent_value'})
            if region_slct:
                partner_filters += [{'filter_type': 'regions', 'filter_value': optn['value'], 'filter_label': optn.text.strip()} for optn in region_slct.find_all('option')]

            # get services options
            services_slct = soup.find('select', {'name': 'partner-services'})
            if services_slct:
                partner_filters += [{'filter_type': 'services', 'filter_value': optn['value'], 'filter_label': optn.text.strip()} for optn in services_slct.find_all('option')]

            # get solutions options
            solutions_slct = soup.find('select', {'name': 'field_industry_focus_value'})
            if solutions_slct:
                partner_filters += [{'filter_type': 'solutions', 'filter_value': optn['value'], 'filter_label': optn.text.strip()} for optn in solutions_slct.find_all('option')]

            page_partners = soup.find_all('div', {'class': 'item-image'})
            self.logger.info(f"Number of Total results = {len(page_partners)}")

            # parse main page partners (without filters)
            for partner in page_partners:
                data = partner.find('a', {'href': True})
                partner_link = data['href']

                parse_partner(partner_link)

            # parse filters to update partners
            for partner_filter in partner_filters:
                if partner_filter['filter_value'] == 'All':
                    continue
                else:
                    parse_category(partner_filter)

            # yield stored partners
            for item in partners.values():
                locations = item['locations_address']
                if type(locations) is list and len(locations) > 0:
                    for location in locations:
                        item['locations_address'] = location
                        yield item
                else:
                    yield item
