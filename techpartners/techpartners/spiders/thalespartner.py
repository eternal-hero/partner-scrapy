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

partners_dict = dict()


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'thalespartner'
    partner_program_link = 'https://cpl.thalesgroup.com/partners/partner-search'
    partner_directory = 'Thales Partners Directory'
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

    def parse(self, response):
        global partners_dict

        # get option country value list
        r = requests.get('https://cpl.thalesgroup.com/json/partner_country')
        countries_data = json.loads(r.text)
        country_lst = [{'country_id': country['tid'], 'country': country['name'], 'region': country['field_region'].upper()} for country in countries_data]

        # get options type and tier value lists
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
            return
        soup = BS(response.text, "html.parser")
        type_lst = [{'value': li['data'], 'label': li.text} for li in (soup.find('div', id='type').find_all('li') if soup.find('div', id='type') else list())]
        tier_lst = [{'value': li['data'], 'label': li.text} for li in (soup.find('div', id='tier').find_all('li') if soup.find('div', id='tier') else list())]

        def parse_option(option_type, option_value, option_label):
            global partners_dict

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
                'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Connection': 'keep-alive',
                'Authority': 'cpl.thalesgroup.com',
                'Origin': 'https://cpl.thalesgroup.com',
                'Referer': 'https://cpl.thalesgroup.com/partners/partner-search',
                'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': 'Windows',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'x-requested-with': 'XMLHttpRequest',
            }
            api_link = 'https://cpl.thalesgroup.com/views/ajax?_wrapper_format=drupal_ajax'
            page = 0
            while True:
                payload = f'view_name=partners&view_display_id=block_1&view_args=&view_path=%2Fnode%2F17616&view_base_path=json%2Fgetallpartners&view_dom_id=ffe2278b4167808a0f42ce3487e831a5ccb8314081271238bf8cd51cdea76181&pager_element=0&field_partner_country_target_id={option_value if option_type == "partner_country" else "All"}&field_partner_region_value=All&field_partner_type_value={option_value if option_type == "partner_type" else "All"}&field_partner_tier_value={option_value if option_type == "partner_tier" else "All"}&field_partner_vertical_value=All&page={page}&_drupal_ajax=1&ajax_page_state%5Btheme%5D=thalesesecurity&ajax_page_state%5Btheme_token%5D=&ajax_page_state%5Blibraries%5D=paragraphs%2Fdrupal.paragraphs.unpublished%2Csystem%2Fbase%2Cthalesesecurity%2Fglobal-styling%2Cviews%2Fviews.ajax%2Cviews%2Fviews.module'
                r = requests.post(api_link, headers=headers, data=payload)
                if r.status_code != 200:
                    self.logger.info(f'ERROR OPTION REQUEST STATUS: {r.status_code}, URL: {r.url}')
                    break
                else:
                    data = json.loads(r.text)
                    soup = BS(data[2]['data'], "html.parser")
                    partners = soup.find_all('div', {'class': 'content-wrap'})

                    if partners and len(partners) > 0:
                        self.logger.info(f'Option: {option_type}, value: {option_value}, Page = {page}, Partners = {len(partners)}')
                        for partner in partners:

                            partner_link = partner.find('div', {'class': 'link-cell'}).find('a', {'href': True})['href'] if partner.find('div', {'class': 'link-cell'}) and partner.find('div', {'class': 'link-cell'}).find('a', {'href': True}) else None
                            if partner_link and partner_link in partners_dict:
                                item = partners_dict[partner_link]

                                if option_type == 'partner_country':
                                    item['headquarters_country'].append(option_label['country'])
                                    item['regions'].append(option_label['region'])
                                elif option_type == 'partner_type':
                                    item['partner_type'].append(option_label)
                                elif option_type == 'partner_tier':
                                    item['partner_tier'].append(option_label)

                # follow next pages
                if soup.find('nav', {'class': 'pager'}) and soup.find('nav', {'class': 'pager'}).find('li', {'class': 'pager__item--next'}):
                    page += 1
                    continue
                else:
                    break

        # handle all partners data
        page = 0
        while True:
            response = requests.get(self.partner_program_link + f'?page={page}')
            if response.status_code != 200:
                self.logger.info(f'ERROR REQUEST STATUS: {response.status_code}, URL: {response.url}')
                break
            else:
                soup = BS(response.text, "html.parser")
                partners = soup.find('div', {'class': 'views-element-container'}).find_all('div', {'class': 'content-wrap'}) if soup.find('div', {'class': 'views-element-container'}) else None

                if partners:
                    self.logger.info(f'Page = {page}, Partners = {len(partners)}')
                    for partner in partners:

                        # Initialize item
                        item = dict()
                        for k in self.item_fields:
                            item[k] = ''

                        item['partner_program_link'] = self.partner_program_link
                        item['partner_directory'] = self.partner_directory
                        item['partner_company_name'] = cleanhtml(partner.find('h2').text) if partner.find('h2') else ''

                        partner_link = partner.find('div', {'class': 'link-cell'}).find('a', {'href': True})['href'] if partner.find('div', {'class': 'link-cell'}) and partner.find('div', {'class': 'link-cell'}).find('a', {'href': True}) else None
                        if item['partner_company_name'] != '' and partner_link and partner_link.startswith('/partners/'):
                            r = requests.get('https://cpl.thalesgroup.com' + partner_link, item)
                            if r.status_code != 200:
                                self.logger.info(f'ERROR PARTNER REQUEST, STATUS: {r.status_code}, RESPONSE: {r.text}')
                            else:
                                s = BS(r.text, "html.parser")
                                data = s.find('section', {'class': 'partner-section'}).find('h2').findParent()
                                if data:
                                    item['company_description'] = cleanhtml(data.find('div', recursive=False).text) if data.find('div', recursive=False) else ''
                                    item['company_domain_name'] = get_domain_from_url(s.find('div', {'class': 'link-cell'}).find('a', {'href': True})['href']) if s.find('div', {'class': 'link-cell'}) and s.find('div', {'class': 'link-cell'}).find('a', {'href': True}) else ''
                                    # spans = data.find_all('span', recursive=False)
                                    # item['headquarters_address'], item['general_email_address'], item['general_phone_number'], item['industries'] = [cleanhtml(span.text) for span in spans]

                                item['partner_type'] = list()
                                item['partner_tier'] = list()
                                item['regions'] = list()
                                item['headquarters_country'] = list()

                            if partner_link in partners_dict:
                                self.logger.info(f'DUPLICATE PARTNER: {item["partner_company_name"]}, Page: {page}')

                            partners_dict[partner_link] = item

                        else:
                            self.logger.info(f'ERROR PARTNER LINK: {partner_link}')

                # follow next pages
                if soup.find('nav', {'class': 'pager'}) and soup.find('nav', {'class': 'pager'}).find('li', {'class': 'pager__item--next'}):
                    page += 1
                    continue
                else:
                    break

        # handle each option partners data
        for type_option in type_lst:
            if type_option['value'] == 'All':
                continue
            parse_option('partner_type', type_option['value'], type_option['label'])

        for type_option in tier_lst:
            if type_option['value'] == 'All':
                continue
            parse_option('partner_tier', type_option['value'], type_option['label'])

        for type_option in country_lst:
            parse_option('partner_country', type_option['country_id'], type_option)

        for item in partners_dict.values():
            yield item
