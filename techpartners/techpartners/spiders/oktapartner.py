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


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'oktapartner'
    partner_program_link = 'https://www.okta.com/partners/meet-our-partners/'
    partner_directory = 'Okta Partner'
    partner_program_name = ''
    crawl_id = None

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

    done_partners = dict()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        'Authority': 'www.okta.com',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
        'Origin': 'https://www.okta.com',
        'Referer': 'https://www.okta.com/partners/meet-our-partners/',
        'Sec-Ch-Ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'Sec-Ch-Ua-mobile': '?0',
        'Sec-Ch-Ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'x-requested-with': 'XMLHttpRequest',
    }
    api_link = 'https://www.okta.com/views/ajax?_wrapper_format=drupal_ajax'

    start_urls = [partner_program_link]

    def parse(self, response):

        soup = BS(response.text, "html.parser")
        regions = [{'label': region.text, 'value': region['value']} for region in soup.find('select', {'name': 'field_region_tid'}).find_all('option')]
        for region_dict in regions:
            region_id = region_dict['value']
            region_label = region_dict['label']
            page = 0
            while True:
                payload = f'view_name=meet_our_partners&view_display_id=meet_our_partners&view_args=&view_path=%2Fnode%2F13021&view_base_path=partners%2Fmeet-our-partners&view_dom_id=5ef4130c1735587ae0196e3b081ad297bece7be1a92041dced3f49290b627ac3&pager_element=0&field_partner_type_tid=All&field_partner_specialization_tid=All&field_partner_level_tid=All&field_region_tid={region_id}&page={page}&_drupal_ajax=1&ajax_page_state%5Btheme%5D=okta_www_theme&ajax_page_state%5Btheme_token%5D=&ajax_page_state%5Blibraries%5D=better_exposed_filters%2Fauto_submit%2Cbetter_exposed_filters%2Fgeneral%2Cokta_attribution%2Fokta_attribution%2Cokta_datalayer%2Fokta_datalayer%2Cokta_geolocation%2Fokta_geolocation%2Cokta_myokta%2Fokta_myokta%2Cokta_www_theme%2Farray-from-polyfill%2Cokta_www_theme%2Fblock-forms-by-location%2Cokta_www_theme%2Fchosen%2Cokta_www_theme%2Fckeditor%2Cokta_www_theme%2Fcustom-event-polyfill%2Cokta_www_theme%2Fdrift_attribution%2Cokta_www_theme%2Fexposed-filters%2Cokta_www_theme%2Ffooter-translate-select%2Cokta_www_theme%2Ffor-each-node-list-polyfill%2Cokta_www_theme%2Fglobal-dependencies%2Cokta_www_theme%2Fheader%2Cokta_www_theme%2Flanguage-switcher%2Cokta_www_theme%2Flazy-load-images%2Cokta_www_theme%2Fmain-menu%2Cokta_www_theme%2Fmodal%2Cokta_www_theme%2Fmunchkin%2Cokta_www_theme%2Fnode-partner--teaser%2Cokta_www_theme%2Fokta-footer%2Cokta_www_theme%2Fparagraph--logo-set%2Cokta_www_theme%2Fparagraphs-column-item%2Cokta_www_theme%2Fparagraphs-columns%2Cokta_www_theme%2Fparagraphs-page-section%2Cokta_www_theme%2Fparagraphs-tippy-top%2Cokta_www_theme%2Fpartners-exposed-filters%2Cokta_www_theme%2Ftable-of-contents%2Cokta_www_theme%2Fview-partners%2Cpagerer%2Fbase.css%2Cpagerer%2Fmultipane%2Cparagraphs%2Fdrupal.paragraphs.unpublished%2Csystem%2Fbase%2Cviews%2Fviews.ajax%2Cviews%2Fviews.module'
                response = requests.request(method='POST',
                                            url=self.api_link,
                                            headers=self.headers,
                                            data=payload
                                            )

                if response.status_code != 200:
                    self.logger.info(f'ERROR REQUEST STATUS: {response.status_code}, URL: {response.url}')
                else:
                    try:
                        jdata = json.loads(response.text)
                        for i in jdata:
                            if 'command' in i and i['command'] == 'insert':
                                data = i['data']
                                break
                        else:
                            raise Exception
                    except:
                        data = None

                    if data:
                        soup = BS(data, "html.parser")
                        partners = soup.find_all('article')
                        self.logger.info(f'Okta Region: {region_label}, Page: {page}, partners: {len(partners)}')

                        for partner in partners:
                            partner_id = partner.find('div', {'class': 'PartnerTeaser__modal-trigger', 'data-target': True})['data-target'] if partner.find('div', {'class': 'PartnerTeaser__modal-trigger', 'data-target': True}) else None
                            if partner_id and partner_id in self.done_partners.keys():
                                item = self.done_partners[partner_id]
                            else:
                                # Initialize item
                                item = dict()
                                for k in self.item_fields:
                                    item[k] = ''

                                item['partner_program_link'] = self.partner_program_link
                                item['partner_directory'] = self.partner_directory
                                # item['partner_program_name'] = ''

                                item['partner_company_name'] = partner.find('div', {'class': 'PartnerTeaser__name'}).text if partner.find('div', {'class': 'PartnerTeaser__name'}) else partner.text

                                info = partner.find('div', {'class': 'PartnerTeaser__content'})
                                if info:
                                    item['company_domain_name'] = get_domain_from_url(info.find('a', {'href': True})['href']) if info.find('a', {'href': True}) else ''
                                    item['specializations'] = list()
                                    sctns = info.find_all('p', recursive=False)
                                    for sctn in sctns:
                                        if 'Partner Type:' in sctn.text:
                                            item['partner_type'] = sctn.text.replace('Partner Type:', '').strip()
                                        elif 'Services Delivery Tier:' in sctn.text:
                                            item['partner_tier'] = sctn.text.replace('Services Delivery Tier:', '').strip()
                                        elif 'Services Delivery Specializations:' in sctn.text:
                                            item['specializations'].append('Services Delivery Partners')
                                        elif 'Industry Specialization:' in sctn.text:
                                            item['specializations'].append('Industry Specialization')
                                        elif sctn.text.strip() != 'Learn more' and sctn.text.strip() != 'Blog Post' and 'Partner Award Winner' not in sctn.text.strip() and 'Partner Testimonial' not in sctn.text.strip():
                                            item['company_description'] = cleanhtml(sctn.text)

                                # handle empty name field
                                if item['partner_company_name'] == '' and item['company_domain_name'] != '':
                                    item['partner_company_name'] = item['company_domain_name'][:item['company_domain_name'].find('.')]

                                item['regions'] = list()

                            if region_label != 'All':
                                item['regions'].append(region_label)
                            self.done_partners[partner_id] = item

                # follow next page
                if soup.find('a', {'title': 'Go to next page'}):
                    page += 1
                else:
                    break

        for item in self.done_partners.values():
            yield item
