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
    name = 'fidelispartner'
    partner_program_link = 'https://fidelissecurity.com/partners/'
    partner_directory = 'Fidelissecurity Partners Directory'
    partner_program_name = ''
    crawl_id = None

    start_urls = [partner_program_link]
    api_link = 'https://fidelissecurity.com/wp-admin/admin-ajax.php'

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
                   'primary_contact_name', 'primary_contact_phone_number', 'primary_contact_email',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts',
                   'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Connection': 'keep-alive',
        'Authority': 'fidelissecurity.com',
        'Origin': 'https://fidelissecurity.com',
        'Referer': 'https://fidelissecurity.com/partners/',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'x-requested-with': 'XMLHttpRequest',
    }

    def parse(self, response):
        soup = None
        page_number = response.meta['page_number'] if 'page_number' in response.meta else 1

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            if page_number == 1:
                soup = BS(response.text, "html.parser")
            else:
                data = json.loads(response.text)
                if 'content' in data:
                    soup = BS(data['content'], "html.parser")

            if soup:
                partners = soup.find_all('div', {'data-post-id': True})
                if len(partners) > 0:
                    self.logger.info(f'Page = {page_number}, Partners = {len(partners)}')
                    for partner in partners:
                        _id = partner['data-post-id']
                        payload = f'action=jet_popup_get_content&data%5BforceLoad%5D=true&data%5BcustomContent%5D=&data%5BpopupId%5D=jet-popup-58043&data%5BisJetEngine%5D=true&data%5BlistingSource%5D=posts&data%5BpostId%5D={_id}&data%5Bpopup_id%5D=58043'
                        yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse_partner,
                                             body=payload,
                                             headers=self.headers, dont_filter=True)

                # follow next pages
                if len(partners) == 20:
                    headers = self.headers.copy()
                    headers.update({
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Referer': f'https://fidelissecurity.com/partners/jsf/jet-engine:partner-container/pagenum/{page_number}/',
                    })
                    payload = f'action=jet_smart_filters&provider=jet-engine%2Fpartner-container&settings%5Blisitng_id%5D=58018&settings%5Bcolumns%5D=4&settings%5Bcolumns_tablet%5D=2&settings%5Bcolumns_mobile%5D=1&settings%5Bis_archive_template%5D=&settings%5Bpost_status%5D%5B%5D=publish&settings%5Buse_random_posts_num%5D=&settings%5Bposts_num%5D=16&settings%5Bmax_posts_num%5D=9&settings%5Bnot_found_message%5D=No+data+was+found&settings%5Bis_masonry%5D=&settings%5Bequal_columns_height%5D=&settings%5Buse_load_more%5D=&settings%5Bload_more_id%5D=&settings%5Bload_more_type%5D=click&settings%5Bloader_text%5D=&settings%5Bloader_spinner%5D=&settings%5Buse_custom_post_types%5D=&settings%5Bcustom_post_types%5D=&settings%5Bhide_widget_if%5D=&settings%5Bcarousel_enabled%5D=&settings%5Bslides_to_scroll%5D=1&settings%5Barrows%5D=true&settings%5Barrow_icon%5D=fa+fa-angle-left&settings%5Bdots%5D=&settings%5Bautoplay%5D=true&settings%5Bautoplay_speed%5D=5000&settings%5Binfinite%5D=true&settings%5Bcenter_mode%5D=&settings%5Beffect%5D=slide&settings%5Bspeed%5D=500&settings%5Binject_alternative_items%5D=&settings%5Bscroll_slider_enabled%5D=&settings%5Bscroll_slider_on%5D%5B%5D=desktop&settings%5Bscroll_slider_on%5D%5B%5D=tablet&settings%5Bscroll_slider_on%5D%5B%5D=mobile&settings%5Bcustom_query%5D=yes&settings%5Bcustom_query_id%5D=7&settings%5B_element_id%5D=partner-container&settings%5Bjet_cct_query%5D=&props%5Bfound_posts%5D=105&props%5Bmax_num_pages%5D=6&props%5Bpage%5D={page_number}&props%5Bquery_type%5D=posts&props%5Bquery_id%5D=7&paged={page_number+1}'
                    yield scrapy.Request(method='POST', url=self.api_link,
                                         callback=self.parse, body=payload,
                                         headers=headers, dont_filter=True, meta={'page_number': page_number+1})

    def parse_partner(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            data = json.loads(response.text)
            if 'content' in data:
                soup = BS(data['content'], "html.parser")

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = ''

                item['partner_company_name'] = soup.find('h3').text
                item['company_domain_name'] = get_domain_from_url(soup.find('a', {'class': 'elementor-button-link', 'href': True})['href']) if soup.find('a', {'class': 'elementor-button-link', 'href': True}) else ''
                item['company_description'] = ' '.join([cleanhtml(p.text) for p in soup.find_all('p')])
                info = soup.find_all('div', {'class': 'jet-listing jet-listing-dynamic-terms'})
                for i in info:
                    if i.find('span', {'class': 'jet-listing-dynamic-terms__prefix'}) and i.find('span', {'class': 'jet-listing-dynamic-terms__link'}):
                        if 'Type' in i.find('span', {'class': 'jet-listing-dynamic-terms__prefix'}).text:
                            item['partner_type'] = i.find('span', {'class': 'jet-listing-dynamic-terms__link'}).text

                        if 'Region' in i.find('span', {'class': 'jet-listing-dynamic-terms__prefix'}).text:
                            item['regions'] = i.find('span', {'class': 'jet-listing-dynamic-terms__link'}).text

                        if 'Country' in i.find('span', {'class': 'jet-listing-dynamic-terms__prefix'}).text:
                            item['headquarters_country'] = i.find('span', {'class': 'jet-listing-dynamic-terms__link'}).text

                yield item
