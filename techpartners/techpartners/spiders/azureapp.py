# import needed libraries
import datetime
import math
import random
import re
import json
import time
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from techpartners.functions import *
from bs4 import BeautifulSoup as BS
import urllib.parse

with open('proxy_25000.txt', 'r') as ifile:
    Ips = [x.strip() for x in ifile.readlines()]


def get_proxy():
    proxy_index = random.randint(0, len(Ips) - 1)
    return {"http": "http://" + Ips[proxy_index], "https": "http://" + Ips[proxy_index]}


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'azureapp'
    partner_program_link = 'https://azuremarketplace.microsoft.com/en-us/marketplace/apps'
    partner_directory = 'Azure Marketplace'
    partner_program_name = ''
    crawl_id = 1436

    custom_settings = {
        # 'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 4,
        'CONCURRENT_REQUESTS_PER_IP': 1,
        'DOWNLOADER_MIDDLEWARES': {
        'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
        'rotating_proxies.middlewares.BanDetectionMiddleware': 620},
        'ROTATING_PROXY_LIST_PATH': 'proxy_25000.txt',
        'ROTATING_PROXY_PAGE_RETRY_TIMES': 5,
    }

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

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        'Authority': 'azuremarketplace.microsoft.com',
        'Accept': '*/*',
        'content-type': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/json',
        'Sec-Ch-Ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'Sec-Ch-Ua-mobile': '?0',
        'Sec-Ch-Ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Referer': f'https://azuremarketplace.microsoft.com/en-us/marketplace/apps/',
        # 'x-ms-client-name': 'AMP',
        # 'x-ms-correlationid': '743d1985-986d-4951-8012-67d0927d1140',
        # 'x-ms-requestid': '3bed3c78-f07f-4d6f-997d-d730522eb9b8',
    }

    cat_data = list()
    product_data = list()
    pricing_data = list()
    requirements_data = list()

    pricing_lst = None

    def start_requests(self):
        """
        landing function of spider class
        will get search result length and create the search pages and pass it to parse function
        :return:
        """

        categories = dict()

        try:
            response = requests.get(self.partner_program_link,
                                    proxies=get_proxy(),
                                    headers=self.headers)
            if response.status_code != 200:
                self.logger.info('ERROR START REQUEST "can not get initial data"')
                return

            json_object = None
            soup = BS(response.text, "html.parser")
            scripts = soup.find('head').find_all('script')

            for script in scripts:
                try:
                    if 'window.__INITIAL_STATE__' in script.text and 'services' in script.text and 'apps' in script.text:
                        content = script.text[script.text.find('window.__INITIAL_STATE__'):]
                        content = content[content.find('=') + 1:]
                        while ';' in content:
                            content = content[: content.rfind(';')]
                            try:
                                json_object = json.loads(content.strip())
                                break
                            except json.decoder.JSONDecodeError:
                                continue
                        if json_object:
                            break
                except:
                    continue
            if json_object:
                categories = json_object['apps']['dataMap']['category']
                for i in categories.keys():
                    for j in categories[i]['subCategoryDataMapping'].keys():
                        if type(categories[i]['subCategoryDataMapping'][j]) == dict:
                            itm = categories[i]['subCategoryDataMapping'][j]
                            filter_group = itm['targetProperty']
                            filter_id = itm['targetMask']
                            filter_title = itm['title']
                            filter_title = categories[i]['title'] if filter_title.startswith('All') else filter_title
                            filter_title = filter_title.strip()

                            self.cat_data.append([filter_group, filter_id, filter_title])

                pricingFilter = json_object['apps']['dataMap']['globalFilter1']
                for i in pricingFilter.keys():
                    for j in pricingFilter[i]['subCategoryDataMapping'].keys():
                        if type(pricingFilter[i]['subCategoryDataMapping'][j]) == dict:
                            itm = pricingFilter[i]['subCategoryDataMapping'][j]
                            filter_group = itm['targetProperty']
                            filter_id = itm['targetMask']
                            filter_title = itm['title']
                            filter_title = pricingFilter[i]['title'] if filter_title.startswith('All') else filter_title
                            filter_title = filter_title.strip()

                            self.pricing_data.append([filter_group, filter_id, filter_title])

                requirementsFilter = json_object['apps']['dataMap']['globalFilter3']
                for i in requirementsFilter.keys():
                    for j in requirementsFilter[i]['subCategoryDataMapping'].keys():
                        if type(requirementsFilter[i]['subCategoryDataMapping'][j]) == dict:
                            itm = requirementsFilter[i]['subCategoryDataMapping'][j]
                            filter_group = itm['targetProperty']
                            filter_id = itm['targetMask']
                            filter_title = itm['title']
                            filter_title = requirementsFilter[i]['title'] if filter_title.startswith('All') else filter_title
                            filter_title = filter_title.strip()

                            self.requirements_data.append([filter_group, filter_id, filter_title])

                products = json_object['apps']['dataMap']['globalFilter5']
                for i in products.keys():
                    for j in products[i]['subCategoryDataMapping'].keys():
                        if type(products[i]['subCategoryDataMapping'][j]) == dict:
                            itm = products[i]['subCategoryDataMapping'][j]
                            filter_group = itm['targetProperty']
                            filter_id = itm['targetMask']
                            filter_title = itm['title']
                            filter_title = products[i]['title'] if filter_title.startswith('All') else filter_title
                            filter_title = filter_title.strip()

                            self.product_data.append([filter_group, filter_id, filter_title])

            self.cat_data = sorted(self.cat_data, key=lambda x: (x[0], x[1]), reverse=True)
            self.pricing_data = sorted(self.pricing_data, key=lambda x: (x[0], x[1]), reverse=True)
            self.requirements_data = sorted(self.requirements_data, key=lambda x: (x[0], x[1]), reverse=True)
            self.product_data = sorted(self.product_data, key=lambda x: (x[0], x[1]), reverse=True)

            pricing_response = requests.get('https://azuremarketplace.microsoft.com/view/appPricing/us?version=2017-04-24',
                                            proxies=get_proxy(),
                                            headers=self.headers)
            if pricing_response.status_code == 200:
                price_data = json.loads(pricing_response.text)
                if 'startingPrices' in price_data:
                    self.pricing_lst = price_data['startingPrices']

            else:
                self.logger.info(f'ERROR: GETTING PRICE DATA, STATUS {response.status_code}, RESPONSE {response.text}')
                return

            for cat in categories.keys():
                app_count = categories[cat]['count']
                pages = math.ceil(app_count / 60)
                for pn in range(1, pages+1):
                    headers = self.headers.copy()
                    headers['Referer'] = f'https://azuremarketplace.microsoft.com/en-us/marketplace/apps/category/{categories[cat]["urlKey"]}?page={pn}'

                    # url = f'https://azuremarketplace.microsoft.com/en-us/marketplace/apps/category/{categories[cat]["urlKey"]}?page={pn}&subcategories=all'
                    url = f'https://azuremarketplace.microsoft.com/view/tiledata/?ReviewsMyCommentsFilter=true&category={categories[cat]["urlKey"]}&country=US&entityType=All&page={pn}&region=ALL&subcategories=all'
                    yield scrapy.Request(url, callback=self.parse,
                                         dont_filter=True, headers=headers,
                                         meta={'message': f'INFO Category: {categories[cat]["title"]},\tPage: {pn} of {pages}'})

        except Exception as e:
            print(e)

    def parse(self, response):
        """
        parse apps page and get json data
        :param response:
        :return:
        """

        if response.status != 200:
            self.logger.info(f'ERROR PARSE: <{response.status}>, REQUEST META {response.meta["message"]},\nURL: {response.request.url}')
            yield scrapy.Request(response.request.url, callback=self.parse, meta=response.meta, dont_filter=True)
        else:
            try:
                json_object = json.loads(response.text.strip())
            except:
                json_object = None

            # soup = BS(response.text, "html.parser")
            # scripts = soup.find('head').find_all('script')
            # for script in scripts:
            #     if 'window.__INITIAL_STATE__' in script.text:
            #         content = script.text[script.text.find('window.__INITIAL_STATE__'):]
            #         content = content[content.find('=') + 1:]
            #         while ';' in content:
            #             content = content[: content.rfind(';')]
            #             try:
            #                 json_object = json.loads(content.strip())
            #                 break
            #             except json.decoder.JSONDecodeError:
            #                 continue
            #
            #         if json_object:
            #             break

            if json_object and 'apps' in json_object:
                self.logger.info((response.meta['message'] + f' Page Apps = {len(json_object["apps"]["dataList"])}'))
                for app in json_object['apps']['dataList']:
                    app_link = f'https://azuremarketplace.microsoft.com/view/app/{app["entityId"]}?billingregion=us&version=2017-04-24'

                    yield scrapy.Request(app_link, callback=self.parse_partner, dont_filter=True,
                                         meta={'trial': 1}, headers=self.headers)

    def parse_partner(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """

        if response.status != 200:
            trial = response.meta['trial']
            if trial < 4:
                self.logger.info(f'ERROR: TRIAL #{trial} PARSE PARTNER - STATUS {response.status}, Page: {response.request.url}')
                yield scrapy.Request(response.request.url, callback=self.parse_partner, dont_filter=True,
                                     meta={'trial': trial+1}, headers=self.headers)
            else:
                self.logger.info(f'ERROR: GAVE UP PARSE PARTNER - STATUS {response.status}, Page: {response.request.url}')

            return

        # Initialize item
        item = dict()
        for k in self.item_fields:
            item[k] = ''

        partner = response.json()

        _id = partner['entityId']

        item['partner_program_link'] = self.partner_program_link
        item['partner_directory'] = self.partner_directory
        item['partner_program_name'] = self.partner_program_name

        item['partner_company_name'] = partner['publisher'] if 'publisher' in partner else ''
        item['product_service_name'] = partner['title'] if 'title' in partner else ''
        item['publisher'] = partner['publisher'] if 'publisher' in partner else ''
        item['product_service_description'] = cleanhtml(' '.join([(partner['shortDescription'] if 'shortDescription' in partner else ''), (partner['Description'] if 'Description' in partner else '')]).strip())
        item['company_domain_name'] = get_domain_from_url(partner['detailInformation']['SupportLink']) if 'detailInformation' in partner and 'SupportLink' in partner['detailInformation'] and partner['detailInformation']['SupportLink'] else ''

        item['products'] = list()
        if 'primaryProduct' in partner:
            for key in partner.keys():
                if key.startswith('globalFilter5_mask_'):
                    mask = key
                    value = partner[key]
                    for i in self.product_data:
                        if mask == i[0] and value >= i[1]:
                            value -= i[1]
                            item['products'].append(i[2])
                            if value == 0:
                                break

        item['categories'] = list()
        for key in partner.keys():
            if key.startswith('category_mask_'):
                mask = key
                value = partner[key]
                for i in self.cat_data:
                    if mask == i[0] and value >= i[1]:
                        value -= i[1]
                        item['categories'].append(i[2])
                        if value == 0:
                            break

        item['product_requirements'] = list()
        for key in partner.keys():
            if key.startswith('globalFilter3_mask_'):
                mask = key
                value = partner[key]
                for i in self.requirements_data:
                    if mask == i[0] and value >= i[1]:
                        value -= i[1]
                        item['product_requirements'].append(i[2])
                        if value == 0:
                            break

        item['product_version'] = partner['detailInformation']['AppVersion'] if 'detailInformation' in partner and 'AppVersion' in partner['detailInformation'] else ''
        item['support_link'] = partner['detailInformation']['SupportLink'] if 'detailInformation' in partner and 'SupportLink' in partner['detailInformation'] and partner['detailInformation']['SupportLink'] else ''
        item['help_link'] = partner['detailInformation']['HelpLink'] if 'detailInformation' in partner and 'HelpLink' in partner['detailInformation'] else ''
        item['privacy_policy_link'] = partner['detailInformation']['PrivacyPolicyUrl'] if 'detailInformation' in partner and 'PrivacyPolicyUrl' in partner['detailInformation'] else ''
        item['license_agreement_link'] = partner['licenseTermsUrl'] if 'licenseTermsUrl' in partner else ''

        item['languages'] = partner['detailInformation']['LanguagesSupported']

        price_dict = self.pricing_lst[_id] if _id in self.pricing_lst and self.pricing_lst[_id] else dict()

        if 'pricingBitmask' not in price_dict and 'pricingData' in price_dict and price_dict['pricingData'] == 8:
            item['pricing'] = 'Prices Varies'
        elif 'pricingBitmask' in price_dict:
            item['pricing_plan_description'] = list()
            for key in price_dict['pricingBitmask'].keys():
                if key.startswith('globalFilter1_mask_'):
                    mask = key
                    value = price_dict['pricingBitmask'][key]
                    for i in self.pricing_data:
                        if mask == i[0] and value >= i[1]:
                            value -= i[1]
                            item['pricing_plan_description'].append(i[2])
                            if value == 0:
                                break

        if 'pricingData' in price_dict and type(price_dict['pricingData']) == dict and item['pricing'] != 'Prices Varies':
            item['pricing_plan'] = price_dict['pricingData']['unit']
            item['pricing_model'] = price_dict['pricingData']['currency']
            item['pricing'] = price_dict['pricingData']['value']

        if 'Free' in item['pricing_plan_description'] and item['pricing'] == 0:
            item['pricing'] = 'Free'
        elif 'Bring your own license' in item['pricing_plan_description'] and (item['pricing'] == 0 or item['pricing'] == ''):
            item['pricing'] = 'Bring your own license'
        elif item['pricing'] != '' and item['pricing_plan'] != '' and item['pricing'] != 'Prices Varies':
            item['pricing'] = f"${item['pricing']}/{item['pricing_plan']}"

        item['pricing_plan'] = ''
        item['pricing_model'] = ''
        item['pricing_plan_description'] = ''

        yield item
