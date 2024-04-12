# import needed libraries
import datetime
import math
import re
import json
import time
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from techpartners.functions import *
from bs4 import BeautifulSoup as BS
import urllib.parse
from collections import defaultdict


us_states = {"ALL":"AllStates", "AL":"Alabama", "AK":"Alaska", "AZ":"Arizona", "AR":"Arkansas", "CA":"California", "CO":"Colorado", "CT":"Connecticut", "DC":"DistrictofColumbia", "DE":"Delaware", "FL":"Florida", "GA":"Georgia", "HI":"Hawaii", "IA":"Iowa", "ID":"Idaho", "IL":"Illinois", "IN":"Indiana", "KS":"Kansas", "KY":"Kentucky", "LA":"Louisiana", "ME":"Maine", "MD":"Maryland", "MA":"Massachusetts", "MI":"Michigan", "MN":"Minnesota", "MS":"Mississippi", "MO":"Missouri", "MT":"Montana", "NE":"Nebraska", "NV":"Nevada", "NH":"NewHampshire", "NJ":"NewJersey", "NM":"NewMexico", "NY":"NewYork", "NC":"NorthCarolina", "ND":"NorthDakota", "OH":"Ohio", "OK":"Oklahoma", "OR":"Oregon", "PA":"Pennsylvania", "RI":"RhodeIsland", "SC":"SouthCarolina", "SD":"SouthDakota", "TN":"Tennessee", "TX":"Texas", "UT":"Utah", "VT":"Vermont", "VA":"Virginia", "WA":"Washington", "WV":"WestVirginia", "WI":"Wisconsin", "WY":"Wyoming"}
ca_states = {"ALL":"AllStates", "AB":"Alberta", "BC":"BritishColumbia", "MB":"Manitoba", "NB":"NewBrunswick", "NL":"NewfoundlandandLabrador", "NS":"NovaScotia", "ON":"Ontario", "PE":"PrinceEdwardIsland", "QC":"Quebec", "SK":"Saskatchewan", "NT":"NorthwestTerritories", "NU":"Nunavut", "YT":"Yukon"}

class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'azureconsult'
    partner_program_link = 'https://azuremarketplace.microsoft.com/en-us/marketplace/consulting-services'
    partner_directory = 'Azure Marketplace'
    partner_program_name = ''
    crawl_id = 1437

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

    cat_data = list()
    serviceTypes_data = list()
    industries_data = list()
    pricing_data = list()
    countries_data = dict()
    pricing_lst = None

    def start_requests(self):
        """
        landing function of spider class
        will get search result length and create the search pages and pass it to parse function
        :return:
        """

        try:
            response = requests.get(self.partner_program_link, headers={'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"})
            soup = BS(response.text, "html.parser")

            scripts = soup.find('head').find_all('script')
            for script in scripts:
                if 'window.__INITIAL_STATE__' in script.text:
                    content = script.text[script.text.find('window.__INITIAL_STATE__'):]
                    content = content[content.find('=') + 1: content.rfind(';')]
                    # print(content.strip())
                    json_object = json.loads(content.strip())

                    categories = json_object['services']['dataMap']['products']
                    for i in categories.keys():
                        itm = categories[i]
                        filter_group = itm['targetProperty']
                        filter_id = itm['targetMask']
                        filter_title = itm['title'].strip()

                        self.cat_data.append([filter_group, filter_id, filter_title])

                    serviceTypes = json_object['services']['dataMap']['serviceTypes']
                    for i in serviceTypes.keys():
                        itm = serviceTypes[i]
                        filter_group = itm['targetProperty']
                        filter_id = itm['targetMask']
                        filter_title = itm['title'].strip()

                        self.serviceTypes_data.append([filter_group, filter_id, filter_title])

                    industries = json_object['services']['dataMap']['industries']
                    for i in industries.keys():
                        itm = industries[i]
                        filter_group = itm['targetProperty']
                        filter_id = itm['targetMask']
                        filter_title = itm['title'].strip()

                        self.industries_data.append([filter_group, filter_id, filter_title])

                    pricingFilter = json_object['services']['dataMap']['servicePricingModel']
                    for i in pricingFilter.keys():
                        itm = pricingFilter[i]
                        filter_group = itm['targetProperty']
                        filter_id = itm['targetMask']
                        filter_title = itm['title'].strip()

                        self.pricing_data.append([filter_group, filter_id, filter_title])

                    countriesFilter = json_object['services']['dataMap']['serviceGlobalFilter1']
                    for i in countriesFilter.keys():
                        for j in countriesFilter[i]['subCategoryDataMapping'].keys():
                            if type(countriesFilter[i]['subCategoryDataMapping'][j]) == dict:
                                itm = countriesFilter[i]['subCategoryDataMapping'][j]
                                self.countries_data[itm['backendKey']] = itm['title']

            self.cat_data = sorted(self.cat_data, key=lambda x: (x[0], x[1]), reverse=True)
            self.serviceTypes_data = sorted(self.serviceTypes_data, key=lambda x: (x[0], x[1]), reverse=True)
            self.industries_data = sorted(self.industries_data, key=lambda x: (x[0], x[1]), reverse=True)
            self.pricing_data = sorted(self.pricing_data, key=lambda x: (x[0], x[1]), reverse=True)

            for cat in categories.keys():
                app_count = categories[cat]['count']
                pages = math.ceil(app_count / 60)
                for pn in range(1, pages+1):
                    print(f'Category: {categories[cat]["title"]}, Page: {pn}')
                    url = f'https://azuremarketplace.microsoft.com/en-us/marketplace/consulting-services/category/{categories[cat]["urlKey"]}?page={pn}'
                    yield scrapy.Request(url, callback=self.parse, dont_filter=True)

        except Exception as e:
            print(e)

    def parse(self, response):
        """
        parse apps page and get json data
        :param response:
        :return:
        """

        if response.status != 200:
            yield scrapy.Request(response.request.url, callback=self.parse, dont_filter=True)
        else:
            # print(response.request.url)

            soup = BS(response.text, "html.parser")
            scripts = soup.find('head').find_all('script')
            for script in scripts:
                if 'window.__INITIAL_STATE__' in script.text:
                    content = script.text[script.text.find('window.__INITIAL_STATE__'):]
                    content = content[content.find('=') + 1: content.rfind(';')]
                    json_object = json.loads(content.strip())

                    if 'services' in json_object:
                        for app in json_object['services']['dataList']:
                            app_link = f'https://azuremarketplace.microsoft.com/en-us/marketplace/consulting-services/{app["entityId"]}'
                            yield scrapy.Request(app_link, callback=self.parse_partner, meta={'trial': 1})

    def parse_partner(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """
        if response.status == 200:
            pass
        elif response.status == 403:
            self.logger.info('*'*60)
            self.logger.info('ERROR PARSE PARTNER')
            self.logger.info(response.status)
            self.logger.info('*' * 60)
            yield scrapy.Request(response.request.url, callback=self.parse_partner, dont_filter=True,
                                 meta={'trial': response.meta['trial']})
            return
        else:
            self.logger.info('*'*60)
            self.logger.info('ERROR PARSE PARTNER')
            self.logger.info(response.status)
            self.logger.info('*' * 60)
            if response.meta['trial'] == 1:
                yield scrapy.Request(response.request.url, callback=self.parse_partner, dont_filter=True,
                                     meta={'trial': 2})
            return

        partner = None
        soup = BS(response.text, "html.parser")
        scripts = soup.find('head').find_all('script')
        for script in scripts:
            if 'window.__INITIAL_STATE__' in script.text:
                try:
                    content = script.text[script.text.find('window.__INITIAL_STATE__'):]
                    content = content[content.find('=') + 1: content.rfind(';')]
                    json_object = json.loads(content.strip())

                    if 'cloudsIndustry' in json_object and 'dataList' in json_object['cloudsIndustry'] and len(json_object['cloudsIndustry']['dataList']) > 0:
                        partner = json_object['cloudsIndustry']['dataList'][0]
                        break
                except Exception as e:
                    print('ERROR: ', e)
        else:
            return

        # Initialize item
        item = dict()
        for k in self.item_fields:
            item[k] = ''

        _id = partner['entityId']
        item['partner_program_link'] = self.partner_program_link
        item['partner_directory'] = self.partner_directory
        item['partner_program_name'] = self.partner_program_name

        item['product_service_name'] = partner['title'] if 'title' in partner else ''
        item['partner_company_name'] = partner['publisher'] if 'publisher' in partner else ''
        item['publisher'] = partner['publisher'] if 'publisher' in partner else ''
        item['product_service_description'] = ' '.join([(cleanhtml(partner['extraData']['shortDescription']) if 'extraData' in partner and 'shortDescription' in partner['extraData'] else ''), (cleanhtml(partner['detailInformation']['Description']) if 'detailInformation' in partner and 'Description' in partner['detailInformation'] else '')]).strip()

        item['solutions'] = list()
        if 'products' in partner:
            mask = 'products'
            value = partner[mask]
            for i in self.cat_data:
                if mask == i[0] and value >= i[1]:
                    value -= i[1]
                    item['solutions'].append(i[2])
                    if value == 0:
                        break

        item['services'] = list()
        if 'serviceTypes' in partner:
            mask = 'serviceTypes'
            value = partner[mask]
            for i in self.serviceTypes_data:
                if mask == i[0] and value >= i[1]:
                    value -= i[1]
                    item['services'].append(i[2])
                    if value == 0:
                        break

        if 'servicePricingModel' in partner:
            mask = 'servicePricingModel'
            value = partner[mask]
            for i in self.pricing_data:
                if mask == i[0] and value >= i[1]:
                    value -= i[1]
                    item['pricing'] = i[2]
                    break

        item['industries'] = list()
        if 'industries' in partner:
            mask = 'industries'
            value = partner[mask]
            for i in self.industries_data:
                if mask == i[0] and value >= i[1]:
                    value -= i[1]
                    item['industries'].append(i[2])
                    if value == 0:
                        break

        if 'detailInformation' in partner and 'competencies' in partner['detailInformation']:
            item['competencies'] = defaultdict(list)
            for i in partner['detailInformation']['competencies']:
                item['competencies'][i['competencyLevel']].append(i['competencyName'])

            item['competencies'] = dict(item['competencies'])

        if 'detailInformation' in partner and 'MultiCountryRegions' in partner['detailInformation']:
            for country in partner['detailInformation']['MultiCountryRegions']:
                if country['countryRegion'] in self.countries_data:
                    item['locations_country'] = self.countries_data[country['countryRegion']]
                else:
                    item['locations_country'] = country['countryRegion']

                if 'regions' in country:
                    item['locations_state'] = list()
                    for s in country['regions']:
                        if country['countryRegion'] == 'CA':
                            item['locations_state'].append(ca_states[s])
                        elif country['countryRegion'] == 'US':
                            item['locations_state'].append(us_states[s])
                        else:
                            item['locations_state'].append(s)

                if 'servicePricingModel' in partner and partner['servicePricingModel'] == 2 and 'extraData' in partner and 'multiCountryPricing' in partner['extraData']:
                    for c in partner['extraData']['multiCountryPricing']:
                        if c['countryRegion'] == country['countryRegion'] and 'pricing' in c and 'planPrices' in c['pricing'] and len(c['pricing']['planPrices']) > 0 and item['pricing'] != 'Free':
                            # item['pricing_model'] = c['pricing']['currencyCode']
                            item['pricing'] = str(c['pricing']['planPrices'][0]['price']) + ' ' + c['pricing']['currencyCode']
                yield item
        elif 'detailInformation' in partner and 'countryRegion' in partner['detailInformation']:
            item['locations_country'] = self.countries_data[partner['detailInformation']['countryRegion']]
            if 'regions' in partner['detailInformation']:
                item['locations_state'] = list()
                for s in partner['detailInformation']['regions']:
                    if partner['detailInformation']['countryRegion'] == 'CA':
                        item['locations_state'].append(ca_states[s])
                    elif partner['detailInformation']['countryRegion'] == 'US':
                        item['locations_state'].append(us_states[s])
                    else:
                        item['locations_state'].append(s)
            yield item
        else:
            yield item