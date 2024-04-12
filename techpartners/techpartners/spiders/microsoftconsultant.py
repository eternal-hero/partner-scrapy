# import needed libraries
import json
import time
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from techpartners.functions import *
from bs4 import BeautifulSoup as BS
import urllib.parse
from collections import defaultdict


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'microsoftconsultant'
    partner_program_link = 'https://appsource.microsoft.com/en-us/marketplace/consulting-services/'
    partner_directory = 'Microsoft AppSource Consulting Services'
    partner_program_name = ''
    crawl_id = 1432

    api_link = 'https://appsource.microsoft.com/view/filteredservices?country=US&region=ALL&page='
    partner_api = 'https://appsource.microsoft.com/view/consultingservice/'

    industries = [['industry_mask_2', 1048576, 'Nonprofit & IGO'], ['industry_mask_2', 524288, 'Other - Unsegmented'],
     ['industry_mask_2', 262144, 'Professional Services Office'], ['industry_mask_2', 131072, 'Professional Services'],
     ['industry_mask_2', 65536, 'Real Estate'], ['industry_mask_2', 32768, 'Architecture & Construction'],
     ['industry_mask_2', 16384, 'Partner Professional Services'], ['industry_mask_2', 8192, 'Legal'],
     ['industry_mask_2', 4096, 'Media & Communications'], ['industry_mask_2', 2048, 'Telecommunications'],
     ['industry_mask_2', 1024, 'Media & Entertainment'], ['industry_mask_2', 512, 'Retail & Consumer Goods Office'],
     ['industry_mask_2', 256, 'Retail'], ['industry_mask_2', 128, 'Retail'], ['industry_mask_2', 64, ' Consumer Goods'],
     ['industry_mask_2', 32, 'Manufacturing & Resources Office'], ['industry_mask_2', 16, 'Manufacturing'],
     ['industry_mask_2', 8, 'Agriculture'], ['industry_mask_2', 4, 'Discrete Manufacturing'],
     ['industry_mask_2', 2, 'Process Manufacturing'], ['industry_mask_2', 1, 'Energy'],
     ['industry_mask_1', 1073741824, 'Other - Unsegmented'], ['industry_mask_1', 536870912, 'Hospitality & Travel'],
     ['industry_mask_1', 268435456, 'Restaurants & Food Services'],
     ['industry_mask_1', 134217728, 'Travel & Transportation'], ['industry_mask_1', 67108864, 'Hotels & Leisure'],
     ['industry_mask_1', 33554432, 'Healthcare Office'], ['industry_mask_1', 16777216, 'Healthcare'],
     ['industry_mask_1', 8388608, 'Life Sciences'], ['industry_mask_1', 4194304, 'Health Provider'],
     ['industry_mask_1', 2097152, 'Health Payor'], ['industry_mask_1', 1048576, 'Government Office'],
     ['industry_mask_1', 524288, 'Government'], ['industry_mask_1', 262144, 'Civilian Government'],
     ['industry_mask_1', 131072, 'Public Safety & Justice'], ['industry_mask_1', 65536, 'Financial Services'],
     ['industry_mask_1', 32768, 'Financial Services'], ['industry_mask_1', 16384, 'Capital Markets'],
     ['industry_mask_1', 8192, 'Insurance'], ['industry_mask_1', 4096, 'Banking'],
     ['industry_mask_1', 2048, 'Education Office'], ['industry_mask_1', 1024, 'Education'],
     ['industry_mask_1', 512, 'Libraries & Museums'], ['industry_mask_1', 256, 'Primary & Secondary Education/K-12'],
     ['industry_mask_1', 128, 'Higher Education'], ['industry_mask_1', 64, 'Distribution Office'],
     ['industry_mask_1', 32, 'Distribution'], ['industry_mask_1', 16, 'Parcel & Package Shipping'],
     ['industry_mask_1', 8, 'Wholesale'], ['industry_mask_1', 4, 'DefenseIntelligence'],
     ['industry_mask_1', 2, 'Other - Unsegmented'], ['industry_mask_1', 1, 'Automotive']]
    products = [['product_mask_2', 2097152, 'Power Apps'], ['product_mask_2', 1048576, 'Power Virtual Agents'],
     ['product_mask_2', 524288, 'Power Automate'], ['product_mask_2', 131072, 'Power BI apps'],
     ['product_mask_2', 512, 'Workplace Analytics'], ['product_mask_2', 256, 'Threat Protection'],
     ['product_mask_2', 128, 'Teamwork Deployment'], ['product_mask_2', 64, 'Teams Custom Solutions'],
     ['product_mask_2', 32, 'Power Platform for Teams'], ['product_mask_2', 16, 'Mobile Device Management'],
     ['product_mask_2', 8, 'Microsoft 365 Live Events'], ['product_mask_2', 4, 'Meetings for Microsoft Teams'],
     ['product_mask_2', 2, 'Meeting Rooms for Microsoft Teams'], ['product_mask_2', 1, 'Knowledge & Insights'],
     ['product_mask_1', 1073741824, 'Insider Risk'],
     ['product_mask_1', 536870912, 'Information Protection & Governance'],
     ['product_mask_1', 268435456, 'Identity & Access Management'], ['product_mask_1', 134217728, 'Firstline Workers'],
     ['product_mask_1', 67108864, 'Device Deployment & Management'],
     ['product_mask_1', 33554432, 'Compliance Advisory Services'], ['product_mask_1', 16777216, 'Cloud Security'],
     ['product_mask_1', 8388608, 'Calling for Microsoft Teams'],
     ['product_mask_1', 4194304, 'Adoption & Change Management'], ['product_mask_1', 1048576, 'Microsoft 365'],
     ['product_mask_1', 524288, 'Dynamics 365 Human Resources'],
     ['product_mask_1', 262144, 'Dynamics 365 Mixed Reality'],
     ['product_mask_1', 65536, 'Dynamics 365 Project Operations'],
     ['product_mask_1', 32768, 'Dynamics 365 Customer Voice'],
     ['product_mask_1', 16384, 'Dynamics 365 Customer Insights'],
     ['product_mask_1', 8192, 'Dynamics 365 Project Service Automation'],
     ['product_mask_1', 4096, 'Dynamics 365 Field Service'], ['product_mask_1', 2048, 'Dynamics 365 Customer Service'],
     ['product_mask_1', 1024, 'Dynamics 365 Business Central'], ['product_mask_1', 512, 'Dynamics 365 Sales'],
     ['product_mask_1', 256, 'Dynamics 365 Finance'], ['product_mask_1', 64, 'Dynamics 365 Supply Chain Management'],
     ['product_mask_1', 32, 'Dynamics 365 Commerce'], ['product_mask_1', 16, 'Dynamics 365 Marketing'],
     ['product_mask_1', 8, 'Dynamics 365'], ['product_mask_1', 4, 'Power Platform']]
    serviceTypes = [['serviceType', 32, 'Workshop'], ['serviceType', 8, 'Proof of concept'], ['serviceType', 4, 'Implementation'],
     ['serviceType', 2, 'Briefing'], ['serviceType', 1, 'Assessment']]

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name', 'product/service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'product/service_description',
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
                   'primary_contact_name', 'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified?',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    def start_requests(self):
        """
        landing function of spider class
        will get api results, create the search pages and pass it to parse function
        :return:
        """

        page = 0
        yield scrapy.Request(self.api_link + str(page), callback=self.parse,
                             meta={'page': page})

    def parse(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """

        # soup = BS(response.text, "html.parser")
        # content = soup.find('div', {'class': 'curatedGalleryWrapper'})
        # all_links = content.find_all('a', {'aria-label': re.compile(r"^see\sall\sfor")})
        # for link in all_links:
        #     cat_link = 'https://appsource.microsoft.com' + link['href'] if 'https://appsource.microsoft.com' not in link['href'] else link['href']
        #     print(cat_link)
        # return

        if response.status != 200:
            yield scrapy.Request(response.request.url, callback=self.parse, dont_filter=True, meta=response.meta)
        else:
            data = response.json()

            # parse products
            apps = data['items']

            for app in apps:
                try:
                    _id = app['entityId']
                    partner_link = self.partner_api + _id
                    yield scrapy.Request(partner_link, callback=self.parse_partner,
                                         meta={'trial': 1})
                except:
                    continue

            # follow next page
            if len(apps) > 30:
                next_page = response.meta['page'] + 1
                next_link = self.api_link + str(next_page)
                yield scrapy.Request(next_link, callback=self.parse, meta={'page': next_page})

    def parse_partner(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """

        if response.status == 200:
            partner = response.json()
        elif response.status == 403:
            # self.crawler.engine.pause()
            # print('Engine Paused')
            # time.sleep(10)
            # self.crawler.engine.unpause()
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

        # scripts = soup.find_all('script')
        # for i in scripts:
        #     if 'window.state=' in i.text and 'detailsState' in i.text:
        #         try:
        #         # if True:
        #             content = i.text[i.text.find('=')+1:]
        #             json_object = json.loads(content)

        # Initialize item
        item = dict()
        for k in self.item_fields:
            item[k] = ''

        item['partner_program_link'] = self.partner_program_link
        item['partner_directory'] = self.partner_directory
        item['partner_program_name'] = self.partner_program_name

        item['partner_company_name'] = partner['publisher']
        item['product/service_name'] = partner['title']

        if partner['extraData']['multiCountryPricing'][0]['pricing']:
            currency = partner['extraData']['multiCountryPricing'][0]['pricing']['currencyCode']
            price = partner['extraData']['multiCountryPricing'][0]['pricing']['planPrices'][0]['price']
            item['pricing'] = ' '.join([str(price), currency])

        else:
            item['pricing'] = 'Free'

        item['products'] = list()
        for mask in partner['products'].keys():
            value = partner['products'][mask]
            for i in self.products:
                if mask == i[0] and value >= i[1]:
                    value -= i[1]
                    item['products'].append(i[2])
                    if value == 0:
                        break

        item['industries'] = list()
        for key in partner.keys():
            if key.startswith('industry_mask_'):
                mask = key
                value = partner[key]
                for i in self.industries:
                    if mask == i[0] and value >= i[1]:
                        value -= i[1]
                        item['industries'].append(i[2])
                        if value == 0:
                            break

        desc = ''
        desc += partner['extraData']['shortDescription'] if 'extraData' in partner and 'shortDescription' in partner['extraData'] else ''
        if 'detailInformation' in partner and 'Description' in partner['detailInformation'] and partner['detailInformation']['Description']:
            if desc != '':
                desc = desc + ', ' + partner['detailInformation']['Description']
            else:
                desc = partner['detailInformation']['Description']
        item['product/service_description'] = cleanhtml(desc)

        item['publisher'] = partner['publisher']

        if 'detailInformation' in partner and 'competencies' in partner['detailInformation']:
            item['competencies'] = defaultdict(list)
            for i in partner['detailInformation']['competencies']:
                item['competencies'][i['competencyLevel']].append(i['competencyName'])

            item['competencies'] = dict(item['competencies'])

        item['services'] = list()
        value = partner['serviceTypes']
        for i in self.serviceTypes:
            if value >= i[1]:
                value -= i[1]
                item['services'].append(i[2])
                if value == 0:
                    break

        item['locations_country'] = partner['detailInformation']['countryRegion']
        item['locations_state'] = partner['detailInformation']['regions']
        yield item
