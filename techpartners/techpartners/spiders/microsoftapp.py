# import needed libraries
import datetime
import re
import json
import time
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from techpartners.functions import *
from bs4 import BeautifulSoup as BS
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'microsoftapp'
    partner_program_link = 'https://appsource.microsoft.com/en-us/marketplace/apps'
    partner_directory = 'Microsoft AppSource Apps'
    partner_program_name = ''
    crawl_id = 1431

    api_link = "https://appsource.microsoft.com/view/app/"

    pricing_data = None
    start_urls = ["https://appsource.microsoft.com/view/appPricing/us"]

    custom_settings = {
        'CONCURRENT_REQUESTS': 50,
        'CONCURRENT_REQUESTS_PER_IP': 1,
        'DOWNLOADER_MIDDLEWARES': {
        'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
        'rotating_proxies.middlewares.BanDetectionMiddleware': 620},
        'ROTATING_PROXY_LIST_PATH': 'proxy_25000.txt',
        'ROTATING_PROXY_PAGE_RETRY_TIMES': 5,
    }

    categories = [['category_mask_1', 1, 'Advanced Analytics'], ['category_mask_1', 2, 'Dashboard & Data Visualization'], ['category_mask_1', 4, 'Analytics'], ['category_mask_1', 8, 'Analytics_office'], ['category_mask_1', 16, 'AI for Business'], ['category_mask_1', 32, 'Bot Apps'], ['category_mask_1', 64, 'AI + Machine Learning'], ['category_mask_1', 128, 'Contact Management'], ['category_mask_1', 256, 'Chat'], ['category_mask_1', 512, 'Meeting & Calendar Management'], ['category_mask_1', 1024, 'Site Design & Management'], ['category_mask_1', 2048, 'Voice & Video Conferencing'], ['category_mask_1', 4096, 'Collaboration'], ['category_mask_1', 8192, 'Collaboration office'], ['category_mask_1', 16384, 'Project Sales Proposals & Bids'], ['category_mask_1', 32768, 'Project Planning & Tracking'], ['category_mask_1', 65536, 'Project Resource Planning & Utilization Metrics'], ['category_mask_1', 131072, 'Project Time & Expense Reporting'], ['category_mask_1', 262144, 'Project Invoicing'], ['category_mask_1', 524288, 'Project Accounting & Revenue Recognition'], ['category_mask_1', 1048576, 'Project Management'], ['category_mask_1', 2097152, 'Project Management office'], ['category_mask_1', 4194304, 'Tax & Audit'], ['category_mask_1', 8388608, 'Legal'], ['category_mask_1', 16777216, 'Data, Governance & Privacy'], ['category_mask_1', 33554432, 'Health & Safety'], ['category_mask_1', 67108864, 'Compliance & Legal'], ['category_mask_1', 134217728, 'Contact Center'], ['category_mask_1', 268435456, 'Face to Face Service'], ['category_mask_1', 536870912, 'Back Office & Employee Service'], ['category_mask_1', 1073741824, 'Knowledge & Case Management'], ['category_mask_2', 1, 'Social Media & Omnichannel Engagement'], ['category_mask_2', 2, 'Customer Service'], ['category_mask_2', 4, 'Customer Service Office'], ['category_mask_2', 8, 'Accounting'], ['category_mask_2', 16, 'Asset Management'], ['category_mask_2', 32, 'Analytics, Consolidation & Reporting'], ['category_mask_2', 64, 'Payments, Credit & Collections'], ['category_mask_2', 128, 'Compliance & Risk Management'], ['category_mask_2', 256, 'Finance'], ['category_mask_2', 512, 'Finance office'], ['category_mask_2', 1024, 'Maps'], ['category_mask_2', 2048, 'News & Weather'], ['category_mask_2', 4096, 'Address Validation'], ['category_mask_2', 8192, 'Geolocation'], ['category_mask_2', 16384, 'Talent Acquisition'], ['category_mask_2', 32768, 'Talent Management'], ['category_mask_2', 65536, 'HR Operations'], ['category_mask_2', 131072, 'People Analytics & Insights'], ['category_mask_2', 262144, 'Human Resources'], ['category_mask_2', 524288, 'Human Resources office'], ['category_mask_2', 1048576, 'Asset Management & Operations'], ['category_mask_2', 2097152, 'Connected Products'], ['category_mask_2', 4194304, 'Intelligent Supply Chain'], ['category_mask_2', 8388608, 'Predictive Maintenance'], ['category_mask_2', 16777216, 'Remote Monitoring'], ['category_mask_2', 33554432, 'Safety & Security'], ['category_mask_2', 67108864, 'Smart Infrastructure & Resources'], ['category_mask_2', 134217728, 'Vehicles & Mobility'], ['category_mask_2', 268435456, 'Internet of Things'], ['category_mask_2', 536870912, 'Management Solutions'], ['category_mask_2', 1073741824, 'Business Applications'], ['category_mask_3', 1, 'IT & Management Tools'], ['category_mask_3', 2, 'IT & Management Tools office'], ['category_mask_3', 4, 'E-Commerce, Social Commerce & Marketplaces'], ['category_mask_3', 8, 'Store Management'], ['category_mask_3', 16, 'Loyalty & Gifting'], ['category_mask_3', 32, 'Product Information & Content Management'], ['category_mask_3', 64, 'Personalization, Ratings & Reviews'], ['category_mask_3', 128, 'Subscription & Post Purchase Services'], ['category_mask_3', 256, 'Commerce'], ['category_mask_3', 512, 'Advertisement'], ['category_mask_3', 1024, 'Analytics'], ['category_mask_3', 2048, 'Campaign Management & Automation'], ['category_mask_3', 4096, 'Email Marketing'], ['category_mask_3', 8192, 'Events & Resource Management'], ['category_mask_3', 16384, 'Research & Analysis'], ['category_mask_3', 32768, 'Social Media'], ['category_mask_3', 65536, 'Pricing Optimization'], ['category_mask_3', 131072, 'Marketing'], ['category_mask_3', 262144, 'Marketing office'], ['category_mask_3', 524288, 'Asset & Production Management'], ['category_mask_3', 1048576, 'Demand Forecasting'], ['category_mask_3', 2097152, 'Information Management & Connectivity'], ['category_mask_3', 4194304, 'Planning, Purchasing & Reporting'], ['category_mask_3', 8388608, 'Quality & Service Management'], ['category_mask_3', 16777216, 'Sales & Order Management'], ['category_mask_3', 33554432, 'Transportation & Warehouse Management'], ['category_mask_3', 67108864, 'Operations & Supply Chain'], ['category_mask_3', 134217728, 'Operations & Supply Chain office'], ['category_mask_3', 268435456, 'Content Creation & Management'], ['category_mask_3', 536870912, 'Language & Translation'], ['category_mask_3', 1073741824, 'Document & File Management'], ['category_mask_4', 1, 'Workflow Automation'], ['category_mask_4', 2, 'Blogs'], ['category_mask_4', 4, 'Email Management'], ['category_mask_4', 8, 'Search & Reference'], ['category_mask_4', 16, 'Gamification'], ['category_mask_4', 32, 'Productivity'], ['category_mask_4', 64, 'Productivity office'], ['category_mask_4', 128, 'Telesales'], ['category_mask_4', 256, 'Configure, Price, Quote (CPQ)'], ['category_mask_4', 512, 'Contract Management'], ['category_mask_4', 1024, 'CRM'], ['category_mask_4', 2048, 'Business Data Management'], ['category_mask_4', 4096, 'Sales Enablement'], ['category_mask_4', 8192, 'Sales'], ['category_mask_4', 16384, 'Sales office']]
    products = [['product_mask_2', 4194304, 'Microsoft 365 Apps'], ['product_mask_2', 2097152, 'Power Apps'], ['product_mask_2', 1048576, 'Power Virtual Agents'], ['product_mask_2', 524288, 'Power Automate'], ['product_mask_2', 262144, 'Power BI visuals'], ['product_mask_2', 131072, 'Power BI apps'], ['product_mask_2', 65536, 'Word'], ['product_mask_2', 32768, 'Teams'], ['product_mask_2', 16384, 'SharePoint'], ['product_mask_2', 8192, 'Project'], ['product_mask_2', 4096, 'PowerPoint'], ['product_mask_2', 2048, 'Outlook'], ['product_mask_2', 1024, 'OneNote'], ['product_mask_1', 2097152, 'Excel'], ['product_mask_1', 1048576, 'Microsoft 365'], ['product_mask_1', 524288, 'Dynamics 365 Human Resources'], ['product_mask_1', 262144, 'Dynamics 365 Mixed Reality'], ['product_mask_1', 65536, 'Dynamics 365 Project Operations'], ['product_mask_1', 32768, 'Dynamics 365 Customer Voice'], ['product_mask_1', 8192, 'Dynamics 365 Project Service Automation'], ['product_mask_1', 4096, 'Dynamics 365 Field Service'], ['product_mask_1', 2048, 'Dynamics 365 Customer Service'], ['product_mask_1', 1024, 'Dynamics 365 Business Central'], ['product_mask_1', 512, 'Dynamics 365 Sales'], ['product_mask_1', 256, 'Dynamics 365 Finance'], ['product_mask_1', 128, 'Dynamics 365 Operations'], ['product_mask_1', 64, 'Dynamics 365 Supply Chain Management'], ['product_mask_1', 32, 'Dynamics 365 Commerce'], ['product_mask_1', 16, 'Dynamics 365 Marketing'], ['product_mask_1', 8, 'Dynamics 365'], ['product_mask_1', 4, 'Power Platform'], ['product_mask_1', 2, 'Web apps'], ['product_mask_1', 1, 'Cloud Solutions']]
    industries = [['industry_mask_2', 1048576, 'Nonprofit & IGO'], ['industry_mask_2', 524288, 'Other - Unsegmented'], ['industry_mask_2', 262144, 'Professional Services Office'], ['industry_mask_2', 131072, 'Professional Services'], ['industry_mask_2', 65536, 'Real Estate'], ['industry_mask_2', 32768, 'Architecture & Construction'], ['industry_mask_2', 16384, 'Partner Professional Services'], ['industry_mask_2', 8192, 'Legal'], ['industry_mask_2', 4096, 'Media & Communications'], ['industry_mask_2', 2048, 'Telecommunications'], ['industry_mask_2', 1024, 'Media & Entertainment'], ['industry_mask_2', 512, 'Retail & Consumer Goods Office'], ['industry_mask_2', 256, 'Retail'], ['industry_mask_2', 128, 'Retail'], ['industry_mask_2', 64, ' Consumer Goods'], ['industry_mask_2', 32, 'Manufacturing & Resources Office'], ['industry_mask_2', 16, 'Manufacturing'], ['industry_mask_2', 8, 'Agriculture'], ['industry_mask_2', 4, 'Discrete Manufacturing'], ['industry_mask_2', 2, 'Process Manufacturing'], ['industry_mask_2', 1, 'Energy'], ['industry_mask_1', 1073741824, 'Other - Unsegmented'], ['industry_mask_1', 536870912, 'Hospitality & Travel'], ['industry_mask_1', 268435456, 'Restaurants & Food Services'], ['industry_mask_1', 134217728, 'Travel & Transportation'], ['industry_mask_1', 67108864, 'Hotels & Leisure'], ['industry_mask_1', 33554432, 'Healthcare Office'], ['industry_mask_1', 16777216, 'Healthcare'], ['industry_mask_1', 8388608, 'Life Sciences'], ['industry_mask_1', 4194304, 'Health Provider'], ['industry_mask_1', 2097152, 'Health Payor'], ['industry_mask_1', 1048576, 'Government Office'], ['industry_mask_1', 524288, 'Government'], ['industry_mask_1', 262144, 'Civilian Government'], ['industry_mask_1', 131072, 'Public Safety & Justice'], ['industry_mask_1', 65536, 'Financial Services'], ['industry_mask_1', 32768, 'Financial Services'], ['industry_mask_1', 16384, 'Capital Markets'], ['industry_mask_1', 8192, 'Insurance'], ['industry_mask_1', 4096, 'Banking'], ['industry_mask_1', 2048, 'Education Office'], ['industry_mask_1', 1024, 'Education'], ['industry_mask_1', 512, 'Libraries & Museums'], ['industry_mask_1', 256, 'Primary & Secondary Education/K-12'], ['industry_mask_1', 128, 'Higher Education'], ['industry_mask_1', 64, 'Distribution Office'], ['industry_mask_1', 32, 'Distribution'], ['industry_mask_1', 16, 'Parcel & Package Shipping'], ['industry_mask_1', 8, 'Wholesale'], ['industry_mask_1', 4, 'DefenseIntelligence'], ['industry_mask_1', 2, 'Other - Unsegmented'], ['industry_mask_1', 1, 'Automotive']]

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

    def parse(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """

        # content = soup.find('div', {'class': 'curatedGalleryWrapper'})
        #
        # all_links = content.find_all('a', {'aria-label': re.compile(r"^see\sall\sfor")})
        # for link in all_links:
        #     cat_link = 'https://appsource.microsoft.com' + link['href'] if 'https://appsource.microsoft.com' not in link['href'] else link['href']
        #     print(cat_link)

        cat_urls = [["https://appsource.microsoft.com/en-us/marketplace/apps?category=ai-machine-learning", 'AI + Machine Learning'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=analytics", 'Analytics'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=collaboration", 'Collaboration'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=compliance-legals", 'Compliance & Legal'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=customer-service", 'Customer Service'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=finance", 'Finance'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=geolocation", 'Geolocation'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=human-resources", 'Human Resources'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=internet-of-things", 'Internet of Things'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=it-management-tools", 'IT & Management Tools'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=marketing", 'Marketing'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=operations", 'Operations'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=productivity", 'Productivity'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=sales", 'Sales'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=project-management", 'Project Management'],
                    ["https://appsource.microsoft.com/en-us/marketplace/apps?category=commerce", 'Commerce'],
        ]

        if response.status == 200:
            price_data = json.loads(response.text)
            if 'startingPrices' in price_data:
                self.pricing_data = price_data['startingPrices']
            for cat_link in cat_urls:
                yield scrapy.Request(cat_link[0], callback=self.parse_cat, meta={'category': cat_link[1]})

    def parse_cat(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """
        if response.status != 200:
            yield scrapy.Request(response.request.url, callback=self.parse_cat, dont_filter=True,
                                 meta={'category': response.meta['category']})
        else:
            soup = BS(response.text, 'html.parser')

            # parse products
            apps = soup.find('div', {'class': 'filteredGalleryContainer'}).find_all('a', {'class': 'tileMobileContainer'})

            for app in apps:
                try:
                    _id = app.find('div')['data-bi-name']
                    partner_link = self.api_link + _id
                    price_dict = self.pricing_data[_id] if _id in self.pricing_data else ''
                    yield scrapy.Request(partner_link, callback=self.parse_partner,
                                         meta={'price_dict': price_dict,
                                               'trial': 1,
                                               'category': response.meta['category']})
                except:
                    continue

            # follow next page
            next_page = soup.find('a', {'aria-label': "Next Page"})
            if next_page:
                next_link = 'https://appsource.microsoft.com' + next_page['href'] if 'https://appsource.microsoft.com' not in next_page['href'] else next_page['href']
                yield scrapy.Request(next_link, callback=self.parse_cat,
                                     meta={'category': response.meta['category']})

    def parse_partner(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """
        if response.status == 200:
            app_response = response.json()
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
                                 meta={'price_dict': response.meta['price_dict'],
                                       'trial': response.meta['trial'],
                                       'category': response.meta['category']})
            return
        else:
            self.logger.info('*'*60)
            self.logger.info('ERROR PARSE PARTNER')
            self.logger.info(response.status)
            self.logger.info('*' * 60)
            if response.meta['trial'] == 1:
                yield scrapy.Request(response.request.url, callback=self.parse_partner, dont_filter=True,
                                     meta={'price_dict': response.meta['price_dict'],
                                           'trial': 2,
                                           'category': response.meta['category']})
            return

        # print(response.text)
        # print(response.meta['price_dict'])

        # soup = BS(response.text, "html.parser")
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

        partner = response.json()

        item['partner_program_link'] = self.partner_program_link
        item['partner_directory'] = self.partner_directory
        item['partner_program_name'] = self.partner_program_name

        item['product/service_name'] = partner['title'] if 'title' in partner else ''
        if 'pricingData' in response.meta['price_dict']:
            if response.meta['price_dict']['pricingData'] == 6:
                item['pricing_plan'] = 'Free'
            elif response.meta['price_dict']['pricingData'] == 7:
                item['pricing_plan'] = 'Additional purchase may be required'
            elif response.meta['price_dict']['pricingData'] == 3:
                item['pricing_plan'] = ''
            else:
                item['pricing_plan'] = response.meta['price_dict']['pricingData']
        item['partner_company_name'] = partner['publisher'] if 'publisher' in partner else ''
        item['publisher'] = partner['publisher'] if 'publisher' in partner else ''
        desc = (partner['shortDescription'] if 'shortDescription' in partner else '') + ' ' + (partner['detailInformation']['Description'] if 'detailInformation' in partner and 'Description' in partner['detailInformation'] else '')
        item['product/service_description'] = cleanhtml(desc)
        item['languages'] = partner['detailInformation']['LanguagesSupported']

        if 'products' in partner and len(partner['products']) > 0:
            item['integrations'] = list()
            for key in partner['products'].keys():
                if key.startswith('product_mask_'):
                    mask = key
                    value = partner['products'][key]
                    for i in self.products:
                        if mask == i[0] and value >= i[1]:
                            value -= i[1]
                            item['integrations'].append(i[2])
                            if value == 0:
                                break

        item['categories'] = list()
        for key in partner.keys():
            if key.startswith('category_mask_'):
                filter_group = key
                filter_id = partner[key]
                for c in self.categories:
                    if c[0] == filter_group and c[1] == filter_id:
                        item['categories'].append(c[2])
        if len(item['categories']) == 0:
            item['categories'].append(response.meta['category'])

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

        item['product_version'] = partner['detailInformation']['AppVersion'] if 'detailInformation' in partner and 'AppVersion' in partner['detailInformation'] else ''
        item['support_link'] = partner['detailInformation']['SupportLink'] if 'detailInformation' in partner and 'SupportLink' in partner['detailInformation'] else ''
        item['help_link'] = partner['detailInformation']['HelpLink'] if 'detailInformation' in partner and 'HelpLink' in partner['detailInformation'] else ''
        item['privacy_policy_link'] = partner['detailInformation']['PrivacyPolicyUrl'] if 'detailInformation' in partner and 'PrivacyPolicyUrl' in partner['detailInformation'] else ''
        item['license_agreement_link'] = partner['licenseTermsUrl'] if 'licenseTermsUrl' in partner else ''

        releaseDate = partner['detailInformation']['ReleaseDate'] if 'detailInformation' in partner and 'ReleaseDate' in partner['detailInformation'] else ''
        if releaseDate and releaseDate != '':
            item['latest_update'] = datetime.datetime.strptime("2021-05-12T16:58:51.285691Z", '%Y-%m-%dT%H:%M:%S.%f%z').strftime('%m/%d/%Y')

        item['company_domain_name'] = partner['detailInformation']['SupportLink'] if 'detailInformation' in partner and 'SupportLink' in partner['detailInformation'] else ''
        url_obj = urllib.parse.urlparse(item['company_domain_name'])
        item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
        x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
        if x:
            item['company_domain_name'] = x[-1]

        try:
            x = requests.get(f'https://appsource.microsoft.com/en-us/product/office/{partner["entityId"]}?tab=DetailsAndSupport', headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"})
            if x.status_code == 200:
                soup = BS(x.text, "html.parser")
                acquire_div = soup.find('div', {'itemprop': 'Accounts_Supported'})
                item['account_requirements'] = [spn.get_text() for spn in acquire_div.find_all('span')]
        except:
            pass

        # item['linkedin_link'] = partner['linkedInOrganizationProfile'] if 'linkedInOrganizationProfile' in partner else ''
        # item['headquarters_street'] = location['addressLine1'] if 'addressLine1' in location else '' + (location['addressLine2'] if 'addressLine2' in location else '')
        # item['headquarters_city'] = location['city'] if 'city' in location else ''
        # item['headquarters_state'] = location['state'] if 'state' in location else ''
        # item['headquarters_zipcode'] = location['postalCode'] if 'postalCode' in location else ''
        # item['headquarters_country'] = location['country'] if 'country' in location else ''
        # item['partner_tier'] = partner['organizationSize'] if 'organizationSize' in partner else ''
        # item['industries'] = partner['industryFocus'] if 'industryFocus' in partner else ''
        # item['services'] = partner['serviceType'] if 'serviceType' in partner else ''
        # item['solutions'] = partner['solutions'] if 'solutions' in partner else ''
        # item['competencies'] = partner['competencies'] if 'competencies' in partner else ''
        # # item['partner_clients'] = partner['targetCustomerCompanySizes'] if 'targetCustomerCompanySizes' in partner else ''
        yield item
