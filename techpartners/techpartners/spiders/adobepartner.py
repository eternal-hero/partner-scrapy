# import needed libraries
from techpartners.spiders.base_spider import BaseSpider
import scrapy
import requests
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'adobepartner'
    partner_program_link = 'https://solutionpartners.adobe.com/s/directory/'
    partner_directory = 'Adobe Solution Partner'
    partner_program_name = ''
    crawl_id = 1257

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

    def start_requests(self):
        """
        landing function of spider class
        will get api results, create the search pages and pass it to parse function
        :return:
        """

        'https://partner-directory.adobe.io/v1/spp/listings?'

        geo_lst = ['asia-pacific',
                   'europe',
                   'japan',
                   'latin-america',
                   'middle-east-africa',
                   'north-america']

        company_size_lst = ['1-9',
                            '10-49',
                            '50-99',
                            '100-499',
                            '500-999',
                            '1000-14999',
                            '15000']

        data_domain = "https://partner-directory.adobe.io/v1/spp/listing/"

        for geo in geo_lst:
            for company_size in company_size_lst:

                n = 0
                last = 0

                while n <= last:
                    try:
                        link = f"https://partner-directory.adobe.io/v1/spp/listings?geo={geo}&company-size={company_size}&page={n}"
                        r = requests.get(link, headers={'x-api-key': 'partner_directory'})
                        list_response = r.json()

                        # update last from response
                        last = list_response['totalPages']
                        items = list_response['listings']

                        for item in items:
                            # get partner page link
                            id = item['id']
                            item_link = data_domain + id
                            yield scrapy.Request(item_link, callback=self.parse, headers={'x-api-key': 'partner_directory'}, dont_filter=True,
                                                 meta={'specialized': item['specialized'],
                                                       'certifiedEmployees': item['certifiedEmployees'],
                                                       'geographic': geo,
                                                       'company_size': company_size
                                                       })

                    except Exception as e:
                        print(e)
                    n += 1

    def parse(self, response):
        """
        parse item api and get results json data
        :param response:
        :return:
        """
        # Initialize item
        item = dict()
        for k in self.item_fields:
            item[k] = ''

        item['partner_program_link'] = self.partner_program_link
        item['partner_directory'] = self.partner_directory
        item['partner_program_name'] = self.partner_program_name

        # specialized = response.meta['specialized']
        # certifiedEmployees = response.meta['certifiedEmployees']

        item['regions'] = response.meta['geographic']
        item['company_size'] = response.meta['company_size']

        item_response = response.json()
        item['partner_company_name'] = item_response['companyInfo']['name'].strip()
        # title = item_response['companyInfo']['shortDescription']
        # logo = item_response['companyInfo']['logoUrl']
        item['partner_tier'] = item_response['companyInfo']['level']
        item['company_description'] = cleanhtml(item_response['companyInfo']['longDescription'])
        item['primary_contact_name'] = item_response['companyInfo']['primaryContactName']
        item['general_email_address'] = item_response['companyInfo']['primaryContactEmail']

        item['general_phone_number'] = item_response['companyInfo']['phone']
        # item['company_domain_name'] = item_response['companyInfo']['website'].strip()
        url_obj = urllib.parse.urlparse(item_response['companyInfo']['website'].strip() if 'website' in item_response['companyInfo'] else '')
        item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
        x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
        if x:
            item['company_domain_name'] = x[-1]

        item['services'] = [itm['name'] for itm in item_response['services']]
        item['industries'] = item_response['industries']
        item['specializations'] = [itm['name'] for itm in item_response['expertise']]
        item['integrations'] = [itm['name'] for itm in item_response['expertise']]
        item['solutions'] = [{'headline': itm['headline'], 'description': cleanhtml(itm['description']), 'solutions': itm['solutions']} for itm in item_response['accreditedSolutions']]
        item['partner_type'] = item_response['companyInfo']['type']
        item['certifications'] = item_response['companyInfo']['certifications']

        addresses = item_response['addresses']
        if len(addresses) > 0:
            # get headquarter data
            for i in addresses:
                if i['isHeadquarter']:
                    item['headquarters_street'] = i['address1']
                    item['headquarters_city'] = i['city']
                    item['headquarters_state'] = i['state']
                    item['headquarters_zipcode'] = i['postalCode']
                    item['headquarters_country'] = i['country']
                    break

            for i in addresses:
                item['locations_street'] = i['address1']
                item['locations_city'] = i['city']
                item['locations_state'] = i['state']
                item['locations_zipcode'] = i['postalCode']
                item['locations_country'] = i['country']
                yield item
        else:
            yield item
