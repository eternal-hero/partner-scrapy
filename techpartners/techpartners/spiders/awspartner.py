# import needed libraries
import json
import re
import time

import requests
from techpartners.spiders.base_spider import BaseSpider
import scrapy
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'awspartner'
    partner_program_link = 'https://partners.amazonaws.com/search/partners'
    partner_directory = 'AWS Partner'
    partner_program_name = ''
    crawl_id = 1255

    site_name = 'AWS Partner Finder'

    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
    }

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'headquarters_street',
                   'headquarters_city', 'headquarters_state', 'headquarters_zipcode', 'headquarters_country',
                   'locations_street', 'locations_city', 'locations_state', 'locations_zipcode', 'locations_country',
                   'regions', 'languages', 'services', 'solutions', 'products', 'pricing', 'specializations',
                   'categories', 'year_founded', 'partnership_timespan', 'general_phone_number', 'general_email_address',
                   'use_case', 'integrations', 'contract_vehicles', 'company_characteristics', 'linkedin_link',
                   'twitter_link', 'facebook_link', 'primary_contact_name', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'certified?', 'industries', 'partner_clients', 'notes']

    def start_requests(self):
        """
        landing function of spider class
        will get search result length and create the search pages and pass it to parse function
        :return:
        """

        base_url = "https://api.finder.partners.aws.a2z.com/search?locale=en&highlight=on&sourceFilter=searchPage&size=50&from="
        partner_url = "https://api.finder.partners.aws.a2z.com/search?id="

        countNumber = 0
        # iterate through search pages
        while True:
            response = requests.get(base_url + str(countNumber))
            data = json.loads(response.text)

            # get search results items
            if 'results' in data['message']:
                partners = data['message']['results']
                # iterate through search results
                for partner in partners:
                    profileId = partner['_id'].strip()
                    # name = partner['_source']['name'].strip()
                    # description = partner['_source']['description'] if 'description' in partner['_source'] else ''

                    yield scrapy.Request((partner_url + profileId),
                                         callback=self.parse, dont_filter=True,
                                         meta={'Partner_id': profileId})

                countNumber += len(partners)
                if len(partners) >= 50:
                    continue
                else:
                    break
            elif data['message'] == 'Forbidden':
                self.crawler.engine.pause()
                print('Engine Paused')
                time.sleep(300)
                self.crawler.engine.unpause()
                continue

    def parse(self, response):
        """
        parse search page and get results json data
        :param response:
        :return:
        """
        info = json.loads(response.text)

        if info['message'] == 'Forbidden':
            self.crawler.engine.pause()
            print('Engine Paused')
            time.sleep(300)
            self.crawler.engine.unpause()

            yield scrapy.Request(response.request.url, callback=self.parse, dont_filter=True,
                                 meta={'Partner_id': response.meta['Partner_id']})
        else:
            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = self.partner_program_name

            data = info['message']['_source']

            item['partner_company_name'] = data['name']
            item['partner_type'] = data['customer_type'] if 'customer_type' in data else ''
            item['languages'] = data['language'] if 'language' in data else ''

            # 'aws certifications count': data['aws_certifications_count'] if 'aws_certifications_count' in data else ''
            # item['specializations'] = data['technology_expertise'] if 'technology_expertise' in data else ''
            # 'solutions practice count': data['solutions_practice_count'] if 'solutions_practice_count' in data else '',
            item['company_description'] = data['description'] if 'description' in data else ''
            if 'references' in data:
                item['partner_clients'] = list()
                for itm in data['references']:
                    if 'customer_name' in itm:
                        item['partner_clients'].append(itm['customer_name'])
                    else:
                        item['partner_clients'].append(itm['title'])
            item['industries'] = data['industry'] if 'industry' in data else ''
            tmp = data['refiners'] if 'refiners' in data else ''
            parsed = list()
            for i in tmp:
                if i.startswith('Product'):
                    parsed.append(i.split(':')[-1].strip())
            item['integrations'] = parsed if len(parsed) != 0 else ''
            # 'number of employees': data['numberofemployees'] if 'numberofemployees' in data else '',
            # 'customer launches count': data['customer_launches_count'] if 'customer_launches_count' in data else '',
            # 'segment': data['segment'] if 'segment' in data else '',
            # 'partner path': data['partner_path'] if 'partner_path' in data else '',
            item['services'] = data['professional_service_types'] if 'professional_service_types' in data else ''
            item['partner_tier'] = data['current_program_status'] if 'current_program_status' in data else ''
            # item['company_domain_name'] = data['website'] if 'website' in data else ''
            url_obj = urllib.parse.urlparse(data['website'] if 'website' in data else '')
            item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
            x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
            if x:
                item['company_domain_name'] = x[-1]

            item['solutions'] = list()
            # item['categories'] = list()         # AWS Practices
            # if 'solutions' in data:
            #     for itm in data['solutions']:
            #         if 'record_type' in itm:
            #             if itm['record_type'] == "Consulting Service":
            #                 item['categories'].append(itm['solution_name'][0] if 'solution_name' in itm and len(itm['solution_name']) > 0 else itm['title'])
            #             else:
            #                 item['solutions'].append(itm['solution_name'][0] if 'solution_name' in itm and len(itm['solution_name']) > 0 else itm['title'])

            item['competencies'] = data['competency_membership'] if 'competency_membership' in data else ''
            # item['categories'] = data['public_sector_program_categories'] if 'public_sector_program_categories' in data else ''
            item['partner_programs'] = data['program_membership'] if 'program_membership' in data else ''
            # 'domain': data['domain'] if 'domain' in data else '',
            item['validations'] = data['service_membership'] if 'service_membership' in data else ''
            item['certifications'] = data['aws_certifications'] if 'aws_certifications' in data else ''
            item['specializations'] = data['use_case_expertise'] if 'use_case_expertise' in data else ''
            item['company_characteristics'] = data['socio_economic_categories'] if 'socio_economic_categories' in data else ''
            item['contract_vehicles'] = [s['name'] for s in data['public_sector_contract_urls']] if 'public_sector_contract_urls' in data and len(data['public_sector_contract_urls']) > 0 else ''
            partner_url = 'https://partners.amazonaws.com/partners/' + response.meta['Partner_id'] + '/' + item['partner_company_name']
            parsed_url = urllib.parse.quote_plus(partner_url)

            item['linkedin_link'] = 'https://www.linkedin.com/shareArticle?url=' + parsed_url
            item['twitter_link'] = 'https://twitter.com/intent/tweet/?via=awscloud&url=' + parsed_url
            item['facebook_link'] = 'https://www.facebook.com/sharer/sharer.php?u=' + parsed_url

            addresses = data['office_address'] if 'office_address' in data else list()
            if len(addresses) > 0:
                # get headquarter data
                for loc in addresses:
                    if 'location_type' in loc and 'Headquarters' in loc['location_type']:
                        item['headquarters_street'] = loc['street'] if 'street' in loc else ''
                        item['headquarters_city'] = loc['city'] if 'city' in loc else ''
                        item['headquarters_state'] = loc['state'] if 'state' in loc else ''
                        item['headquarters_zipcode'] = loc['postalcode'] if 'postalcode' in loc else ''
                        item['headquarters_country'] = loc['country'] if 'country' in loc else ''
                        break

                for loc in addresses:
                    item['locations_street'] = loc['street'] if 'street' in loc else ''
                    item['locations_city'] = loc['city'] if 'city' in loc else ''
                    item['locations_state'] = loc['state'] if 'state' in loc else ''
                    item['locations_zipcode'] = loc['postalcode'] if 'postalcode' in loc else ''
                    item['locations_country'] = loc['country'] if 'country' in loc else ''
                    yield item
            else:
                yield item
