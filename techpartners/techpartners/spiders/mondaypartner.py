# import needed libraries
import json
import re
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'mondaypartner'
    partner_program_link = 'https://monday.com/p/find-a-partner/'
    partner_directory = 'Monday Partner Directory'
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
                   'primary_contact_name', 'primary_contact_phone_number',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    api_link = 'https://monday.com/p/wp-admin/admin-ajax.php'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'Authority': 'monday.com',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://monday.com',
        'Referer': 'https://monday.com/p/find-a-partner/',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'X-Requested-With': 'XMLHttpRequest',
        }

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
            return

        soup = BS(response.text, "html.parser")
        div = soup.find('div', {'class': 'partners-list'})
        if div:
            partners = div.find_all('div', {'class': 'partner-item mb-3 mb-md-5'})
            for partner in partners:
                tier_div = partner.select_one('div[class*="partner-item__tiering"]')
                if tier_div:
                    partner_tier = tier_div.text.strip()
                else:
                    partner_tier = ''

                desc_div = partner.find('div', {'class': 'partner-item__description'})
                if desc_div:
                    partner_desc = desc_div.text.strip()
                else:
                    partner_desc = ''

                payload = 'action=get_partner_info&slug=' + partner['data-partner-slug']
                yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse_partner, body=payload,
                                     headers=self.headers,
                                     meta={'partner_tier': partner_tier, 'partner_desc': partner_desc},
                                     dont_filter=True)

        # follow next pages
        load_more_ids = None
        scripts = soup.find_all('script')
        for script in scripts:
            if 'partnersData' in script.text:
                content = script.text[script.text.find('partnersData'):]
                content = content[content.find('=') + 1: content.rfind(';')]
                for line in content.strip().splitlines():
                    if 'load_more_ids' in line:
                        load_more_ids = line.strip().replace('load_more_ids', '').strip().strip(':').strip(',').strip()
                        load_more_ids = json.loads(load_more_ids)
                        break
                break

        if load_more_ids and len(load_more_ids) > 0:
            payload = 'action=load_more_partners&posts_per_page=30'
            for _id in load_more_ids:
                payload += f'&load_more_ids[]={_id}'
            yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse_next, headers=self.headers, body=payload)

    def parse_next(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
        else:
            page_data = json.loads(response.text)

            page_html = page_data['html']
            soup = BS(page_html, "html.parser")
            partners = soup.find_all('div', {'class': 'partner-item mb-3 mb-md-5'})
            for partner in partners:
                tier_div = partner.select_one('div[class*="partner-item__tiering"]')
                if tier_div:
                    partner_tier = tier_div.text.strip()
                else:
                    partner_tier = ''

                desc_div = partner.find('div', {'class': 'partner-item__description'})
                if desc_div:
                    partner_desc = desc_div.text.strip()
                else:
                    partner_desc = ''

                payload = 'action=get_partner_info&slug=' + partner['data-partner-slug']
                yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse_partner, body=payload,
                                     headers=self.headers,
                                     meta={'partner_tier': partner_tier, 'partner_desc': partner_desc},
                                     dont_filter=True)

            load_more_ids = page_data['new_load_more_ids']
            if len(load_more_ids) > 0:
                payload = 'action=load_more_partners&posts_per_page=30'
                for _id in load_more_ids:
                    payload += f'&load_more_ids[]={_id}'
                yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse_next, headers=self.headers, body=payload)

    def parse_partner(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
        else:
            partner_data = json.loads(response.text)['html']
            soup = BS(partner_data, "html.parser")

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = self.partner_program_name

            item['partner_company_name'] = soup.find('h2').text.strip()
            item['company_description'] = cleanhtml(response.meta['partner_desc'])
            item['partner_tier'] = response.meta['partner_tier']

            item['headquarters_country'] = soup.find('span', {'class': 'partner-popup__location'}).text.strip()

            contact_div = soup.find('div', {'class': 'partner-popup__content'})
            if contact_div:
                lines = contact_div.find_all('p')
                for line in lines:
                    txt = line.text.strip()
                    if txt != '':
                        if txt.startswith('website:'):
                            item['company_domain_name'] = txt.replace('website:', '').strip()
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

                        elif txt.startswith('Address:'):
                            item['headquarters_address'] = txt.replace('Address:', '').strip()
                        elif txt.startswith('Phone:'):
                            item['general_phone_number'] = txt.replace('Phone:', '').strip()
                        elif txt.startswith('Email:'):
                            item['general_email_address'] = txt.replace('Email:', '').strip()

            services_lst = soup.find('ul', {'id': 'services'})
            if services_lst:
                item['services'] = [li.text.strip() for li in services_lst.find_all('li')]

            industries_lst = soup.find('ul', {'id': 'industries-we-specialize-in'})
            if industries_lst:
                item['industries'] = [li.text.strip() for li in industries_lst.find_all('li')]

            regions_lst = soup.find('ul', {'id': 'regions-we-serve'})
            if regions_lst:
                item['regions'] = [li.text.strip() for li in regions_lst.find_all('li')]

            yield item
