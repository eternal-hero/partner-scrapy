# import needed libraries
import json
import math
import re
import time

import requests
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse

partners = dict()
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
           'Connection': 'keep-alive'}


def get_solutions(solution_link, item):
    r = requests.get(solution_link)
    if r.status_code != 200:
        print(f'ERROR SOLUTION STATUS: {r.status_code}, URL: {r.url}')
    else:
        soup = BS(r.text, "html.parser")
        solutions_div = soup.find('div', {'class': 'js-search-results'})
        if solutions_div:
            solutions = solutions_div.find('bolt-grid').find_all('bolt-grid-item', recursive=False)
            item['solutions'] = [solution.find('h3').text.strip() for solution in solutions]
            return item


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'pegapartner'
    partner_program_link = 'https://www.pega.com/services/partnerships/partner-finder/results'
    partner_directory = 'Pega Partner Directory'
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
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'Connection': 'keep-alive',
        }

    def parse(self, response):
        global partners
        if response.status != 200:
            print(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
            return

        industry_lst = [None]

        soup = BS(response.text, "html.parser")
        industries = soup.find_all('input', {'type': 'checkbox'}, id=re.compile(r'^industry_.*'))
        industry_lst += [inpt['value'] for inpt in industries]

        def parse_page(page_number, response, industry=None):
            global partners
            page_partners = list()

            soup = BS(response.text, "html.parser")
            div = soup.find('div', {'class': 'js-search-results'})
            if div:
                page_partners = div.find('div').find_all('div', recursive=False)
                print(f"Page number: {page_number}{f', Industry: {industry}' if industry else ''}, Number of results = {len(page_partners)}")

                for partner in page_partners:
                    name = partner.find('h2').text.strip()

                    if name in partners.keys():
                        item = partners[name]
                    else:
                        # Initialize item
                        item = dict()
                        for k in self.item_fields:
                            item[k] = ''

                        item['partner_program_link'] = self.partner_program_link
                        item['partner_directory'] = self.partner_directory
                        item['partner_program_name'] = self.partner_program_name

                        item['partner_company_name'] = name
                        item['certifications'] = [tag.text.strip() for tag in partner.find_all('bolt-list-item', {'role': 'presentation'})]
                        item['company_description'] = cleanhtml(partner.find('bolt-text').text.strip())

                        item['industries'] = list()

                        all_grid = partner.find_all('h3')
                        for grid in all_grid:
                            if 'Specialties' in grid.text:
                                item['specializations'] = [li.text.strip() for li in grid.find_next('ul').find_all('li')]

                            elif 'Authorized regions' in grid.text:
                                item['regions'] = [li.text.strip() for li in grid.find_next('ul').find_all('li')]

                        partner_link = ''
                        all_links = partner.find_all('a', {'href': True})
                        for link in all_links:
                            if 'Partner details' in link.text.strip() or 'Partner website' in link.text.strip():
                                partner_link = link['href']

                            elif 'See solutions' in link.text.strip():
                                item = get_solutions(link['href'], item)

                        if 'https://www.pega.com/' in partner_link:
                            item = parse_partner(item, partner_link)
                        else:
                            item['company_domain_name'] = get_domain_from_url(partner_link)

                        partners[item['partner_company_name']] = item

                    if item and industry:
                        item['industries'].append(industry)

            return len(page_partners)

        def parse_partner(item, partner_link):
            response = requests.get(partner_link, headers=headers)
            if response.status_code != 200:
                print(f'ERROR REQUEST STATUS: {response.status_code}, URL: {response.url}')
                return item

            soup = BS(response.text, "html.parser")
            all_links = soup.find_all('a', {'href': True})
            for link in all_links:
                if link and 'Partner website' in link.text.strip():
                    item['company_domain_name'] = get_domain_from_url(link['href'])

                elif link and 'See solutions' in link.text.strip():
                    item = get_solutions(link['href'], item)

            all_grid = soup.find_all('bolt-grid-item')
            for grid in all_grid:
                if grid and grid.find('h4'):
                    if grid.find('h4') and 'Partner capabilities' in grid.find('h4').text:
                        item['services'] = [li.text.strip() for li in grid.find_all('li')]

            return item

        # parse initial response
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            for industry in industry_lst:
                page_number = 0
                while True:
                    link = f'https://www.pega.com/services/partnerships/partner-finder/results?{f"f[]=industry:{urllib.parse.quote_plus(industry)}&" if industry else ""}page={page_number}'
                    response = requests.get(link, headers=headers)
                    partners_number = parse_page(page_number, response, industry)

                    # follow next pages
                    if partners_number == 30:
                        page_number += 1
                        continue
                    else:
                        break

        for item in partners.values():
            yield item
