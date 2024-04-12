# import needed libraries
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
    name = 'xeropartner'
    partner_program_link = 'https://www.xero.com/us/advisors/find-advisors/'
    partner_directory = 'Xero Partner Directory'
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
        'Authority': 'www.xero.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'x-requested-with': 'XMLHttpRequest',
        'upgrade-insecure-requests': 1,
        }

    countries = ['United States', 'Afghanistan', 'Aland Islands', 'Albania', 'Algeria', 'American Samoa', 'Andorra', 'Angola', 'Anguilla', 'Antarctica', 'Antigua and Barbuda', 'Argentina', 'Armenia', 'Aruba', 'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bermuda', 'Bhutan', 'Bolivia, Plurinational State of', 'Bonaire, Sint Eustatius and Saba', 'Bosnia and Herzegovina', 'Botswana', 'Bouvet Island', 'Brazil', 'British Indian Ocean Territory', 'Brunei Darussalam', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cambodia', 'Cameroon', 'Canada', 'Cape Verde', 'Cayman Islands', 'Central African Republic', 'Chad', 'Chile', 'China', 'Christmas Island', 'Cocos (Keeling) Islands', 'Colombia', 'Comoros', 'Congo', 'Congo, The Democratic Republic of the', 'Cook Islands', 'Costa Rica', "Côte d'Ivoire", 'Croatia', 'Cuba', 'Curaçao', 'Cyprus', 'Czech Republic', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic', 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Ethiopia', 'Falkland Islands (Malvinas)', 'Faroe Islands', 'Fiji', 'Finland', 'France', 'French Guiana', 'French Polynesia', 'French Southern Territories', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Gibraltar', 'Greece', 'Greenland', 'Grenada', 'Guadeloupe', 'Guam', 'Guatemala', 'Guernsey', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti', 'Heard Island and McDonald Islands', 'Holy See (Vatican City State)', 'Honduras', 'Hong Kong', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran, Islamic Republic of', 'Iraq', 'Ireland', 'Isle of Man', 'Israel', 'Italy', 'Jamaica', 'Japan', 'Jersey', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati', "Korea, Democratic People's Republic of", 'Korea, Republic of', 'Kuwait', 'Kyrgyzstan', "Lao People's Democratic Republic", 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macao', 'Macedonia, Republic of', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Martinique', 'Mauritania', 'Mauritius', 'Mayotte', 'Mexico', 'Micronesia, Federated States of', 'Moldova, Republic of', 'Monaco', 'Mongolia', 'Montenegro', 'Montserrat', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nauru', 'Nepal', 'Netherlands', 'New Caledonia', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'Niue', 'Norfolk Island', 'Northern Mariana Islands', 'Norway', 'Oman', 'Pakistan', 'Palau', 'Palestinian Territory, Occupied', 'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Pitcairn', 'Poland', 'Portugal', 'Puerto Rico', 'Qatar', 'Réunion', 'Romania', 'Russian Federation', 'Rwanda', 'Saint Barthélemy', 'Saint Helena, Ascension and Tristan da Cunha', 'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Martin (French part)', 'Saint Pierre and Miquelon', 'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 'Sint Maarten (Dutch part)', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 'South Georgia and the South Sandwich Islands', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname', 'South Sudan', 'Svalbard and Jan Mayen', 'Swaziland', 'Sweden', 'Switzerland', 'Syrian Arab Republic', 'Taiwan, Province of China', 'Tajikistan', 'Tanzania, United Republic of', 'Thailand', 'Timor-Leste', 'Togo', 'Tokelau', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Turks and Caicos Islands', 'Tuvalu', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 'Uruguay', 'Uzbekistan', 'Vanuatu', 'Venezuela, Bolivarian Republic of', 'Viet Nam', 'Virgin Islands, British', 'Virgin Islands, U.S.', 'Wallis and Futuna', 'Yemen', 'Zambia', 'Zimbabwe']

    def start_requests(self):
        for country in self.countries:
            page_number = 1
            link = self.partner_program_link + f'{country}/?type=advisors&orderBy=ADVISOR_RELEVANCE&sort=ASC&pageNumber={page_number}'
            yield scrapy.Request(method='GET', url=link, callback=self.parse,
                                 headers=self.headers,
                                 meta={'page_number': page_number,
                                       'country': country})

    def parse(self, response):
        page_number = response.meta['page_number']
        country = response.meta['country']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:

            soup = BS(response.text, "html.parser")

            # check data location
            current_location = soup.find('div', {'data-advisor-results-location': True})
            if country != 'United States' and (not current_location or current_location['data-advisor-results-location'] != country):
                return

            partners = soup.find_all('a', {'class': 'advisors-result-card-link'})

            self.logger.info(f"Country: {country}, Page Number = {page_number}, Number of results = {len(partners)}")

            for partner in partners:
                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item['partner_company_name'] = partner.find('h3').text
                item['headquarters_country'] = country
                try:
                    partner_link = partner['href']
                except:
                    partner_link = None

                if partner_link:
                    yield scrapy.Request(method='GET', url=partner_link, headers=self.headers,
                                         callback=self.parse_partner,
                                         meta={'item': item})
                else:
                    yield item

            # follow next pages
            if page_number == 1:
                pagination_div = soup.find('div', {'class': re.compile('^advisor-search-tally.*')})
                if pagination_div:
                    txt = pagination_div.text
                    total_pages = math.ceil(int(txt[txt.find('of')+2:].replace('results', '').strip())/10)
                    for page_number in range(2, total_pages+1):
                        link = self.partner_program_link + f'{country}/?type=advisors&orderBy=ADVISOR_RELEVANCE&sort=ASC&pageNumber={page_number}'
                        yield scrapy.Request(method='GET', url=link, callback=self.parse,
                                             headers=self.headers,
                                             meta={'page_number': page_number,
                                                   'country': country})

    def parse_partner(self, response):
        item = response.meta['item']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST PARTNER, COMPANY: {item["partner_company_name"]}, URL: {response.request.url}')
        else:
            soup = BS(response.text, "html.parser")

            data = soup.find('p', {'class': 'advisors-profile-hero-detailed-info-sub national'})
            if data:
                sep = data.find('span', {'class': 'divider'})
                if sep:
                    item['partner_type'] = cleanhtml(data.text[:data.text.find(sep.text)])
                else:
                    item['partner_type'] = cleanhtml(data.text)

            item['company_description'] = cleanhtml(soup.find('div', {'class': 'advisor-profile-practice-desc'}).text if soup.find('div', {'class': 'advisor-profile-practice-desc'}) else '')

            all_links = soup.find_all('a')
            for link in all_links:
                if 'View website' in link.text:
                    item['company_domain_name'] = link['href']
                    try:
                        url_obj = urllib.parse.urlparse(item['company_domain_name'])
                        item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                        x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                        if x:
                            item['company_domain_name'] = x[-1]
                        if '/' in item['company_domain_name']:
                            item['company_domain_name'] = item['company_domain_name'][:item['company_domain_name'].find('/')]
                    except Exception as e:
                        print('DOMAIN ERROR: ', e)
                    break

            item['general_phone_number'] = soup.find('a', {'data-phone': True})['data-phone'] if soup.find('a', {'data-phone': True}) else ''

            badges_div = soup.find('div', {'data-react-id': 'partner-badge-group'})
            if badges_div:
                labels = badges_div.find_all('p')
                for lbl in labels:
                    if 'Partner status' in lbl.text:
                        item['partner_tier'] = lbl.parent.find('h6').text.strip()
                    elif 'Certification' in lbl.text:
                        item['certifications'] = lbl.parent.find('h6').text.strip()
                    elif 'Experience' in lbl.text:
                        item['partnership_founding_date'] = lbl.parent.find('h6').text.replace('Partner since', '').strip()

            specialist_div = soup.find('div', {'data-react-id': 'industry-tag-specialist-content'})
            if specialist_div:
                labels = specialist_div.find_all('h6')
                if len(labels) > 0:
                    item['integrations'] = [ind.text.strip() for ind in labels]

            industries_div = soup.find('div', {'data-react-id': 'industry-tag-served-content'})
            if industries_div:
                labels = industries_div.find_all('li')
                if len(labels) > 0:
                    item['industries'] = [ind.text.strip() for ind in labels]

            social_links = soup.find('ul', {'class': 'advisor-profile-practice-social-links'}).find_all('a') if soup.find('ul', {'class': 'advisor-profile-practice-social-links'}) else list()
            for social_link in social_links:
                if 'Facebook' in social_link.text:
                    item['facebook_link'] = social_link['href']

                elif 'Twitter' in social_link.text:
                    item['twitter_link'] = social_link['href']

                elif 'LinkedIn' in social_link.text:
                    item['linkedin_link'] = social_link['href']

            if soup.find('div', {'class': 'advisors-profile-locations-list-wrapper'}):
                locations = soup.find('div', {'class': 'advisors-profile-locations-list-wrapper'}).find_all('dl')
                if len(locations) > 0:
                    item['headquarters_address'] = locations[0].find('dd', {'class': 'advisors-profile-locations-list-item-address'}).text
            else:
                locations = list()

            if len(locations) > 1:
                for location in locations[1:]:
                    item['locations_address'] = location.find('dd', {'class': 'advisors-profile-locations-list-item-address'}).text
                    item['general_phone_number'] = location.find('dd', {'class': 'advisors-profile-locations-list-item-phone'}).text
                    yield item
            else:
                yield item
