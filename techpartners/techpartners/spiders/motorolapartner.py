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
    name = 'motorolapartner'
    partner_program_link = 'https://motorolasolutions.my.salesforce-sites.com/mmappfinder/?loc=XP-EN&dest=Channel&_ga=2.208552274.1166975015.1669758217-1931158018.1669758216&_gl=1n33apr__ga_MTkzMTE1ODAxOC4xNjY5NzU4MjE2_ga_23THW5EV9N*MTY2OTc2Mzk1My4yLjAuMTY2OTc2Mzk1My42MC4wLjA'
    partner_directory = 'Motorola Partner'
    partner_program_name = ''
    crawl_id = None

    custom_settings = {
        'CONCURRENT_REQUESTS': 8,
    }

    api_link = 'https://motorolasolutions.my.salesforce-sites.com/mmappfinder/MM_PartnerFinderHome'
    start_urls = [api_link]
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
                   'general_phone_number', 'general_email_address', 'general_fax_number',
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
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'motorolasolutions.my.salesforce-sites.com',
        'Origin': 'https://motorolasolutions.my.salesforce-sites.com',
        'Referer': 'https://motorolasolutions.my.salesforce-sites.com/mmappfinder/?loc=XP-EN&dest=Channel&_ga=2.208552274.1166975015.1669758217-1931158018.1669758216&_gl=1n33apr__ga_MTkzMTE1ODAxOC4xNjY5NzU4MjE2_ga_23THW5EV9N*MTY2OTc2Mzk1My4yLjAuMTY2OTc2Mzk1My42MC4wLjA',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        }

    regions = {"Asia Pacific": ["Australia", "Bangladesh", "Bhutan", "Cambodia", "China", "Christmas Island", "Cocos (Keeling) Islands", "Cook Islands", "Fiji", "French Polynesia", "Guam", "Hong Kong", "India", "Indonesia", "Japan", "Kiribati", "Korea, Republic Of", "Laos", "Macao", "Malaysia", "Maldives", "Marshall Islands", "Micronesia, Federated States Of", "Mongolia", "Myanmar", "Nauru", "Nepal", "New Caledonia", "New Zealand", "Niue", "Norfolk Island", "Northern Mariana Islands", "Palau", "Papua New Guinea", "Philippines", "Pitcairn", "Samoa", "Singapore", "Solomon Islands", "Sri Lanka", "Tahititi", "Taiwan", "Thailand", "Timor-Leste", "Tokelau", "Tonga", "Tuvalu", "Vanuatu", "Vietnam", "Wallis And Futuna"],
               "Latin America": ["Antigua And Barbuda", "Antilles", "Argentina", "Aruba", "Bahamas", "Barbados", "Belize", "Bermuda", "Bolivia", "Bonaire", "Brazil", "Cayhill", "Cayman Islands", "Chile", "Colombia", "Costa Rica", "Cuba", "Curacao", "Dominica", "Dominican Republic", "Ecuador", "El Salvador", "Falkland Islands (Malvinas)", "French Guiana", "Grenada", "Guadeloupe", "Guatemala", "Haiti", "Honduras", "Jamaica", "Martinique", "Mexico", "Montserrat", "Nicaragua", "Panama", "Paraguay", "Peru", "Puerto Rico", "Saint Kitts And Nevis", "Saint Lucia", "Saint Vincent And The Grenadines", "St. Barhelemy", "St. Maarten N.A.", "Suriname", "Trinidad And Tobago", "Turks And Caicos Islands", "Uruguay", "Venezuela", "Virgin Islands, British", "Virgin Islands, U.S."],
               "North America": ["Canada", "United States"],
               "U.S. Federal Government": ["Afghanistan", "Aland Islands", "Albania", "Algeria", "Andorra", "Angola", "Anguilla", "Antigua And Barbuda", "Antilles", "Argentina", "Armenia", "Aruba", "Australia", "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin", "Bermuda", "Bhutan", "Bolivia", "Bonaire", "Bosnia And Herzegovina", "Botswana", "Bouvet Island", "Brazil", "British Indian Ocean Territory", "Brunei Darussalam", "Bulgaria", "Burkina Faso", "Burundi", "Cambodia", "Cameroon", "Canada", "Cape Verde", "Cayhill", "Cayman Islands", "Central African Republic", "Chad", "Chile", "China", "Christmas Island", "Cocos (Keeling) Islands", "Colombia", "Comoros", "Congo", "Congo, The Democratic Republic Of The", "Cook Islands", "Costa Rica", "Croatia", "Cuba", "Curacao", "Cyprus", "Czech Republic", "Denmark", "Djibouti", "Dominica", "Dominican Republic", "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia", "Ethiopia", "Falkland Islands (Malvinas)", "Faroe Islands", "Fiji", "Finland", "France", "French Guiana", "French Polynesia", "French Southern Territories", "Gabon", "Gambia", "Georgia", "Germany", "Ghana", "Gibraltar", "Greece", "Grenada", "Guadeloupe", "Guam", "Guatemala", "Guernsey", "Guinea", "Guinea-Bissau", "Guyana", "Haiti", "Heard Island And Mcdonald Islands", "Honduras", "Hong Kong", "Hungary", "Iceland", "India", "Indonesia", "Iran, Islamic Republic Of", "Iraq", "Ireland", "Isle Of Man", "Israel", "Italy", "Ivory Coast", "Jamaica", "Japan", "Jersey", "Jordan", "Kazakhstan", "Kenya", "Kiribati", "Kosovo", "Kuwait", "Kyrgyzstan", "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libyan Arab Jamahiriya", "Liechtenstein", "Lithuania", "Luxembourg", "Macao", "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Islands", "Martinique", "Mauritania", "Mauritius", "Mayotte", "Mexico", "Micronesia, Federated States Of", "Moldova, Republic Of", "Monaco", "Mongolia", "Montenegro", "Montserrat", "Morocco", "Mozambique", "Myanmar", "Namibia", "Nauru", "Nepal", "Netherlands", "Netherlands Antilles", "New Caledonia", "New Zealand", "Nicaragua", "Niger", "Nigeria", "Niue", "Norfolk Island", "Northern Mariana Islands", "Norway", "Oman", "Palau", "Palestine", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines", "Pitcairn", "Poland", "Portugal", "Puerto Rico", "Qatar", "Republic of Macedonia", "Réunion", "Romania", "Russian Federation", "Rwanda", "Saint Helena", "Saint Kitts And Nevis", "Saint Lucia", "Saint Vincent And The Grenadines", "Samoa", "San Marino", "Sao Tome And Principe", "Saudi Arabia", "Senegal", "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia", "Solomon Islands", "Somalia", "South Africa", "South Georgia And The South Sa", "Spain", "Sri Lanka", "St. Barhelemy", "St. Maarten N.A.", "Sudan", "Suriname", "Svalbard And Jan Mayen", "Swaziland", "Sweden", "Switzerland", "Syrian Arab Republic", "Tahititi", "Tajikistan", "Tanzania, United Republic Of", "Thailand", "Timor-Leste", "Togo", "Tokelau", "Tonga", "Trinidad And Tobago", "Tunisia", "Turkey", "Turkmenistan", "Turks And Caicos Islands", "Tuvalu", "Uganda", "Ukraine", "United Arab Emirates", "United Kingdom", "United States", "Uruguay", "Uzbekistan", "Vanuatu", "Vatican City State", "Venezuela", "Virgin Islands, British", "Virgin Islands, U.S.", "Wallis And Futuna", "Western Sahara", "Yemen", "Zambia", "Zimbabwe"]}

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
            return

        soup = BS(response.text, "html.parser")

        ViewState = soup.find('input', {'id': 'com.salesforce.visualforce.ViewState'})['value']
        ViewStateVersion = soup.find('input', {'id': 'com.salesforce.visualforce.ViewStateVersion'})['value']
        ViewStateMAC = soup.find('input', {'id': 'com.salesforce.visualforce.ViewStateMAC'})['value']

        for region, countries in self.regions.items():
            for country in countries:
                page_index = 1
                payload = f'AJAXREQUEST=_viewRoot&j_id0%3AtheForm=j_id0%3AtheForm&j_id0%3AtheForm%3AsearchBox=&j_id0%3AtheForm%3ApartnerTechnologyIndustry=&j_id0%3AtheForm%3ApartnerTechnology=&j_id0%3AtheForm%3ApartnerIndustry=&j_id0%3AtheForm%3ApartnerRegion={urllib.parse.quote(region)}&j_id0%3AtheForm%3ApartnerCountry={urllib.parse.quote(country)}&j_id0%3AtheForm%3ApartnerState=&j_id0%3AtheForm%3ApartnerCounty=&j_id0%3AtheForm%3AtechHiddenfield=Business%20Radio%3BProfessional%20and%20Commercial%20Radio%3BSCADA&com.salesforce.visualforce.ViewState={urllib.parse.quote(ViewState)}&com.salesforce.visualforce.ViewStateVersion={ViewStateVersion}&com.salesforce.visualforce.ViewStateMAC={urllib.parse.quote(ViewStateMAC)}&selectedPage={page_index}&j_id0%3AtheForm%3Aj_id24=j_id0%3AtheForm%3Aj_id24&'
                yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse_region, body=payload,
                                     headers=self.headers,
                                     meta={'region': region, 'country': country, 'page_index': page_index},
                                     dont_filter=True)

    def parse_region(self, response):
        region = response.meta['region']
        country = response.meta['country']
        page_index = response.meta['page_index']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
        else:
            soup = BS(response.text, "html.parser")

            with open(f'motorola_region_{page_index}.html', 'a', encoding='utf-8') as ifile:
                ifile.write(soup.prettify())

            ViewState = soup.find('input', {'id': 'com.salesforce.visualforce.ViewState'})['value']
            ViewStateVersion = soup.find('input', {'id': 'com.salesforce.visualforce.ViewStateVersion'})['value']
            ViewStateMAC = soup.find('input', {'id': 'com.salesforce.visualforce.ViewStateMAC'})['value']

            partners = soup.find_all('div', {'class': 'even'})

            self.logger.info(f'Region: {region}, Country: {country}, Page: {page_index}, Partners#: {len(partners)}')

            for partner in partners:

                for br in partner.find_all("br"):
                    br.replace_with(" ")

                name = partner.find('h4').text

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                # item['partner_program_name'] = self.partner_program_name

                item['partner_company_name'] = name
                item['regions'] = region
                item['headquarters_country'] = country

                info1 = partner.find('div', {'class': 'searchcol1'})
                txt = cleanhtml(info1.text)

                partner_link = info1.find('a').text if info1.find('a') else None
                if partner_link:

                    item['company_domain_name'] = get_domain_from_url(partner_link)
                    txt = txt.replace(partner_link, '')

                x = re.search(r"Community.*?$", txt, re.IGNORECASE)
                if x:
                    item['partner_type'] = x.group().strip()
                    txt = txt.replace(item['partner_type'], '')
                    item['partner_type'] = item['partner_type'].replace('Community', '', 1).strip()

                x = re.search(r"Program\sLevel.*", txt, re.IGNORECASE)
                if x:
                    item['partner_tier'] = x.group().strip()
                    txt = txt.replace(item['partner_tier'], '')
                    item['partner_tier'] = item['partner_tier'].replace('Program Level', '', 1).strip()

                if 'fax' in txt.lower():
                    x = re.search(r"Fax.*", txt, re.IGNORECASE)
                    if x:
                        item['general_fax_number'] = x.group().strip()
                        txt = txt.replace(item['general_fax_number'], '')
                        item['general_fax_number'] = item['general_fax_number'].replace('Fax', '', 1).strip().strip(':')

                x = re.search(r"Phone.*", txt, re.IGNORECASE)
                if x:
                    item['general_phone_number'] = x.group().strip()
                    txt = txt.replace(item['general_phone_number'], '')
                    item['general_phone_number'] = item['general_phone_number'].replace('Phone', '', 1).strip().strip(':')

                if txt != '':
                    item['headquarters_address'] = txt.strip()

                info2 = partner.find('div', {'class': 'searchcol2'})
                txt = cleanhtml(info2.text)

                x = re.search(r"Industries.*", txt, re.IGNORECASE)
                if x:
                    item['industries'] = x.group().strip()
                    txt = txt.replace(item['industries'], '')
                    item['industries'] = item['industries'].replace('Industries', '', 1).strip()

                x = re.search(r"Authorized\sProducts.*", txt, re.IGNORECASE)
                if x:
                    item['products'] = x.group().strip()
                    txt = txt.replace(item['products'], '')
                    item['products'] = item['products'].replace('Authorized Products', '', 1).strip()

                x = re.search(r"Technology\sSpecialization.*", txt, re.IGNORECASE)
                if x:
                    item['specializations'] = x.group().strip()
                    txt = txt.replace(item['specializations'], '')
                    item['specializations'] = item['specializations'].replace('Technology Specialization', '', 1).strip()

                yield item

            # follow next page
            if len(partners) == 12:
                page_index += 1
                payload = f'AJAXREQUEST=_viewRoot&j_id0%3AtheForm=j_id0%3AtheForm&j_id0%3AtheForm%3AsearchBox=&j_id0%3AtheForm%3ApartnerTechnologyIndustry=&j_id0%3AtheForm%3ApartnerTechnology=&j_id0%3AtheForm%3ApartnerIndustry=&j_id0%3AtheForm%3ApartnerRegion={urllib.parse.quote(region)}&j_id0%3AtheForm%3ApartnerCountry={urllib.parse.quote(country)}&j_id0%3AtheForm%3ApartnerState=&j_id0%3AtheForm%3ApartnerCounty=&j_id0%3AtheForm%3AtechHiddenfield=Business%20Radio%3BProfessional%20and%20Commercial%20Radio%3BSCADA&com.salesforce.visualforce.ViewState={urllib.parse.quote(ViewState)}&com.salesforce.visualforce.ViewStateVersion={ViewStateVersion}&com.salesforce.visualforce.ViewStateMAC={urllib.parse.quote(ViewStateMAC)}&selectedPage={page_index}&j_id0%3AtheForm%3Aj_id304=j_id0%3AtheForm%3Aj_id304&'
                yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse_region, body=payload,
                                     headers=self.headers,
                                     meta={'region': region, 'country': country, 'page_index': page_index},
                                     dont_filter=True)
