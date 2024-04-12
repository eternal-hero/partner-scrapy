# import needed libraries
import time
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse
from selenium import webdriver


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'securitypartner'
    partner_program_link = 'https://www.securityinformed.com/international-security-companies.html'
    partner_directory = 'Securityinformed Partner'
    partner_program_name = ''
    crawl_id = None

    start_urls = ['https://www.example.com']
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
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
               'Accept-Encoding': 'gzip, deflate, br',
               'Accept-Language': 'en-US,en;q=0.9',
               'Cache-Control': 'max-age=0',
               'Authority': 'www.securityinformed.com',
               'Connection': 'keep-alive',
               'Sec-Ch-Ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
               'Sec-Ch-Ua-Platform': 'Windows',
               'Sec-Fetch-Dest': 'document',
               'Sec-Fetch-Mode': 'navigate',
               'Sec-Fetch-Site': 'same-origin',
               'Sec-Ch-Ua-Mobile': '?0',
               'Sec-Fetch-User': '?1',
               'Upgrade-Insecure-Requests': '1',
               }

    def parse(self, response):
        chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument('--disable-dev-shm-usage')

        categories = ['manufacturers',
                      'distributors',
                      'resellers-dealers-reps',
                      'events-training-services',
                      'installers',
                      'consultants',
                      'systems-integrators',
                      'manned-guarding',
                      ]

        for category in categories:
            page_count = 1

            driver = webdriver.Chrome('chromedriver', options=chrome_options)
            driver.maximize_window()

            data_link = 'https://www.securityinformed.com/international-security-companies.html?type=' + category

            cat_name = category.replace('-', ' ').title()
            driver.get(data_link)
            while True:
                for i in range(120):
                    soup = BS(driver.page_source, "html.parser")
                    div = soup.find('div', {'class': 'section internal-section-23 mt-5'})
                    if not div:
                        time.sleep(1)
                        continue
                    break
                else:
                    self.logger.info("ERROR PAGE CONTENTS, GO FOR NEXT CATEGORY")
                    break

                cookie_txt = ''
                all_cookies = driver.get_cookies()
                for cookie in all_cookies:
                    if 'securityinformed' in cookie['domain']:
                        cookie_txt += f"{cookie['name']}={cookie['value']}; "

                partners = div.find_all("div", {'class': 'row mt-3'})
                self.logger.info(f'Type: {cat_name}, Page number: {page_count}, Results: {len(partners)}')
                for partner in partners:
                    partner_link = partner.find('a', href=True, title=True)['href'] if partner.find('a', href=True, title=True) else None
                    if partner_link:
                        self.logger.info(f'PARTNER LINK: {partner_link}')

                        self.headers['Referer'] = partner_link
                        self.headers['Cookie'] = cookie_txt

                        time.sleep(1)
                        r = requests.request(method="GET", url=partner_link, headers=self.headers)
                        partner_soup = BS(r.text, "html.parser")
                        company_name = partner_soup.find('h1', {'itemprop': 'name'}).text if partner_soup.find('h1', {'itemprop': 'name'}) else None

                        if r.status_code != 200 or not company_name:
                            self.logger.info(f'ERROR REQUEST: {partner_link}')
                            # Open a new window
                            driver.execute_script('''window.open("","_blank");''')
                            driver.switch_to.window(driver.window_handles[-1])
                            for i in range(4):
                                try:
                                    driver.get(partner_link)
                                    break
                                except Exception as e:
                                    self.logger.info('SELENIUM EXCEPTION: ', e)
                                    time.sleep(2)
                            else:
                                driver.close()
                                time.sleep(1)
                                # Switch back to the first tab
                                driver.switch_to.window(driver.window_handles[0])
                                continue

                            partner_soup = BS(driver.page_source, "html.parser")
                            company_name = partner_soup.find('h1', {'itemprop': 'name'}).text if partner_soup.find('h1', {'itemprop': 'name'}) else None
                            if not company_name:
                                driver.close()
                                time.sleep(1)
                                # Switch back to the first tab
                                driver.switch_to.window(driver.window_handles[0])

                                # Initialize item
                                item = dict()
                                for k in self.item_fields:
                                    item[k] = ''

                                item['partner_program_link'] = self.partner_program_link
                                item['partner_directory'] = self.partner_directory
                                item['partner_program_name'] = self.partner_program_name

                                item['partner_company_name'] = partner.find('a', href=True, title=True)['title'] if partner.find('a', href=True, title=True) else ''
                                item['partner_type'] = cat_name
                                item['company_description'] = partner.find('p', {'class': "one-line-text p-0"}).text if partner.find('p', {'class': "one-line-text p-0"}) else ''
                                if item['partner_company_name'] != '':
                                    yield item

                                continue

                            time.sleep(10)
                            cookie_txt = ''
                            all_cookies = driver.get_cookies()
                            for cookie in all_cookies:
                                if 'securityinformed' in cookie['domain']:
                                    cookie_txt += f"{cookie['name']}={cookie['value']}; "
                            self.headers['Cookie'] = cookie_txt

                            driver.close()
                            time.sleep(1)
                            # Switch back to the first tab
                            driver.switch_to.window(driver.window_handles[0])

                        # Initialize item
                        item = dict()
                        for k in self.item_fields:
                            item[k] = ''

                        item['partner_program_link'] = self.partner_program_link
                        item['partner_directory'] = self.partner_directory
                        item['partner_program_name'] = self.partner_program_name

                        item['partner_company_name'] = company_name
                        item['partner_type'] = cat_name
                        item['company_domain_name'] = partner_soup.find('li', {'class': "web-icon"}).find('a', href=True)['href'] if partner_soup.find('li', {'class': "web-icon"}) and partner_soup.find('li', {'class': "web-icon"}).find('a', href=True) else ''
                        url_obj = urllib.parse.urlparse(item['company_domain_name'])
                        item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                        x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                        if x:
                            item['company_domain_name'] = x[-1]
                        if '/' in item['company_domain_name']:
                            item['company_domain_name'] = item['company_domain_name'][:item['company_domain_name'].find('/')]

                        social_links = partner_soup.find_all('a', href=True)
                        for social_link in social_links:
                            txt = social_link['href']
                            if 'linkedin.com/' in txt and txt != 'https://www.linkedin.com/showcase/securityinformed':
                                item['linkedin_link'] = txt
                            if 'twitter.com/' in txt and txt != 'https://twitter.com/SourceSecurity':
                                item['twitter_link'] = txt
                            if 'facebook.com/' in txt and txt != 'https://www.facebook.com/pages/SourceSecuritycom/122726484417962':
                                item['facebook_link'] = txt
                            if 'youtube.com/' in txt and txt != 'https://www.youtube.com/user/sourcesecurity?sub_confirmation=1':
                                item['youtube_link'] = txt

                        item['general_phone_number'] = partner_soup.find('li', {'class': "contact-icon"}).text if partner_soup.find('li', {'class': "contact-icon"}) else ''
                        item['company_description'] = partner_soup.find('p', {'itemprop': "description"}).text if partner_soup.find('p', {'itemprop': "description"}) else ''

                        address = partner_soup.find('li', {'class': "address-icon"})
                        if not address:
                            partner_soup.find('div', {'class': "address-icon"})
                        if address:
                            item['headquarters_street'] = address.find('meta', {'itemprop': "streetAddress"})['content'] if address.find('meta', {'itemprop': "streetAddress"}) else ''
                            item['headquarters_city'] = address.find('meta', {'itemprop': "addressLocality"})['content'] if address.find('meta', {'itemprop': "addressLocality"}) else ''
                            item['headquarters_state'] = address.find('meta', {'itemprop': "addressRegion"})['content'] if address.find('meta', {'itemprop': "addressRegion"}) else ''
                            item['headquarters_zipcode'] = address.find('meta', {'itemprop': "postalCode"})['content'] if address.find('meta', {'itemprop': "postalCode"}) else ''
                            item['headquarters_country'] = address.find('meta', {'itemprop': "addressCountry"})['content'] if address.find('meta', {'itemprop': "addressCountry"}) else ''

                        data_lists = partner_soup.find('div', {'class': 'browse-box'}).find_all('ul') if partner_soup.find('div', {'class': 'browse-box'}) else partner_soup.find_all('ul', {'class': 'p-0'})

                        for lst in data_lists:
                            lines = lst.find_all('li')
                            if len(lines) > 0:
                                if 'companies like' in lines[0].text.lower():
                                    continue
                                elif 'products' in lines[0].text.lower():
                                    item['products'] = list()
                                    for line in lines[1:]:
                                        item['products'].append(line.text)
                                elif 'industries' in lines[0].text.lower():
                                    item['industries'] = list()
                                    for line in lines[1:]:
                                        item['industries'].append(line.text)
                                elif 'certifications' in lines[0].text.lower():
                                    item['certifications'] = list()
                                    for line in lines[1:]:
                                        item['certifications'].append(line.text)
                        yield item

                next_page = None
                if soup.find('ul', {'class': 'pagination'}):
                    if soup.find('ul', {'class': 'pagination'}).find('a', {'rel': 'next'}):
                        next_page = True

                if next_page:
                    if page_count % 20 == 0:
                        self.logger.info("WAIT FOR 2 MINUTES")
                        time.sleep(2 * 60)

                    page_count += 1

                    data_link = f'https://www.securityinformed.com/international-security-companies.html?type={category}&page={page_count}'
                    for i in range(4):
                        try:
                            driver.get(data_link)
                            break
                        except Exception as e:
                            self.logger.info('SELENIUM EXCEPTION: ', e)
                            time.sleep(2)
                    else:
                        try:
                            driver.close()
                            driver.quit()
                        except:
                            pass
                        driver = webdriver.Chrome('chromedriver', options=chrome_options)
                        driver.maximize_window()
                        driver.get(data_link)
                else:
                    self.logger.info('Category finished, start next category')
                    break
            try:
                driver.close()
                driver.quit()
            except:
                pass
            self.logger.info("WAIT FOR 5 MINUTES")
            time.sleep(5 * 60)
