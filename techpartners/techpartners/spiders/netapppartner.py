# import needed libraries
import json
import math
import re
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'netapppartner'
    partner_program_link = 'https://www.netapp.com/partners/partner-connect/#t=Partners&sort=%40partnerweight%20descending&layout=card&f:@facet_language_mktg=[English]'
    partner_directory = 'Netapp Partner Directory'
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

    api_link = 'https://platform.cloud.coveo.com/rest/search/v2?organizationId=netappproductiono5s3vzkp'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'Connection': 'keep-alive',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Authority': 'platform.cloud.coveo.com',
        'Origin': 'https://www.netapp.com',
        'Referer': 'https://www.netapp.com/',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        }

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
            return

        soup = BS(response.text, "html.parser")
        scripts = soup.find('head').find_all('script')
        for script in scripts:
            if re.search(r"searchToken\s?=", script.text):
                content = script.text[script.text.find('searchToken'):]
                content = content[content.find('=') + 1:]
                content = content[: content.find(';')]
                searchToken = content.strip().strip('"').strip()
                break
        else:
            searchToken = ''

        firstResult = 0
        numberOfResults = 100

        payload = f'actionsHistory=%5B%5D&referrer=&visitorId=&isGuestUser=false&cq=%40ogtype%3D%3D%22Partner%20Connect%22&tab=Partners&locale=en&pipeline=NetApp%20Marketing%20Search%20Pipeline&wildcards=true&firstResult={firstResult}&numberOfResults={numberOfResults}&excerptLength=200&filterField=%40foldingcollection&filterFieldRange=1&parentField=%40foldingchild&childField=%40foldingparent&fieldsToInclude=%5B%22%40foldingcollection%22%2C%22%40foldingparent%22%2C%22%40foldingchild%22%2C%22title%22%2C%22facet_contenttype_mktg%22%2C%22author%22%2C%22publish_date_mktg%22%2C%22source%22%2C%22sourcetype%22%2C%22outlookformacuri%22%2C%22outlookuri%22%2C%22connectortype%22%2C%22urihash%22%2C%22collection%22%2C%22language%22%2C%22objecttype%22%2C%22permanentid%22%2C%22pageimage%22%2C%22facet_specsolution_mktg%22%2C%22facet_specservices_mktg%22%2C%22partner_service_count%22%2C%22limessageauthor%22%2C%22product%22%5D&enableDidYouMean=true&sortCriteria=%40partnerweight%20descending&queryFunctions=%5B%5D&rankingFunctions=%5B%5D&facets=%5B%7B%22facetId%22%3A%22%40facet_industry_mktg%22%2C%22field%22%3A%22facet_industry_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_product_mktg%22%2C%22field%22%3A%22facet_product_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_language_mktg%22%2C%22field%22%3A%22facet_language_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%7B%22value%22%3A%22English%22%2C%22state%22%3A%22idle%22%7D%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40geo%22%2C%22field%22%3A%22geo%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_areaserved_mktg%22%2C%22field%22%3A%22facet_areaserved_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40country_region%22%2C%22field%22%3A%22country_region%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_partnertype_mktg%22%2C%22field%22%3A%22facet_partnertype_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_specsolution_mktg%22%2C%22field%22%3A%22facet_specsolution_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_specservices_mktg%22%2C%22field%22%3A%22facet_specservices_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_techsolution_mktg%22%2C%22field%22%3A%22facet_techsolution_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%5D&facetOptions=%7B%7D&categoryFacets=%5B%5D&retrieveFirstSentences=true&timezone=Africa%2FCairo&enableQuerySyntax=false&enableDuplicateFiltering=false&enableCollaborativeRating=false&debug=false&allowQueriesWithoutKeywords=true'
        self.headers['Authorization'] = 'Bearer ' + searchToken
        yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse_page, body=payload,
                             headers=self.headers,
                             meta={'firstResult': firstResult, 'numberOfResults': numberOfResults},
                             dont_filter=True)

    def parse_page(self, response):
        firstResult = response.meta['firstResult']
        numberOfResults = response.meta['numberOfResults']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
            return
        data, totalCount = None, None
        try:
            data = json.loads(response.text)
            if 'totalCount' in data:
                totalCount = data['totalCount']
        except Exception as e:
            print('ERROR PARSE PAGE: ', e)
            return

        if data and totalCount:
            partners = data['results']
            for partner in partners:

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item['partner_company_name'] = partner['title'] if 'title' in partner else ''
                # item['partner_type'] = partner['raw']['facet_contenttype_mktg'] if 'raw' in partner and 'facet_contenttype_mktg' in partner['raw'] else ''
                # item['solutions'] = partner['raw']['facet_specsolution_mktg'] if 'raw' in partner and 'facet_specsolution_mktg' in partner['raw'] else ''
                # item['services'] = partner['raw']['facet_specservices_mktg'] if 'raw' in partner and 'facet_specservices_mktg' in partner['raw'] else ''
                item['languages'] = partner['raw']['language'] if 'raw' in partner and 'language' in partner['raw'] else ''

                partner_link = partner['uri']
                yield scrapy.Request(method='GET', url=partner_link, callback=self.parse_partner,
                                     meta={'item': item},
                                     headers=self.headers, dont_filter=True)

            # follow next page
            if firstResult == 0 and len(partners) >= numberOfResults and totalCount > numberOfResults:
                np = math.ceil(totalCount/numberOfResults)
                for i in range(1, np+1):
                    firstResult = i * numberOfResults
                    payload = f'actionsHistory=%5B%5D&referrer=&visitorId=&isGuestUser=false&cq=%40ogtype%3D%3D%22Partner%20Connect%22&tab=Partners&locale=en&pipeline=NetApp%20Marketing%20Search%20Pipeline&wildcards=true&firstResult={firstResult}&numberOfResults={numberOfResults}&excerptLength=200&filterField=%40foldingcollection&filterFieldRange=1&parentField=%40foldingchild&childField=%40foldingparent&fieldsToInclude=%5B%22%40foldingcollection%22%2C%22%40foldingparent%22%2C%22%40foldingchild%22%2C%22title%22%2C%22facet_contenttype_mktg%22%2C%22author%22%2C%22publish_date_mktg%22%2C%22source%22%2C%22sourcetype%22%2C%22outlookformacuri%22%2C%22outlookuri%22%2C%22connectortype%22%2C%22urihash%22%2C%22collection%22%2C%22language%22%2C%22objecttype%22%2C%22permanentid%22%2C%22pageimage%22%2C%22facet_specsolution_mktg%22%2C%22facet_specservices_mktg%22%2C%22partner_service_count%22%2C%22limessageauthor%22%2C%22product%22%5D&enableDidYouMean=true&sortCriteria=%40partnerweight%20descending&queryFunctions=%5B%5D&rankingFunctions=%5B%5D&facets=%5B%7B%22facetId%22%3A%22%40facet_industry_mktg%22%2C%22field%22%3A%22facet_industry_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_product_mktg%22%2C%22field%22%3A%22facet_product_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_language_mktg%22%2C%22field%22%3A%22facet_language_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%7B%22value%22%3A%22English%22%2C%22state%22%3A%22idle%22%7D%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40geo%22%2C%22field%22%3A%22geo%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_areaserved_mktg%22%2C%22field%22%3A%22facet_areaserved_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40country_region%22%2C%22field%22%3A%22country_region%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_partnertype_mktg%22%2C%22field%22%3A%22facet_partnertype_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_specsolution_mktg%22%2C%22field%22%3A%22facet_specsolution_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_specservices_mktg%22%2C%22field%22%3A%22facet_specservices_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%2C%7B%22facetId%22%3A%22%40facet_techsolution_mktg%22%2C%22field%22%3A%22facet_techsolution_mktg%22%2C%22type%22%3A%22specific%22%2C%22injectionDepth%22%3A1000%2C%22filterFacetCount%22%3Afalse%2C%22currentValues%22%3A%5B%5D%2C%22numberOfValues%22%3A5%2C%22freezeCurrentValues%22%3Afalse%2C%22preventAutoSelect%22%3Afalse%2C%22isFieldExpanded%22%3Afalse%7D%5D&facetOptions=%7B%7D&categoryFacets=%5B%5D&retrieveFirstSentences=true&timezone=Africa%2FCairo&enableQuerySyntax=false&enableDuplicateFiltering=false&enableCollaborativeRating=false&debug=false&allowQueriesWithoutKeywords=true'
                    yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse_page, body=payload,
                                         headers=self.headers,
                                         meta={'firstResult': firstResult, 'numberOfResults': numberOfResults},
                                         dont_filter=True)

    def parse_partner(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
            return
        item = response.meta['item']
        soup = BS(response.text, "html.parser")

        partenr_data = soup.find('n-partner-services')
        if partenr_data:

            item['partner_type'] = soup.find('meta', {'name': "partnerType"})['content'] if soup.find('meta', {'name': "partnerType"}) else ''
            item['regions'] = soup.find('meta', {'name': "areaServed"})['content'] if soup.find('meta', {'name': "areaServed"}) else ''
            item['services'] = soup.find('meta', {'name': "serviceType"})['content'] if soup.find('meta', {'name': "serviceType"}) else ''
            item['specializations'] = soup.find('meta', {'name': "partnerSpecializations"})['content'] if soup.find('meta', {'name': "partnerSpecializations"}) else ''
            item['solutions'] = soup.find('meta', {'name': "techSolution"})['content'] if soup.find('meta', {'name': "techSolution"}) else ''

            txt = soup.find('n-partner-description')
            if txt:
                item['company_description'] = txt.find('n-content').text if txt.find('n-content') else ''

            sections = partenr_data.find_all('section')
            for section in sections:
                if 'Website' in section.text:
                    item['company_domain_name'] = section.find('a', {'href': True})['href'] if section.find('a', {'href': True}) else ''
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
                elif 'Headquarters' in section.text:
                    locations = section.find_all('li')
                    for location in locations:
                        address = location.text
                        if ',' in address:
                            item['headquarters_city'] = address[:address.rfind(',')]
                            item['headquarters_country'] = address[address.rfind(',')+1:]
                        else:
                            item['headquarters_address'] = address

        yield item
