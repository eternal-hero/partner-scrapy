from techpartners.spiders.base_spider import BaseSpider
import scrapy,requests,string,json
from techpartners.functions import *
from price_parser import Price

domain = 'https://appexchange.salesforce.com'
class Spider(BaseSpider):
    name = 'salesforce'
    partner_program_link = 'https://appexchange.salesforce.com/appxStore?type=App'
    partner_directory = 'Salesforce Directory'
    partner_program_name = ''
    crawl_id = 1291

    start_urls=['https://appexchange.salesforce.com/appxStore?type=App']
    TMP=['partner_program_link','partner_directory','partner_program_name','partner_company_name','product/service_name','company_domain_name','partner_type','partner_tier','company_description','headquarters_street','headquarters_city','headquarters_state','headquarters_zipcode','headquarters_country','locations_street','locations_city','locations_state','locations_zipcode','locations_country','regions','languages','services','solutions','product','pricing','specializations','categories','year_founded','general_phone_number','general_email_address','linkedin_link','twitter_link','facebook_link','primary_contact_name','competencies','partner_programs','validations','certifications','designations','industries','partner_clients','notes']
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0','Accept': '*/*','Accept-Language': 'en-GB,en;q=0.5','Accept-Encoding': 'gzip, deflate, br','Referer': 'https://appexchange.salesforce.com/appxStore','Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8','Origin': 'https://appexchange.salesforce.com','Connection': 'keep-alive','Sec-Fetch-Dest': 'empty','Sec-Fetch-Mode': 'cors','Sec-Fetch-Site': 'same-origin',}
    IDS=[]
    def parse(self, response):
        url='https://appexchange.salesforce.com/appxStore'
        DATA=response.xpath('//fieldset[@data-cutoff]')
        DATA_SET={}
        for i in range(len(DATA)):
            NAME=DATA[i].xpath('./legend/span/text()').get()
            DATA_SET[NAME]={}
            DATA_SET[NAME]['LABEL']=DATA[i].xpath('.//label//span[contains(@class,"__label")]/text()').getall()
            DATA_SET[NAME]['ID']=DATA[i].xpath('.//input/@id').getall()
            DATA_SET[NAME]['DATA']=DATA[i].xpath('.//input/@data-value').getall()
        # Block
        for i in range(len(DATA_SET['Prices']['ID'])):
            CATE=DATA_SET['Prices']['LABEL'][i]
            for j in range(len(DATA_SET['Editions']['ID'])):
                CATE1=DATA_SET['Editions']['LABEL'][j]
                data = {'AJAXREQUEST': '_viewRoot','j_id0:AppxLayout:j_id990:j_id991:filtersForm': 'j_id0:AppxLayout:j_id990:j_id991:filtersForm',str(DATA_SET['Prices']['ID'][i]): 'on',str(DATA_SET['Editions']['ID'][j]): 'on','store-ratings-option1': 'on','store-ratings-option2': 'on','store-ratings-option3': 'on','store-ratings-option4': 'on','store-ratings-option5': 'on','store-ratings-option6': 'on','store-languages-option1': 'on','store-languages-option2': 'on','store-languages-option3': 'on','store-languages-option4': 'on','store-languages-option5': 'on','store-languages-option6': 'on','store-languages-option7': 'on','store-languages-option8': 'on','store-languages-option9': 'on','store-languages-option10': 'on','store-languages-option11': 'on','store-languages-option12': 'on','store-languages-option13': 'on','store-languages-option14': 'on','store-languages-option15': 'on','store-languages-option16': 'on','com.salesforce.visualforce.ViewState': str(response.xpath('//input[@id="com.salesforce.visualforce.ViewState"]/@value').get()),'com.salesforce.visualforce.ViewStateVersion': str(response.xpath('//input[@id="com.salesforce.visualforce.ViewStateVersion"]/@value').get()),'com.salesforce.visualforce.ViewStateMAC': str(response.xpath('//input[@id="com.salesforce.visualforce.ViewStateMAC"]/@value').get()),'j_id0:AppxLayout:j_id990:j_id991:filtersForm:j_id1002': 'j_id0:AppxLayout:j_id990:j_id991:filtersForm:j_id1002','isReset': 'false','filtersUrl': str(DATA_SET['Prices']['DATA'][i])+','+str(DATA_SET['Editions']['DATA'][j])+',rt5,rt4,rt3,rt2,rt1,rt0,lg=nl,lg=en,lg=fi,lg=fr,lg=de,lg=da,lg=it,lg=ja,lg=ko,lg=pt,lg=ru,lg=zh_CN,lg=es,lg=sv,lg=th,lg=zh_TW'}
                response_requests = requests.post(url, headers=self.headers, data=data)
                response_requests=scrapy.Selector(text=response_requests.text)
                Data=response_requests.xpath('//li/a[@data-listing-id]/@href').getall()
                # Goto get content
                for link in Data:
                    if link not in self.IDS:
                        self.IDS.append(link)
                        yield scrapy.Request(link,callback=self.parse_product,dont_filter=True,meta={'CATE':CATE})
                BUTTON=response_requests.xpath('//button[contains(@id,"appx-load-more-button-")]/@onclick').getall()
                if len(BUTTON)>0 and len(Data)>=10:
                    for button in BUTTON:
                        if 'ListingsJS()' in str(button):
                            acc=str(button).split('ListingsJS()')[0]
                        else:
                            acc='Level'+str(button).split('Level')[1].split('(')[0]
                        control=response.xpath('//script[@id and contains(text(),"'+acc+'")]/@id').get()
                        RUN=True
                        page=1
                        while RUN:
                            page+=1
                            page_data = {'AJAXREQUEST': '_viewRoot','j_id0:AppxLayout:actionsForm': 'j_id0:AppxLayout:actionsForm','com.salesforce.visualforce.ViewState': str(response_requests.xpath('//input[@id="com.salesforce.visualforce.ViewState"]/@value').get()),'com.salesforce.visualforce.ViewStateVersion': str(response_requests.xpath('//input[@id="com.salesforce.visualforce.ViewStateVersion"]/@value').get()),'com.salesforce.visualforce.ViewStateMAC': str(response_requests.xpath('//input[@id="com.salesforce.visualforce.ViewStateMAC"]/@value').get()),str(control): str(control)}
                            response_requests = requests.post('https://appexchange.salesforce.com/appxStore', headers=self.headers, data=page_data)
                            response_requests=scrapy.Selector(text=response_requests.text)
                            page_Data=response_requests.xpath('//li/a[@data-listing-id]/@href').getall()
                            for link in page_Data:
                                if link not in self.IDS:
                                    self.IDS.append(link)
                                    yield scrapy.Request(link,callback=self.parse_product,dont_filter=True,meta={'CATE':CATE})
                            if len(page_Data)<10:
                                RUN=False                    
    def parse_product(self,response):
        item={}
        for k in self.TMP:
            item[k]=''
        NAME=response.xpath('//h1[@class="appx-page-header-2_title"]/text()').get()
        item['partner_company_name']=response.xpath('//p[@class="appx-page-header-link"]/text()').get()
        if str(item['partner_company_name']).startswith('By '):
            item['partner_company_name']=str(item['partner_company_name'])[3:]
        CATE=response.xpath('//div[@class="appx-detail-section-categories"]/a/strong/text()').getall()
        item['categories']=', '.join(CATE)
        industries=response.xpath('//div[@class="appx-detail-section-industries"]/a/span/text()').getall()
        item['industries']=', '.join(industries)
        item['product/service_name']=str(NAME).strip()
        PRICE_TXT=response.xpath('//p[@class="appx-pricing-detail-header"]/span[contains(@id,"planCharges")]/text()').get()
        PRICE = Price.fromstring(PRICE_TXT)
        if not PRICE.amount is None:
            item['pricing']=(str(PRICE.currency)+str(PRICE.amount)).replace('USD','$')
        COND=cleanhtml(response.xpath('//div[contains(@class,"appx-form-element__static")]').get())
        item['terms_and_conditions']=COND
        url='https://appexchange.salesforce.com/AppxListingDetailOverviewTab?listingId='+str(response.url).split('listingId=')[1]
        yield scrapy.Request(url,callback=self.parse_content,meta={'item':item,'URL':response.url},dont_filter=True)
    def parse_content(self,response):
        item=response.meta['item']
        DATA=response.xpath('//div[@class="appx-extended-detail-subsection-label-description"]')
        DATASET={}
        for ROW in DATA:
            TITLE=ROW.xpath('./div[@class="appx-extended-detail-subsection-label"]/text()').get()
            if TITLE:
                TXT=cleanhtml(ROW.xpath('./div[contains(@class,"appx-extended-detail-subsection-description")]').get())
                DATASET[TITLE]=kill_space(TXT)
            else:
                TEL=ROW.xpath('./div[@id]/text()').get()
                if TEL and len(Get_Number(TEL))>5:
                    DATASET['TEL']=str(TEL).split(',')[0]
                EMAIL=ROW.xpath('./div[@id]//a[contains(@href,"mailto:")]/@href').get()
                if EMAIL:
                    DATASET['EMAIL']=str(EMAIL).replace('mailto:','')
                LINKS=ROW.xpath('.//a')
                for LINK in LINKS:
                    TXT=str(LINK.xpath('./text()').get()).strip()
                    HREF=str(LINK.xpath('./@href').get()).strip()
                    DATASET[TXT]=HREF
        
        item['partner_program_link']='https://appexchange.salesforce.com'
        partner_company_name=response.xpath('//div[@class="appx-company-name"]/text()').get()
        if partner_company_name and item['partner_company_name']=='' or item['partner_company_name']=='None':
            item['partner_company_name']=str(partner_company_name).strip()
        DES=response.xpath('//p[@class="appx-extended-detail-company-description"]/text()').getall()
        item['company_description']=' \n '.join(DES)
        ADD=response.xpath('//div[@class="appx-extended-detail-subsection-segment "]//div[contains(@id,"AppxListingDetailOverviewTab")]/text()').get()
        ADD=str(ADD).split(',')
        if len(ADD)>0:
            item['headquarters_city']=ADD[0]
            if len(ADD)>1:
                if len(str(ADD[1]).strip())==2:
                    item['headquarters_state']=ADD[1]
                else:
                    item['headquarters_city']+=', '+ADD[1]
                if len(ADD)>2:
                    item['headquarters_country']=ADD[2]
        if 'Founded' in DATASET:
            item['year_founded']=DATASET['Founded']
        if 'Phone' in DATASET:
            item['general_phone_number']=DATASET['Phone']
        if 'Email' in DATASET:
            item['general_email_address']=DATASET['Email']
        if 'Website' in DATASET:
            item['company_domain_name']=DATASET['Website']
            if '//' in item['company_domain_name'] or 'www.' in item['company_domain_name']:
                DOMAIN=re.split('www.|//',item['company_domain_name'])
                item['company_domain_name']=DOMAIN[len(DOMAIN)-1].split('/')[0]
                if 'salesforce.com' in item['company_domain_name']:
                    item['company_domain_name']='salesforce.com'
        #HightLights=response.xpath('//div[contains(@class,"appx-extended-detail-highlights")]/ul/li//span[contains(@id,"appxListingDetailOverviewTabComp")]/text()').getall()
        #item['product/service_name']='; '.join(HightLights)
        item['product/service_description']=cleanhtml(response.xpath('//div[contains(@class,"appx-extended-detail-description")]').get())
        if 'Salesforce Edition' in DATASET:
            item['partner_tier']=DATASET['Salesforce Edition']
        DATAS=response.xpath('//div[@class="appx-extended-detail-subsection-segment"]/span')
        for ROW in DATAS:
            TITLE=ROW.xpath('./p[@class="slds-section__title"]/text()').get()
            if TITLE:
                TITLE=str(TITLE).strip()
                DT=[]
                VALUES=ROW.xpath('.//li//span[not(@class="appx-tooltip-text")]/text()').getall()
                for rs in VALUES:
                    rs=str(rs).strip()
                    if rs!='':
                        DT.append(rs)
                VALUES='; '.join(DT)
                DATASET[TITLE]=VALUES
        if 'Features' in DATASET:
            item['features']=DATASET['Features']
        else:
            item['features']=''
        if 'Package Name' in DATASET:
            item['product_package_name']=DATASET['Package Name']
        else:
            item['product_package_name']=''
        if 'Latest Release' in DATASET:
            item['latest_update']=DATASET['Latest Release']
        else:
            item['latest_update']=''
        if 'Listed On' in DATASET:
            item['partnership_founding_date']=DATASET['Listed On']
        else:
            item['partnership_founding_date']==''
        if 'Version' in DATASET:
            item['product_version']=DATASET['Version']
        else:
            item['product_version']=''
        if 'Other System Requirements' in DATASET:
            item['product_requirements']=DATASET['Other System Requirements']
        else:
            item['product_requirements']=''
        if 'TEL' in DATASET:
            item['support_phone_number']=DATASET['TEL']
        else:
            item['support_phone_number']=''
        if 'EMAIL' in DATASET:
            item['support_email_address']=DATASET['EMAIL']
        else:
            item['support_email_address']=''
        if 'Knowledge Base' in DATASET:
            item['help_link']=DATASET['Knowledge Base']
        else:
            item['help_link']=''
        if 'Languages' in DATASET:
            item['languages']=DATASET['Languages']
        if str(item['product/service_name']).strip()!='':
            item['URL']=response.meta['URL']
            yield(item)
        else:
            print(response.url)
