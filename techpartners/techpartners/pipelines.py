# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


from datetime import datetime
import psycopg2
from scrapy.exporters import CsvItemExporter


class SapPipeline:

    # def __init__(self):
    #     # PostGreSQL configuration parameters
    #     self.db_name = 'partners'
    #     self.password = 'password'
    #     self.user = 'postgres'
    #     self.port = '5432'
    #     self.host = "localhost"
    #     self.conn = None
    #     self.cur = None
    #
    #     self.company_id = None
    #     self.crawl_id = None

    def open_spider(self, spider):
        file_path_producer = f'{spider.settings.get("DATA_FILE_PATH")}/{spider.name}_{datetime.now().strftime("%Y-%m-%d_%H.%M.%S")}.csv'
        self.file = open(file_path_producer, 'wb')
        # file_path_producer = f'{spider.settings.get("DATA_FILE_PATH")}/Total_Partners.csv'
        # self.file = open(file_path_producer, 'ab')
        self.exporter = CsvItemExporter(self.file, encoding='utf-8-sig')
        self.exporter.start_exporting()

        # # PostGreSQL Create DB
        # self.create_db(self.db_name)
        #
        # self.conn = psycopg2.connect(host=self.host,
        #                              port=self.port,
        #                              user=self.user,
        #                              password=self.password,
        #                              database=self.db_name)
        # self.conn.autocommit = True
        # self.cur = self.conn.cursor()
        # # Create company table
        # self.create_table_company()
        # # Create crawl table
        # self.create_table_crawl()
        # # Create partner table
        # self.create_table_partner()
        # # Create location table
        # self.create_table_location()
        # # Create service table
        # self.create_table_service()
        # # insert / update company and crawl tables records
        # self.insert_company(spider)

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

        # # PostGreSQL close conn
        # self.conn.commit()
        # self.cur.close()
        # if self.conn:
        #     self.conn.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)

        # # PostGreSQL Insert item
        # self.insert_partner(item)

        return item

    def create_db(self, db_name):
        """
        Create partners PostGreSQL database
        :return:
        """
        conn = None
        try:
            # establishing the connection
            conn = psycopg2.connect(host=self.host,
                                    port=self.port,
                                    user=self.user,
                                    password=self.password)
            conn.autocommit = True

            # Creating a cursor object using the cursor() method
            cursor = conn.cursor()

            # Creating a database / Drop old database if exists
            # cursor.execute("""DROP DATABASE IF EXISTS %s;""" % db_name)
            cursor.execute("""CREATE database %s;""" % db_name)
            print("Database created successfully........")

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        # close communication with the PostgreSQL database server
        if conn:
            conn.close()

    def create_table_company(self):
        sql = """
                    CREATE TABLE IF NOT EXISTS company (
                    id SERIAL PRIMARY KEY,
                    partner_program_link TEXT UNIQUE,
                    partner_directory TEXT,
                    partner_program_name TEXT,
                    UNIQUE (partner_program_link, partner_directory, partner_program_name)
                    )
                    """
        try:
            # Execute DB command
            self.cur.execute(sql)
            print("company table created successfully........")
            # commit the changes
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def create_table_crawl(self):
        sql = """
                    CREATE TABLE IF NOT EXISTS crawl
                    (
                    id INTEGER PRIMARY KEY,
                    company_id INTEGER,
                    created_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    status TEXT,
                    completed_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    Scrapy_Script TEXT,
                    CONSTRAINT fk_company_id FOREIGN KEY (company_id)
                        REFERENCES company (id) MATCH SIMPLE
                        ON UPDATE CASCADE
                        ON DELETE CASCADE
                    )
                    """
        try:
            # Execute DB command
            self.cur.execute(sql)
            print("crawl table created successfully........")
            # commit the changes
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def create_table_partner(self):
        """
        Create partner table with columns: id(auto increment), name, partner_type, raw_website(Not Null), website, tagline, description,phone,
                                            industries, services, solutions, qualifications, country, hq_Location, source
        :param db_name:
        :return:
        """
        sql = """CREATE TABLE IF NOT EXISTS partner
            (
            id SERIAL PRIMARY KEY,
            company_id integer,
            crawl_id integer,
            partner_company_name TEXT,
            product_service_name TEXT,
            company_domain_name TEXT,
            partner_type TEXT,
            partner_tier TEXT,
            company_description TEXT,
            product_service_description TEXT,
            CONSTRAINT fk_company_id FOREIGN KEY (company_id)
                REFERENCES company (id) MATCH SIMPLE
                ON UPDATE CASCADE ON DELETE CASCADE,
            CONSTRAINT fk_crawl_id FOREIGN KEY (crawl_id)
                REFERENCES crawl (id) MATCH SIMPLE
                ON UPDATE CASCADE ON DELETE CASCADE,
            UNIQUE (company_id, partner_company_name, product_service_name)
            )
            """
        try:
            # Execute DB command
            self.cur.execute(sql)
            print("partner table created successfully........")
            # commit the changes
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def create_table_location(self):
        sql = """
                    CREATE TABLE IF NOT EXISTS location (
                    partner_id BIGINT,
                    headquarter BOOLEAN,
                    locations_street TEXT,
                    locations_city TEXT,
                    locations_state TEXT,
                    locations_zipcode TEXT,
                    locations_country TEXT,
                    UNIQUE (locations_street, locations_city, locations_state, locations_zipcode, locations_country),
                    CONSTRAINT fk_location FOREIGN KEY(partner_id) REFERENCES partner(id) ON UPDATE CASCADE ON DELETE CASCADE
                    )
                    """
        try:
            # Execute DB command
            self.cur.execute(sql)
            print("location table created successfully........")
            # commit the changes
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def create_table_service(self):
        sql = """
                    CREATE TABLE IF NOT EXISTS service (
                    partner_id BIGINT,
                    regions TEXT,
                    languages TEXT,
                    products TEXT,
                    services TEXT,
                    solutions TEXT,
                    pricing_plan TEXT,
                    pricing_model TEXT,
                    pricing_plan_description TEXT,
                    pricing TEXT,
                    specializations TEXT,
                    categories TEXT,
                    features TEXT,
                    account_requirements TEXT,
                    product_package_name TEXT,
                    year_founded TEXT,
                    latest_update TEXT,
                    publisher TEXT,
                    partnership_timespan TEXT,
                    partnership_founding_date TEXT,
                    product_version TEXT,
                    product_requirements TEXT,
                    general_phone_number TEXT,
                    general_email_address TEXT,
                    support_phone_number TEXT,
                    support_email_address TEXT,
                    support_link TEXT,
                    help_link TEXT,
                    terms_and_conditions TEXT,
                    license_agreement_link TEXT,
                    privacy_policy_link TEXT,
                    linkedin_link TEXT,
                    twitter_link TEXT,
                    facebook_link TEXT,
                    youtube_link TEXT,
                    instagram_link TEXT,
                    xing_link TEXT,
                    primary_contact_name TEXT,
                    primary_contact_phone_number TEXT,
                    industries TEXT,
                    integrations TEXT,
                    integration_level TEXT,
                    competencies TEXT,
                    partner_programs TEXT,
                    validations TEXT,
                    certifications TEXT,
                    designations TEXT,
                    contract_vehicles TEXT,
                    certified_experts TEXT,
                    certified TEXT,
                    company_size TEXT,
                    company_characteristics TEXT,
                    partner_clients TEXT,
                    notes TEXT,
                    CONSTRAINT fk_service FOREIGN KEY(partner_id) REFERENCES partner(id) ON UPDATE CASCADE ON DELETE CASCADE
                    )
                    """
        try:
            # Execute DB command
            self.cur.execute(sql)
            print("service table created successfully........")
            # commit the changes
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def insert_company(self, spider):
        """ insert or update record into the company and crawl table """

        self.crawl_id = spider.crawl_id

        select_sql = """SELECT id FROM company WHERE (partner_program_link, partner_directory, partner_program_name) = (%s, %s, %s) LIMIT 1;"""

        try:
            self.cur.execute(select_sql, (spider.partner_program_link, spider.partner_directory, spider.partner_program_name))
            data = self.cur.fetchone()
            if data:
                self.company_id = data[0]
        except (Exception, psycopg2.DatabaseError) as error:
            print('COMPANY SELECT: ', error)

        if not self.company_id:
            insert_sql = """INSERT INTO company (partner_program_link, partner_directory, partner_program_name) 
                     VALUES(%s, %s, %s) RETURNING id;"""
            try:
                self.cur.execute(insert_sql, (spider.partner_program_link, spider.partner_directory, spider.partner_program_name))
                data = self.cur.fetchone()
                if data:
                    self.company_id = data[0]
            # except (Exception, psycopg2.IntegrityError) as error:
            #     pass
            except (Exception, psycopg2.DatabaseError) as error:
                print('COMPANY INSERT: ', error)

        if self.company_id:
            update_sql = """INSERT INTO crawl(id, company_id, created_date, status, completed_date, Scrapy_Script) 
                     VALUES(%s, %s, %s, %s, %s, %s)
                     ON CONFLICT (id) DO UPDATE SET 
                     (id, company_id, created_date, status, completed_date, Scrapy_Script) = (crawl.id, 
                     crawl.company_id, crawl.created_date, EXCLUDED.status, EXCLUDED.completed_date, EXCLUDED.Scrapy_Script);"""
            try:
                self.cur.execute(update_sql, (spider.crawl_id, self.company_id, datetime.now(), 'Ok', datetime.now(), spider.name))
            # except (Exception, psycopg2.IntegrityError) as error:
            #     pass
            except (Exception, psycopg2.DatabaseError) as error:
                print('CRAWL INSERT: ', error)

    def insert_partner(self, item):
        """ insert new record into the partner table """

        partner_id = None
        # select_sql = """SELECT id FROM partner WHERE (company_id, partner_company_name, product_service_name) = (%s, %s, %s) LIMIT 1;"""
        #
        # try:
        #     self.cur.execute(select_sql, (self.company_id, item['partner_company_name'], item['product_service_name'] if 'product_service_name' in item else ''))
        #     data = self.cur.fetchone()
        #     if data:
        #         partner_id = data[0]
        #         print(partner_id)
        # except (Exception, psycopg2.DatabaseError) as error:
        #     print('PARTNER SELECT: ', error)
        #
        # if partner_id:
        #     delete_sql = """DELETE FROM partner WHERE (company_id, partner_company_name, product_service_name) = (%s, %s, %s) RETURNING id;"""
        #
        #     try:
        #         self.cur.execute(delete_sql, (self.company_id, item['partner_company_name'], item['product_service_name'] if 'product_service_name' in item else ''))
        #         data = self.cur.fetchone()
        #         if data:
        #             partner_id = None
        #     except (Exception, psycopg2.DatabaseError) as error:
        #         print('PARTNER DELETE: ', error)

        if not partner_id:
            insert_sql = """INSERT INTO partner(company_id, crawl_id, partner_company_name, product_service_name,
                            company_domain_name, partner_type, partner_tier, company_description,
                             product_service_description) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                             ON CONFLICT (company_id, partner_company_name, product_service_name)
                             DO UPDATE SET (company_id, crawl_id, partner_company_name, product_service_name,
                             company_domain_name, partner_type, partner_tier, company_description,
                             product_service_description) = (EXCLUDED.company_id, EXCLUDED.crawl_id, EXCLUDED.partner_company_name,
                             EXCLUDED.product_service_name, EXCLUDED.company_domain_name, EXCLUDED.partner_type,
                             EXCLUDED.partner_tier, EXCLUDED.company_description, EXCLUDED.product_service_description) RETURNING id;"""
            try:
                self.cur.execute(insert_sql, (self.company_id, self.crawl_id, item['partner_company_name'],
                                              item['product_service_name'] if 'product_service_name' in item else '',
                                              item['company_domain_name'],
                                              item['partner_type'], item['partner_tier'],
                                              item['company_description'],
                                              item['product_service_description'] if 'product_service_description' in item else ''))
                data = self.cur.fetchone()
                if data:
                    partner_id = data[0]
            # except (Exception, psycopg2.IntegrityError) as error:
            #     pass
            except (Exception, psycopg2.DatabaseError) as error:
                print('PARTNER INSERT: ', error)

        if partner_id:
            insert_sql = """INSERT INTO location (partner_id, headquarter, locations_street, locations_city,
                            locations_state, locations_zipcode, locations_country) 
                            VALUES(%s, %s, %s, %s, %s, %s, %s);"""
            try:
                if item['headquarters_street'] != '' or item['headquarters_city'] != ''\
                        or item['headquarters_state'] != '' or item['headquarters_zipcode'] != ''\
                        or item['headquarters_country']:
                    self.cur.execute(insert_sql, (partner_id, True,
                                                  item['headquarters_street'], item['headquarters_city'],
                                                  item['headquarters_state'], item['headquarters_zipcode'],
                                                  item['headquarters_country']))
            except (Exception, psycopg2.IntegrityError) as error:
                pass
            except (Exception, psycopg2.DatabaseError) as error:
                print('HEADQUARTER INSERT: ', error)
            try:
                self.cur.execute(insert_sql, (partner_id, False,
                                              item['locations_street'], item['locations_city'],
                                              item['locations_state'], item['locations_zipcode'],
                                              item['locations_country']))
            except (Exception, psycopg2.IntegrityError) as error:
                pass
            except (Exception, psycopg2.DatabaseError) as error:
                print('LOCATION INSERT: ', error)

            insert_sql = """INSERT INTO service(partner_id, regions, languages, products, services, solutions,
                          pricing_plan, pricing_model, pricing_plan_description, pricing, specializations, categories,
                          features, account_requirements, product_package_name, year_founded, latest_update, publisher,
                          partnership_timespan, partnership_founding_date, product_version, product_requirements,
                          general_phone_number, general_email_address, support_phone_number, support_email_address,
                          support_link, help_link, terms_and_conditions, license_agreement_link, privacy_policy_link,
                          linkedin_link, twitter_link, facebook_link, youtube_link, instagram_link, xing_link,
                          primary_contact_name, primary_contact_phone_number, industries, integrations, integration_level, competencies, 
                          partner_programs, validations, certifications, designations, contract_vehicles,
                          certified_experts, certified, company_size, company_characteristics, partner_clients, notes)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s);"""
            try:
                self.cur.execute(insert_sql, (partner_id, item['regions'], item['languages'], item['products'] if 'products' in item else item['product'],
                                     item['services'], item['solutions'], item['pricing_plan'] if 'pricing_plan' in item else '',
                                     item['pricing_model'] if 'pricing_model' in item else '',
                                     item['pricing_plan_description'] if 'pricing_plan_description' in item else '',
                                     item['pricing'],
                                     item['specializations'], item['categories'],
                                     item['features'] if 'features' in item else '',
                                     item['account_requirements'] if 'account_requirements' in item else '',
                                     item['product_package_name'] if 'product_package_name' in item else '',
                                     item['year_founded'] if 'year_founded' in item else '',
                                     item['latest_update'] if 'latest_update' in item else '',
                                     item['publisher'] if 'publisher' in item else '',
                                     item['partnership_timespan'] if 'partnership_timespan' in item else '',
                                     item['partnership_founding_date'] if 'partnership_founding_date' in item else '',
                                     item['product_version'] if 'product_version' in item else '',
                                     item['product_requirements'] if 'product_requirements' in item else '',
                                     item['general_phone_number'], item['general_email_address'],
                                     item['support_phone_number'] if 'support_phone_number' in item else '',
                                     item['support_email_address'] if 'support_email_address' in item else '',
                                     item['support_link'] if 'support_link' in item else '',
                                     item['help_link'] if 'help_link' in item else '',
                                     item['terms_and_conditions'] if 'terms_and_conditions' in item else '',
                                     item['license_agreement_link'] if 'license_agreement_link' in item else '',
                                     item['privacy_policy_link'] if 'privacy_policy_link' in item else '',
                                     item['linkedin_link'] if 'linkedin_link' in item else '',
                                     item['twitter_link'] if 'twitter_link' in item else '',
                                     item['facebook_link'] if 'facebook_link' in item else '',
                                     item['youtube_link'] if 'youtube_link' in item else '',
                                     item['instagram_link'] if 'instagram_link' in item else '',
                                     item['xing_link'] if 'xing_link' in item else '',
                                     item['primary_contact_name'], item['primary_contact_phone_number'] if 'primary_contact_phone_number' in item else '', item['industries'],
                                     item['integrations'] if 'integrations' in item else '',
                                     item['integration_level'] if 'integration_level' in item else '',
                                     item['competencies'] if 'competencies' in item else '',
                                     item['partner_programs'] if 'partner_programs' in item else '',
                                     item['validations'] if 'validations' in item else '',
                                     item['certifications'] if 'certifications' in item else '',
                                     item['designations'] if 'designations' in item else '',
                                     item['contract_vehicles'] if 'contract_vehicles' in item else '',
                                     item['certified_experts'] if 'certified_experts' in item else '',
                                     item['certified'] if 'certified' in item else '',
                                     item['company_size'] if 'company_size' in item else '',
                                     item['company_characteristics'] if 'company_characteristics' in item else '',
                                     item['partner_clients'] if 'partner_clients' in item else '',
                                     item['notes'] if 'notes' in item else '',))
            except (Exception, psycopg2.IntegrityError) as error:
                pass
            except (Exception, psycopg2.DatabaseError) as error:
                print('SERVICE INSERT: ', error)
