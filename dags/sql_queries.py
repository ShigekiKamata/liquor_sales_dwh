import configparser, os


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_CRIME_DATA = config.get('S3', 'crime_data')
S3_CENSUS_DATA = config.get('S3', 'county_census_data')
S3_TEMPERATURE_DATA = config.get('S3', 'temperature_data')
S3_LIQUOR_SALES_DATA = config.get('S3', 'liquor_sales_data')
DWH_IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_liquor_sales_table_drop = "DROP TABLE IF EXISTS staging_liquor_sales;"
staging_temperature_table_drop = "DROP TABLE IF EXISTS staging_temperature;"
staging_census_table_drop = "DROP TABLE IF EXISTS staging_census;"
staging_crime_table_drop = "DROP TABLE IF EXISTS staging_crime;"

sales_fact_table_drop = "DROP TABLE IF EXISTS sales_fact;"
store_dim_table_drop = "DROP TABLE IF EXISTS store_dim;"
item_dim_table_drop = "DROP TABLE IF EXISTS item_dim;"
time_dim_table_drop = "DROP TABLE IF EXISTS time_dim;"
temperature_dim_table_drop = "DROP TABLE IF EXISTS temperature_dim;"
county_census_dim_table_drop = "DROP TABLE IF EXISTS county_census_dim;"

# CREATE TABLES
staging_liquor_sales_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_liquor_sales (
        invoice_num         VARCHAR(30),
        date                DATE,
        store_num           INTEGER,
        store_name          VARCHAR(100),
        address             VARCHAR(200),
        city                VARCHAR(25),
        zip                 VARCHAR(10),
        store_location      VARCHAR(200),
        county_num          INTEGER,
        county_name         VARCHAR(25),
        category            VARCHAR,
        category_name       VARCHAR(100),
        vendor_num          INTEGER,
        vendor_name         VARCHAR(50),
        item_num            INTEGER,
        item_name           VARCHAR(100),
        pack                INTEGER,
        bottle_volume       INTEGER,
        state_bottle_cost   VARCHAR(10),
        state_bottle_retail VARCHAR(10),
        bottle_sold         INTEGER,
        sale                VARCHAR(12),
        volume_sold_ltr     NUMERIC,
        volume_sold_gal     NUMERIC  
    );
""")

staging_temperature_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_temperature (
        region      VARCHAR(20),
        country     VARCHAR(50),
        state       VARCHAR(20),
        city        VARCHAR(30),
        month       INTEGER,
        day         INTEGER,
        year        INTEGER,
        temperature DECIMAL(4,1)
    );
""")

staging_census_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_census (
        county_id          INTEGER,
        state              VARCHAR(30),
        county             VARCHAR(80),
        total_pop          INTEGER,
        men                INTEGER,
        women              INTEGER,
        hispanic           DECIMAL(4,1),
        white              DECIMAL(4,1),
        black              DECIMAL(4,1),
        native             DECIMAL(4,1),
        asian              DECIMAL(4,1),
        pacific            DECIMAL(4,1),
        voting_age_citizen INTEGER,
        income             INTEGER,
        income_err         INTEGER,
        income_per_cap     INTEGER,
        income_per_cap_err INTEGER,
        poverty            DECIMAL(4,1),
        child_poverty      DECIMAL(4,1),
        professional       DECIMAL(4,1),
        service            DECIMAL(4,1),
        office             DECIMAL(4,1),
        construction       DECIMAL(4,1),
        production         DECIMAL(4,1),
        drive              DECIMAL(4,1),
        carpool            DECIMAL(4,1),
        transit            DECIMAL(4,1),
        walk               DECIMAL(4,1),
        other_transp       DECIMAL(4,1),
        work_at_home       DECIMAL(4,1),
        mean_commute       DECIMAL(4,1),
        employed           INTEGER,
        private_work       DECIMAL(4,1),
        public_work        DECIMAL(4,1),
        self_employed      DECIMAL(4,1),
        family_work        DECIMAL(4,1),
        unemployment       DECIMAL(4,1)
    );
""")

staging_crime_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_crime (
        county_name           VARCHAR(50),
        crime_rate_per_100000 DECIMAL(7,1),
        index_1               INTEGER,
        edition               INTEGER,
        part                  INTEGER,
        idno                  INTEGER,
        cpoparst              INTEGER,
        cpopcrim              INTEGER,
        ag_arrst              INTEGER,
        ag_off                INTEGER,
        covind                NUMERIC,
        index_2               INTEGER,
        modindx               INTEGER,
        murder                INTEGER,
        rape                  INTEGER,
        robbery               INTEGER,
        agasslt               INTEGER,
        burglry               INTEGER,
        larceny               INTEGER,
        mvtheft               INTEGER,
        arson                 INTEGER,
        population            INTEGER,
        fips_st               INTEGER,
        fips_cty              INTEGER
    );
""")


sales_fact_table_create = ("""
    CREATE TABLE IF NOT EXISTS sales_fact (
        sales_id      INTEGER     IDENTITY(0,1) SORTKEY,
        date          DATE        REFERENCES time_dim (date),
        store_id      INTEGER     REFERENCES store_dim (store_id),
        brand_id      INTEGER,
        item_id       INTEGER     REFERENCES item_dim (item_id),
        sold_count    INTEGER,
        volume_sold   INTEGER,
        sales         DECIMAL(8,2),
        county        VARCHAR(25) REFERENCES county_census_dim (county),
        city          VARCHAR(25)
    );
""")

store_dim_table_create = ("""
    CREATE TABLE IF NOT EXISTS store_dim (
        store_id    INTEGER PRIMARY KEY,
        store_name  VARCHAR(100),
        address     VARCHAR(200),
        city        VARCHAR(25),
        zip_code    VARCHAR(5),
        county      VARCHAR(25)
    );

""")

item_dim_table_create = ("""
    CREATE TABLE IF NOT EXISTS item_dim (
    item_id             INTEGER PRIMARY KEY,
    brand_id            INTEGER,
    item_name           VARCHAR(200),
    brand_name          VARCHAR(100),
    bottle_volume       INTEGER,
    state_bottle_cost   DECIMAL(8,2),
    state_bottle_retail DECIMAL(8,2)
    );
""")

time_dim_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_dim (
        date    DATE PRIMARY KEY,
        year    NUMERIC,
        month   NUMERIC,
        day     NUMERIC,
        weekday NUMERIC
    );

""")

temperature_dim_table_create = ("""
    CREATE TABLE IF NOT EXISTS temperature_dim (
        date         DATE,
        city         VARCHAR(25),
        temperature  DECIMAL(4,1),
        PRIMARY KEY (date, city)
    );
""")

county_census_dim_table_create = ("""
    CREATE TABLE IF NOT EXISTS county_census_dim (
        county                VARCHAR(25) PRIMARY KEY,
        total_pop             INTEGER,
        men                   INTEGER,
        women                 INTEGER,
        hispanic              DECIMAL(4,1),
        white                 DECIMAL(4,1),
        black                 DECIMAL(4,1),
        native                DECIMAL(4,1),
        asian                 DECIMAL(4,1),
        pacific               DECIMAL(4,1),
        voting_age_citizen    INTEGER,
        income                INTEGER,
        income_per_cap        INTEGER,
        poverty               DECIMAL(4,1),
        child_poverty         DECIMAL(4,1),
        unemployment          DECIMAL(4,1),
        crime_rate_per_100000 DECIMAL(7,1)
    );
""")



# Copy S3 data to STAGING TABLES
load_data_from_S3 = ("""
    copy {{}}
    from {{}}
    region 'us-west-2'
    iam_role {iam}
    compupdate off statupdate off
    format as csv
    ignoreheader 1 
    DATEFORMAT AS 'MM/DD/YYYY'
""").format(iam = DWH_IAM_ROLE_ARN)


copy_from_s3_to_staging_census_table = ("""
    copy staging_census
    from {}
    region 'us-west-2'
    iam_role '{}'
    compupdate off statupdate off
    format as csv
    ignoreheader 1 
""").format(S3_CENSUS_DATA, DWH_IAM_ROLE_ARN)

copy_from_s3_to_staging_crime_table = ("""
    copy staging_crime
    from {}
    region 'us-west-2'
    iam_role '{}'
    compupdate off statupdate off
    format as csv
    ignoreheader 1 
""").format(S3_CRIME_DATA , DWH_IAM_ROLE_ARN)

copy_from_s3_to_staging_temperature_table = ("""
    copy staging_temperature
    from {}
    region 'us-west-2'
    iam_role '{}'
    json 'auto'
""").format(S3_TEMPERATURE_DATA , DWH_IAM_ROLE_ARN)

copy_from_s3_to_staging_liquor_sales_table = ("""
    copy staging_liquor_sales
    from {}
    region 'us-west-2'
    iam_role '{}'
    compupdate off statupdate off
    format as csv
    ignoreheader 1 
    DATEFORMAT AS 'MM/DD/YYYY'
""").format(S3_LIQUOR_SALES_DATA , DWH_IAM_ROLE_ARN)


# POPULATING FINAL TABLES W/ INSERT SELECT

sales_fact_table_insert = ("""
    INSERT INTO sales_fact (date, store_id, brand_id, item_id, sold_count, volume_sold, sales, county, city)
    SELECT sls.date,
           sls.store_num                                            AS store_id,
           sls.vendor_num                                           AS brand_id,
           sls.item_num                                             AS item_id,
           sls.bottle_sold                                          AS sold_count,
           sls.volume_sold_ltr                                      AS volume_sold,
           TO_NUMBER(RIGHT(sls.sale, LEN(sls.sale)-1), '99999D99')  AS sales,
           sls.county_name                                          AS county,
           sls.city                        
    FROM   staging_liquor_sales sls    
""")

store_dim_table_insert = ("""
    INSERT INTO store_dim (store_id, store_name, address, city, zip_code, county)
    SELECT DISTINCT sls.store_num   AS store_id,
                    sls.store_name,
                    sls.address,
                    sls.city,
                    sls.zip         AS zip_code,
                    sls.county_name AS county
    FROM            staging_liquor_sales sls
""")


item_dim_table_insert = ("""
    INSERT INTO item_dim (item_id, brand_id, item_name, brand_name, bottle_volume, state_bottle_cost, state_bottle_retail)
    SELECT DISTINCT sls.item_num    AS item_id,
                    sls.vendor_num  AS brand_id,
                    sls.item_name,
                    sls.vendor_name AS brand_name,
                    sls.bottle_volume,
                    TO_NUMBER(RIGHT(sls.state_bottle_cost, LEN(sls.sale)-1), '99999D99'),
                    TO_NUMBER(RIGHT(sls.state_bottle_retail, LEN(sls.sale)-1), '99999D99')
    FROM            staging_liquor_sales sls
""")

time_dim_table_insert = ("""
    INSERT INTO time_dim (date, year, month, day, weekday)
    SELECT DISTINCT sls.date,
                    EXTRACT(yr FROM sls.date)  AS year,
                    EXTRACT(mon FROM sls.date) AS month,
                    EXTRACT(d FROM sls.date)   AS day,
                    EXTRACT(dw FROM sls.date)   AS weekday
    FROM            staging_liquor_sales sls
""")

temperature_dim_table_insert = ("""
    INSERT INTO temperature_dim (date, city, temperature)
    SELECT DISTINCT TO_DATE(CONCAT(year::TEXT, CONCAT(RIGHT((100+month)::VARCHAR, 2), RIGHT((100+day)::VARCHAR, 2))), 'YYYYMMDD') AS date,
                    city,
                    temperature
    FROM staging_temperature
    WHERE country = 'US' AND state = 'Iowa'
""")

county_census_dim_table_insert = ("""
    INSERT INTO county_census_dim (county, total_pop, men, women, hispanic, white, black, native, asian, pacific, voting_age_citizen, income, income_per_cap, poverty, child_poverty, unemployment, crime_rate_per_100000)
    SELECT DISTINCT LEFT(cen.county, LEN(cen.county) - 7) AS county,
                    cen.total_pop,
                    cen.men,
                    cen.women,
                    cen.hispanic, 
                    cen.white, 
                    cen.black, 
                    cen.native, 
                    cen.asian, 
                    cen.pacific, 
                    cen.voting_age_citizen, 
                    cen.income, 
                    cen.income_per_cap, 
                    cen.poverty, 
                    cen.child_poverty, 
                    cen.unemployment, 
                    cri.crime_rate_per_100000
    FROM            staging_census cen
    INNER JOIN staging_crime cri ON LEFT(cen.county, LEN(cen.county) - 7) = LEFT(cri.county_name, LEN(cri.county_name) - 11)
    WHERE cen.state = 'Iowa' AND RIGHT(cri.county_name, 2) = 'IA'
    
""")



# QUERY LISTS
drop_then_create_tables_queries = [
    staging_liquor_sales_table_drop,
    staging_temperature_table_drop,
    staging_census_table_drop,
    staging_crime_table_drop,
    sales_fact_table_drop,
    store_dim_table_drop,
    item_dim_table_drop,
    time_dim_table_drop,
    temperature_dim_table_drop,
    county_census_dim_table_drop,
    staging_liquor_sales_table_create,
    staging_temperature_table_create,
    staging_census_table_create,
    staging_crime_table_create,
    store_dim_table_create,
    item_dim_table_create,
    time_dim_table_create,
    temperature_dim_table_create,
    county_census_dim_table_create,
    sales_fact_table_create
]

check_tables_queries = [
    'SELECT * FROM staging_liquor_sales     LIMIT 5',
    'SELECT COUNT(*) FROM staging_liquor_sales',
    
    'SELECT * FROM staging_temperature     LIMIT 5',
    'SELECT COUNT(*) FROM staging_temperature',
    
    'SELECT * FROM staging_census     LIMIT 5',
    'SELECT COUNT(*) FROM staging_census',
    
    'SELECT * FROM staging_crime     LIMIT 5',
    'SELECT COUNT(*) FROM staging_crime',    
    
    'SELECT * FROM sales_fact        LIMIT 5',
    'SELECT COUNT(*) FROM sales_fact',
    
    'SELECT * FROM store_dim         LIMIT 5',
    'SELECT COUNT(*) FROM store_dim',
    
    'SELECT * FROM item_dim          LIMIT 5',
    'SELECT COUNT(*) FROM item_dim',
    
    'SELECT * FROM time_dim          LIMIT 5',
    'SELECT COUNT(*) FROM time_dim',
    
    'SELECT * FROM temperature_dim   LIMIT 5',
    'SELECT COUNT(*) FROM temperature_dim',
    
    'SELECT * FROM county_census_dim LIMIT 5',
    'SELECT COUNT(*) FROM county_census_dim'
]

