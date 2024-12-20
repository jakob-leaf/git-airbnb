import pandas as pd
import os
import mysql.connector as mysconnect
import numpy as np

os.chdir("/Users/jake/Library/CloudStorage/OneDrive-AlbanyBeck/airbnb/datasets") # Mac
# os.chdir("C:/Users/jleaf/OneDrive - Albany Beck/airbnb/datasets") # Windows

locations = {
    0: ['Albany', 'New York', 'United States', 'North America'],
    1: ['Amsterdam', 'North Holland', 'The Netherlands', 'Europe'],
    2: ['Antwerp', 'Flemish Region', 'Belgium', 'Europe'],
    3: ['Asheville', 'North Carolina', 'United States', 'North America'],
    4: ['Athens', 'Attica', 'Greece', 'Europe'],
    5: ['Austin', 'Texas', 'United States', 'North America'],
    6: ['Bangkok', 'Central Thailand', 'Thailand', 'Asia'],
    7: ['Barcelona', 'Catalonia', 'Spain', 'Europe'],
    8: ['Barossa Valley', 'South Australia', 'Australia', 'Oceania'],
    9: ['Barwon South West', 'Victoria', 'Australia', 'Oceania'],
    10: ['Belize', 'Belize', 'Belize', 'North America'],
    11: ['Bergamo', 'Lombardia', 'Italy', 'Europe'],
    12: ['Berlin', 'Berlin', 'Germany', 'Europe'],
    13: ['Bologna', 'Emiglia-Romagna', 'Italy', 'Europe'],
    14: ['Bordeaux', 'Bouvelle-Aquitaine', 'France', 'Europe'],
    15: ['Boston', 'Massachusetts', 'United States', 'North America'],
    16: ['Bozeman', 'Montana', 'United States', 'North America'],
    17: ['Brisbane', 'Queensland', 'Australia', 'Oceania'],
    18: ['Bristol', 'England', 'United Kingdom', 'Europe'],
    19: ['Broward County', 'Florida', 'United States', 'North America'],
    20: ['Brussels', 'Brussels', 'Belgium', 'Europe'],
    21: ['Buenos Aires', 'Ciudad Autonoma de Buenos Aires', 'Argentina', 'South America'],
    22: ['Cambridge', 'Massachusetts', 'United States', 'North America'],
    23: ['Cape Town', 'Western Cape', 'South Africa', 'Africa'],
    24: ['Chicago', 'Illinois', 'United States', 'North America'],
    25: ['Clark County', 'Nevada', 'United States', 'North America'],
    26: ['Columbus', 'Ohio', 'United States', 'North America'],
    27: ['Copenhagen', 'Hovedstaden', 'Denmark', 'Europe'],
    28: ['Crete', 'Crete', 'Greece', 'Europe'],
    29: ['Dallas', 'Texas', 'United States', 'North America'],
    30: ['Denver', 'Colorado', 'United States', 'North America'],
    31: ['Dublin', 'Leinster', 'Ireland', 'Europe'],
    32: ['Edinburgh', 'Scotland', 'United Kingdom', 'Europe'],
    33: ['Euskadi', 'Euskadi', 'Spain', 'Europe'],
    34: ['Florence', 'Toscana', 'Italy', 'Europe'],
    35: ['Fort Worth', 'Texas', 'United States', 'North America'],
    36: ['Geneva', 'Geneva', 'Switzerland', 'Europe'],
    37: ['Ghent', 'Flemish Region', 'Belgium', 'Europe'],
    38: ['Girona', 'Catalonia', 'Spain', 'Europe'],
    39: ['Hawaii', 'Hawaii', 'United States', 'North America'],
    40: ['Hong Kong', 'Hong Kong', 'China', 'Asia'],
    41: ['Istanbul', 'Marmara', 'Turkey', 'Asia'],
    42: ['Jersey City', 'New Jersey', 'United States', 'North America'],
    43: ['Lisbon', 'Lisbon', 'Portugal', 'Europe'],
    44: ['London', 'England', 'United Kingdom', 'Europe'],
    45: ['Los Angeles', 'California', 'United States', 'North America'],
    46: ['Lyon', 'Auvergne-Rhone-Alpes', 'France', 'Europe'],
    47: ['Madrid', 'Comunidad de Madrid', 'Spain', 'Europe'],
    48: ['Malaga', 'Andalucia', 'Spain', 'Europe'],
    49: ['Mallorca', 'Islas Baleares', 'Spain', 'Europe'],
    50: ['Manchester', 'England', 'United Kingdom', 'Europe'],
    51: ['Melbourne', 'Victoria', 'Australia', 'Oceania'],
    52: ['Menorca', 'Islas Baleares', 'Spain', 'Europe'],
    53: ['Mexico City', 'Distrito Federal', 'Mexico', 'North America'],
    54: ['Mid North Coast', 'New South Wales', 'Australia', 'Oceania'],
    55: ['Milan', 'Lombardy', 'Italy', 'Europe'],
    56: ['Montreal', 'Quebec', 'Canada', 'North America'],
    57: ['Mornington Peninsula', 'Victoria', 'Australia', 'Oceania'],
    58: ['Munich', 'Bavaria', 'Germany', 'Europe'],
    59: ['Naples', 'Campania', 'Italy', 'Europe'],
    60: ['Nashville', 'Tennessee', 'United States', 'North America'],
    61: ['New Brunswick', 'New Brunswick', 'Canada', 'North America'],
    62: ['New Orleans', 'Louisiana', 'United States', 'North America'],
    63: ['New York City', 'New York', 'United States', 'North America'],
    64: ['New Zealand', 'New Zealand', 'New Zealand', 'Oceania'],
    65: ['Newark', 'New Jersey', 'United States', 'North America'],
    66: ['Northern Rivers', 'New South Wales', 'Australia', 'Oceania'],
    67: ['Oakland', 'California', 'United States', 'North America'],
    68: ['Oslo', 'Oslo', 'Norway', 'Europe'],
    69: ['Ottawa', 'Ontario', 'Canada', 'North America'],
    70: ['Pacific Grove', 'California', 'United States', 'North America'],
    71: ['Paris', 'Ile-de-France', 'France', 'Europe'],
    72: ['Pays Basque', 'Pyrenees-Atlantiques', 'France', 'Europe'],
    73: ['Portland', 'Oregon', 'United States', 'North America'],
    74: ['Porto', 'Norte', 'Portugal', 'Europe'],
    75: ['Prague', 'Prague', 'Czech Republic', 'Europe'],
    76: ['Puglia', 'Puglia', 'Italy', 'Europe'],
    77: ['Quebec City', 'Quebec', 'Canada', 'North America'],
    78: ['Rhode Island', 'Rhode Island', 'United States', 'North America'],
    79: ['Riga', 'Riga', 'Latvia', 'Europe'],
    80: ['Rio de Janeiro', 'Rio de Janeiro', 'Brazil', 'South America'],
    81: ['Rochester', 'New York', 'United States', 'North America'],
    82: ['Rome', 'Lazio', 'Italy', 'Europe'],
    83: ['Rotterdam', 'South Holland', 'The Netherlands', 'Europe'],
    84: ['Salem', 'Oregon', 'United States', 'North America'],
    85: ['San Diego', 'California', 'United States', 'North America'],
    86: ['San Francisco', 'California', 'United States', 'North America'],
    87: ['San Mateo County', 'California', 'United States', 'North America'],
    88: ['Santa Clara Country', 'California', 'United States', 'North America'],
    89: ['Santa Cruz Country', 'California', 'United States', 'North America'],
    90: ['Santiago', 'Region Metropolitana de Santiago', 'Chile', 'Europe'],
    91: ['Seattle', 'Washington', 'United States', 'North America'],
    92: ['Sevilla', 'Andalucia', 'Spain', 'Europe'],
    93: ['Sicily', 'Sicilia', 'Italy', 'Europe'],
    94: ['Singapore', 'Singapore', 'Singapore', 'Asia'],
    95: ['South Aegean', 'South Aegean', 'Greece', 'Europe'],
    96: ['Stockholm', 'Stockholm Ian', 'Sweden', 'Europe'],
    97: ['Sunshine Coast', 'Queensland', 'Australia', 'Oceania'],
    98: ['Syndey', 'New South Wales', 'Australia', 'Oceania'],
    99: ['Taipei', 'Northern Taiwan', 'Taiwan', 'Asia'],
    100: ['Tasmania', 'Tasmania', 'Australia', 'Oceania'],
    101: ['The Hague', 'South Holland', 'The Netherlands', 'Europe'],
    102: ['Thessaloniki', 'Central Macedonia', 'Greece', 'Europe'],
    103: ['Tokyo', 'Kanto', 'Japan', 'Asia'],
    104: ['Toronto', 'Ontario', 'Canada', 'North America'],
    105: ['Trentino', 'Trentino-Alto Adige/Sudtirol', 'Italy', 'Europe'],
    106: ['Twin Cities', 'Minnesota', 'United States', 'North America'],
    107: ['Valencia', 'Valencia', 'Spain', 'Europe'],
    108: ['Vancouver', 'British Columbia', 'Canada', 'North America'],
    109: ['Vaud', 'Vaud', 'Switzerland', 'Europe'],
    110: ['Venice', 'Veneto', 'Italy', 'Europe'],
    111: ['Victoria', 'British Columbia', 'Canada', 'North America'],
    112: ['Vienna', 'Vienna', 'Austria', 'Europe'],
    113: ['Washington DC', 'District of Columbia', 'United States', 'North America'],
    114: ['Western Australia', 'Western Australia', 'Australia', 'Oceania'],
    115: ['Winnipeg', 'Manitoba', 'Canada', 'North America'],
    116: ['Zurich', 'Zurich', 'Switzerland', 'Europe']
} # Dictionary of locations key k:v pairs as <location id> : [<city>,<region>,<country>,<continent>]

xls = {
    0: 'albany.xlsx', 1: 'amsterdam.xlsx', 2: 'antwerp.xlsx', 3: 'asheville.xlsx', 4: 'athens.xlsx', 
    5: 'austin.xlsx', 6: 'bangkok.xlsx', 7: 'barcelona.xlsx', 8: 'barossa_valley.xlsx', 
    9: 'barwon_south_west.xlsx', 10: 'belize.xlsx', 11: 'bergamo.xlsx', 12: 'berlin.xlsx', 
    13: 'bologna.xlsx', 14: 'bordeaux.xlsx', 15: 'boston.xlsx', 16: 'bozeman.xlsx', 17: 'brisbane.xlsx', 
    18: 'bristol.xlsx', 19: 'broward_county.xlsx', 20: 'brussels.xlsx', 21: 'buenos_aires.xlsx', 
    22: 'cambridge.xlsx', 23: 'cape_town.xlsx', 24: 'chicago.xlsx', 25: 'clark_county.xlsx', 
    26: 'columbus.xlsx', 27: 'copenhagen.xlsx', 28: 'crete.xlsx', 29: 'dallas.xlsx', 30: 'denver.xlsx', 
    31: 'dublin.xlsx', 32: 'edinburgh.xlsx', 33: 'euskadi.xlsx', 34: 'florence.xlsx', 35: 'fort_worth.xlsx', 
    36: 'geneva.xlsx', 37: 'ghent.xlsx', 38: 'girona.xlsx', 39: 'hawaii.xlsx', 40: 'hong_kong.xlsx', 
    41: 'istanbul.xlsx', 42: 'jersey_city.xlsx', 43: 'lisbon.xlsx', 44: 'london.xlsx', 
    45: 'los_angeles.xlsx', 46: 'lyon.xlsx', 47: 'madrid.xlsx', 48: 'malaga.xlsx', 49: 'mallorca.xlsx', 
    50: 'manchester.xlsx', 51: 'melbourne.xlsx', 52: 'menorca.xlsx', 53: 'mexico_city.xlsx', 
    54: 'mid_north_coast.xlsx', 55: 'milan.xlsx', 56: 'montreal.xlsx', 57: 'mornington_peninsula.xlsx', 
    58: 'munich.xlsx', 59: 'naples.xlsx', 60: 'nashville.xlsx', 61: 'new_brunswick.xlsx', 
    62: 'new_orleans.xlsx', 63: 'new_york_city.xlsx', 64: 'new_zealand.xlsx', 65: 'newark.xlsx', 
    66: 'northern_rivers.xlsx', 67: 'oakland.xlsx', 68: 'oslo.xlsx', 69: 'ottawa.xlsx', 
    70: 'pacific_grove.xlsx', 71: 'paris.xlsx', 72: 'pays_basque.xlsx', 73: 'portland.xlsx', 
    74: 'porto.xlsx', 75: 'prague.xlsx', 76: 'puglia.xlsx', 77: 'quebec_city.xlsx', 78: 'rhode_island.xlsx', 
    79: 'riga.xlsx', 80: 'rio_de_janeiro.xlsx', 81: 'rochester.xlsx', 82: 'rome.xlsx', 83: 'rotterdam.xlsx', 
    84: 'salem.xlsx', 85: 'san_diego.xlsx', 86: 'san_francisco.xlsx', 87: 'san_mateo_county.xlsx', 
    88: 'santa_clara_country.xlsx', 89: 'santa_cruz_country.xlsx', 90: 'santiago.xlsx', 91: 'seattle.xlsx', 
    92: 'sevilla.xlsx', 93: 'sicily.xlsx', 94: 'singapore.xlsx', 95: 'south_aegean.xlsx', 
    96: 'stockholm.xlsx', 97: 'sunshine_coast.xlsx', 98: 'syndey.xlsx', 99: 'taipei.xlsx', 
    100: 'tasmania.xlsx', 101: 'the_hague.xlsx', 102: 'thessaloniki.xlsx', 103: 'tokyo.xlsx', 
    104: 'toronto.xlsx', 105: 'trentino.xlsx', 106: 'twin_cities.xlsx', 107: 'valencia.xlsx', 
    108: 'vancouver.xlsx', 109: 'vaud.xlsx', 110: 'venice.xlsx', 111: 'victoria.xlsx', 
    112: 'vienna.xlsx', 113: 'washington_dc.xlsx', 114: 'western_australia.xlsx', 115: 'winnipeg.xlsx', 
    116: 'zurich.xlsx'
} # Dictionary of all excel files for ingestion 

locations = pd.DataFrame.from_dict(locations, orient='index', columns = ['city', 'region', 'country', 'continent']) # Create data frame of locations
locations = locations.reset_index()
locations['location_id'] = locations['index'] # Pull index into dataframe and name it location_id
locations = locations.drop('index', axis=1) # Drop duplicate column

# Create dataframes for listings (main), hosts, and reviews and define datatypes for columns

main_columns = [
    "id",
    "host_id",
    "location_id",
    "neighbourhood",
    "neighbourhood_cleansed",
    "neighbourhood_group_cleansed",
    "latitude",
    "longitude",
    "property_type",
    "room_type",
    "accommodates",
    "bathrooms",
    "bedrooms",
    "beds",
    "price",
    "minimum_nights",
    "maximum_nights",
    "availability_30",
    "availability_60",
    "availability_90",
    "availability_365"
]
main_dtypes = {
    "id": "Int64",
    "host_id": "Int64",
    "location_id": "Int64",
    "neighbourhood": "object",
    "neighbourhood_cleansed": "object",
    "neighbourhood_group_cleansed": "object",
    "latitude": "Float64",
    "longitude": "Float64",
    "property_type": "object",
    "room_type": "object",
    "accommodates": "Int64",
    "bathrooms": "Float64",
    "bedrooms": "Float64",
    "beds": "Float64",
    "price": "Float64",
    "minimum_nights": "Int64",
    "maximum_nights": "Int64",
    "availability_30": "Int64",
    "availability_60": "Int64",
    "availability_90": "Int64",
    "availability_365": "Int64"
}
main = pd.DataFrame(columns = main_columns)

hosts_columns = [
    "host_id",
    "host_since",
    "host_location",
    "host_response_time",
    "host_response_rate",
    "host_acceptance_rate",
    "host_is_superhost",
    "host_neighbourhood",
    "host_listings_count",
    "host_total_listings_count",
    "host_verifications",
    "host_has_profile_pic",
    "host_identity_verified",
    "calculated_host_listings_count",
    "calculated_host_listings_count_entire_homes",
    "calculated_host_listings_count_private_rooms",
    "calculated_host_listings_count_shared_rooms"
]
hosts_dtypes = {
    "host_id": "Int64",
    "host_since": "datetime64[ns]",
    "host_location": "object",
    "host_response_time": "object",
    "host_response_rate": "Float64",
    "host_acceptance_rate": "Float64",
    "host_is_superhost": "bool",
    "host_neighbourhood": "object",
    "host_listings_count": "Int64",
    "host_total_listings_count": "Int64",
    "host_verifications": "object",
    "host_has_profile_pic": "bool",
    "host_identity_verified": "bool",
    "calculated_host_listings_count": "Int64",
    "calculated_host_listings_count_entire_homes": "Int64",
    "calculated_host_listings_count_private_rooms": "Int64",
    "calculated_host_listings_count_shared_rooms": "Int64"
}
hosts = pd.DataFrame(columns = hosts_columns)

reviews_columns = [
    "id",
    "location_id",
    "number_of_reviews",
    "number_of_reviews_ltm",
    "number_of_reviews_l30d",
    "review_scores_rating",
    "review_scores_accuracy",
    "review_scores_cleanliness",
    "review_scores_checkin",
    "review_scores_communication",
    "review_scores_location",
    "review_scores_value"
]
reviews_dtypes = {
    "id": "Int64",
    "location_id": "Int64",
    "number_of_reviews": "Int64",
    "number_of_reviews_ltm": "Int64",
    "number_of_reviews_l30d": "Int64",
    "review_scores_rating": "Float64",
    "review_scores_accuracy": "Float64",
    "review_scores_cleanliness": "Float64",
    "review_scores_checkin": "Float64",
    "review_scores_communication": "Float64",
    "review_scores_location": "Float64",
    "review_scores_value": "Float64"
}
reviews = pd.DataFrame(columns = reviews_columns)

for i in range(len(xls)): # Iterate through excels dictionary for ingestion
    try:
        df = pd.DataFrame(pd.read_excel(xls[i])) # Read xlsx into memory as a dataframe
        df["location_id"] = i # Add location_id column to iterative dataframe
        df['host_since']=df['host_since'].dt.date # Drop time from host_since (only date column in entire database)
        i_main = df[main_columns] # Split iterative dataframe into main, hosts, and listings tables
        main = pd.concat([main, i_main], ignore_index = True) # Concatenate iterative subtables into final dataframes
        i_hosts = df[hosts_columns]
        hosts = pd.concat([hosts, i_hosts], ignore_index = True)        
        i_reviews = df[reviews_columns]
        reviews = pd.concat([reviews, i_reviews], ignore_index = True)
        print(f"location_id {i} loaded successfully") # Update for clarity during long runtime
    except Exception as e:
        print(f"issue loading location_id {i}")
        continue

main['price'] = main['price'].str.replace(r'[^\d.]', '', regex=True).astype(float) # Price column has dollar signs so remove all non-digit or decimal point string values
hosts['host_since'] = pd.to_datetime(hosts['host_since'], format='%y/%m/%d %H:%M:%S') # Format date

bool_cols = ['host_is_superhost', 'host_has_profile_pic', 'host_identity_verified'] # Convert 't'/'f' strings to booleans for three columns
for i in bool_cols:
    hosts[i] = hosts[i].replace('t', True)
    hosts[i] = hosts[i].replace('f', False)

# Convert Numpy nulls to None (for MySQL ingestion) and apply datatypes 

main = main.replace({np.nan: None})
main.astype(main_dtypes).dtypes

hosts = hosts.replace({np.nan: None})
hosts.astype(hosts_dtypes).dtypes

reviews = reviews.replace({np.nan: None})
reviews.astype(reviews_dtypes).dtypes

db = mysconnect.connect(host = 'localhost', user = 'root', password = '') # Connect to local MySQL server
cursor = db.cursor()
cursor.execute('DROP DATABASE IF EXISTS airbnb;') # Create new airbnb datbase
cursor.execute('CREATE DATABASE airbnb;')
print('Database airbnb created')

db = mysconnect.connect(host = 'localhost', user = 'root', password = '', database = 'airbnb') # Use new database in local MySQL server
cursor = db.cursor()

cursor.execute('DROP TABLE IF EXISTS locations;') # Create and populate locations table from locations dataframe
cursor.execute('''
    CREATE TABLE locations (
        city varchar(255),
        region varchar(255),
        country varchar(255),
        continent varchar(255),
        location_id INT PRIMARY KEY
    );
''')

sql = 'INSERT INTO locations (city, region, country, continent, location_id) VALUES (%s, %s, %s, %s, %s)'
for index, row in locations.iterrows():
    cursor.execute(sql, tuple(row))

print('Locations table created')

cursor.execute('DROP TABLE IF EXISTS hosts_all_rows;') # Create and populate hosts_all_rows table from hosts dataframe
cursor.execute('''
    CREATE TABLE hosts_all_rows (
        host_id BIGINT,
        host_since DATETIME,
        host_location VARCHAR(255),
        host_response_time VARCHAR(255),
        host_response_rate VARCHAR(255),
        host_acceptance_rate VARCHAR(255),
        host_is_superhost BOOL,
        host_neighbourhood VARCHAR(255),
        host_listings_count INT,
        host_total_listings_count INT,
        host_verifications VARCHAR(255),
        host_has_profile_pic BOOL,
        host_identity_verified BOOL,
        calculated_host_listings_count INT,
        calculated_host_listings_count_entire_homes INT,
        calculated_host_listings_count_private_rooms INT,
        calculated_host_listings_count_shared_rooms INT
    );
''')

sql = "INSERT INTO hosts_all_rows (host_id, host_since, host_location, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, calculated_host_listings_count, calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, calculated_host_listings_count_shared_rooms) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
for index, row in hosts.iterrows():
    cursor.execute(sql, tuple(row))

cursor.execute('DROP TABLE IF EXISTS hosts;') # Create hosts table by removing duplicate host_ids from hosts_all_rows
cursor.execute('''
    CREATE TABLE hosts AS (
        SELECT *
        FROM hosts_all_rows
        GROUP BY host_id
        )
    ;
    ''')
cursor.execute('ALTER TABLE hosts ADD PRIMARY KEY (host_id)')
cursor.execute('DROP TABLE hosts_all_rows')
print('Hosts table created')

cursor.execute('DROP TABLE IF EXISTS listings_all_rows;') # Create and populate listings_all_rows table from main dataframe
cursor.execute('''
    CREATE TABLE listings_all_rows (
        id BIGINT,
        host_id BIGINT,
        location_id INT,
        neighbourhood VARCHAR(255),
        neighbourhood_cleansed VARCHAR(255),
        neighbourhood_group_cleansed VARCHAR(255),
        latitude DOUBLE,
        longitude DOUBLE,
        property_type VARCHAR(255),
        room_type VARCHAR(255),
        accommodates INT,
        bathrooms DOUBLE,
        bedrooms DOUBLE,
        beds DOUBLE,
        price DOUBLE,
        minimum_nights INT,
        maximum_nights INT,
        availability_30 INT,
        availability_60 INT,
        availability_90 INT,
        availability_365 INT
    );
''')

sql = 'INSERT INTO listings_all_rows (id, host_id, location_id, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, bathrooms, bedrooms, beds, price, minimum_nights, maximum_nights, availability_30, availability_60, availability_90, availability_365) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
for index, row in main.iterrows():
    cursor.execute(sql, tuple(row))

cursor.execute('DROP TABLE IF EXISTS listings;') # Create listings table by removing duplicate ids from listings_all_rows
cursor.execute('''
    CREATE TABLE listings AS (
        SELECT *
        FROM listings_all_rows
        GROUP BY id
        )
    ;
    ''')
cursor.execute('ALTER TABLE listings ADD PRIMARY KEY (id)')
cursor.execute('ALTER TABLE listings ADD FOREIGN KEY (host_id) REFERENCES hosts(host_id)')
cursor.execute('ALTER TABLE listings ADD FOREIGN KEY (location_id) REFERENCES locations(location_id)')
cursor.execute('DROP TABLE listings_all_rows')
print('Listings table created')

cursor.execute('DROP TABLE IF EXISTS reviews_all;') # Create and populate reviews_all table from reviews dataframe
cursor.execute('''
    CREATE TABLE reviews_all (
        id BIGINT,
        location_id INT,
        number_of_reviews BIGINT,
        number_of_reviews_ltm BIGINT,
        number_of_reviews_l30d BIGINT,
        review_scores_rating DOUBLE,
        review_scores_accuracy DOUBLE,
        review_scores_cleanliness DOUBLE,
        review_scores_checkin DOUBLE,
        review_scores_communication DOUBLE,
        review_scores_location DOUBLE,
        review_scores_value DOUBLE
);
''')

sql = "INSERT INTO reviews_all (id, location_id, number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
for index, row in reviews.iterrows():
    cursor.execute(sql, tuple(row))

cursor.execute('DROP TABLE IF EXISTS reviews;') # Create reviews table by removing duplicate ids from reviews_all
cursor.execute('''
    CREATE TABLE reviews AS (
        SELECT *
        FROM reviews_all
        GROUP BY id
        )
    ;
    ''')

cursor.execute('ALTER TABLE reviews ADD FOREIGN KEY (id) REFERENCES listings(id)')
cursor.execute('ALTER TABLE reviews ADD FOREIGN KEY (location_id) REFERENCES locations(location_id)')
cursor.execute('DROP TABLE reviews_all')
print('Reviews table created')

db.commit() # Commit changes
cursor.close() # Close cursor in database
db.close() # Close connection to database
