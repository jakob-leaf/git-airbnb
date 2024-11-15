import pandas as pd
import os
import mysql.connector as mysconnect

os.chdir("/Users/jake/Downloads/ab/airbnb-datasets")

csvs = {
    0: 'albany.csv', 1: 'amsterdam.csv', 2: 'antwerp.csv', 3: 'asheville.csv', 4: 'athens.csv', 
    5: 'austin.csv', 6: 'bangkok.csv', 7: 'barcelona.csv', 8: 'barossa_valley.csv', 
    9: 'barwon_south_west.csv', 10: 'belize.csv', 11: 'bergamo.csv', 12: 'berlin.csv', 
    13: 'bologna.csv', 14: 'bordeaux.csv', 15: 'boston.csv', 16: 'bozeman.csv', 17: 'brisbane.csv', 
    18: 'bristol.csv', 19: 'broward_county.csv', 20: 'brussels.csv', 21: 'buenos_aires.csv', 
    22: 'cambridge.csv', 23: 'cape_town.csv', 24: 'chicago.csv', 25: 'clark_county.csv', 
    26: 'columbus.csv', 27: 'copenhagen.csv', 28: 'crete.csv', 29: 'dallas.csv', 30: 'denver.csv', 
    31: 'dublin.csv', 32: 'edinburgh.csv', 33: 'euskadi.csv', 34: 'florence.csv', 35: 'fort_worth.csv', 
    36: 'geneva.csv', 37: 'ghent.csv', 38: 'girona.csv', 39: 'hawaii.csv', 40: 'hong_kong.csv', 
    41: 'istanbul.csv', 42: 'jersey_city.csv', 43: 'lisbon.csv', 44: 'london.csv', 
    45: 'los_angeles.csv', 46: 'lyon.csv', 47: 'madrid.csv', 48: 'malaga.csv', 49: 'mallorca.csv', 
    50: 'malta.csv', 51: 'manchester.csv', 52: 'melbourne.csv', 53: 'menorca.csv', 54: 'mexico_city.csv', 
    55: 'mid_north_coast.csv', 56: 'milan.csv', 57: 'montreal.csv', 58: 'mornington_peninsula.csv', 
    59: 'munich.csv', 60: 'naples.csv', 61: 'nashville.csv', 62: 'new_brunswick.csv', 
    63: 'new_orleans.csv', 64: 'new_york_city.csv', 65: 'new_zealand.csv', 66: 'newark.csv', 
    67: 'northern_rivers.csv', 68: 'oakland.csv', 69: 'oslo.csv', 70: 'ottawa.csv', 
    71: 'pacific_grove.csv', 72: 'paris.csv', 73: 'pays_basque.csv', 74: 'portland.csv', 
    75: 'porto.csv', 76: 'prague.csv', 77: 'puglia.csv', 78: 'quebec_city.csv', 79: 'rhode_island.csv', 
    80: 'riga.csv', 81: 'rio_de_janeiro.csv', 82: 'rochester.csv', 83: 'rome.csv', 84: 'rotterdam.csv', 
    85: 'salem.csv', 86: 'san_diego.csv', 87: 'san_francisco.csv', 88: 'san_mateo_county.csv', 
    89: 'santa_clara_country.csv', 90: 'santa_cruz_country.csv', 91: 'santiago.csv', 92: 'seattle.csv', 
    93: 'sevilla.csv', 94: 'sicily.csv', 95: 'singapore.csv', 96: 'south_aegean.csv', 
    97: 'stockholm.csv', 98: 'sunshine_coast.csv', 99: 'syndey.csv', 100: 'taipei.csv', 
    101: 'tasmania.csv', 102: 'the_hague.csv', 103: 'thessaloniki.csv', 104: 'tokyo.csv', 
    105: 'toronto.csv', 106: 'trentino.csv', 107: 'twin_cities.csv', 108: 'valencia.csv', 
    109: 'vancouver.csv', 110: 'vaud.csv', 111: 'venice.csv', 112: 'victoria.csv', 
    113: 'vienna.csv', 114: 'washington_dc.csv', 115: 'western_australia.csv', 116: 'winnipeg.csv', 
    117: 'zurich.csv'
}

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
    11: ['Bergamo', 'Belize', 'Belize', 'North America'],
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
    50: ['Malta', 'Malta', 'Malta', 'Europe'],
    51: ['Manchester', 'England', 'United Kingdom', 'Europe'],
    52: ['Melbourne', 'Victoria', 'Australia', 'Oceania'],
    53: ['Menorca', 'Islas Baleares', 'Spain', 'Europe'],
    54: ['Mexico City', 'Distrito Federal', 'Mexico', 'North America'],
    55: ['Mid North Coast', 'New South Wales', 'Australia', 'Oceania'],
    56: ['Milan', 'Lombardy', 'Italy', 'Europe'],
    57: ['Montreal', 'Quebec', 'Canada', 'North America'],
    58: ['Mornington Peninsula', 'Victoria', 'Austrlia', 'Oceania'],
    59: ['Munich', 'Bavaria', 'Germany', 'Europe'],
    60: ['Naples', 'Campania', 'Italy', 'Europe'],
    61: ['Nashville', 'Tennessee', 'United States', 'North America'],
    62: ['New Brunswick', 'New Brunswick', 'Canada', 'North America'],
    63: ['New Orleans', 'Louisiana', 'United States', 'North America'],
    64: ['New York City', 'New York', 'United States', 'North America'],
    65: ['New Zealand', 'New Zealand', 'New Zealand', 'Oceania'],
    66: ['Newark', 'New Jersey', 'United States', 'North America'],
    67: ['Northern Rivers', 'New South Wales', 'Australia', 'Oceania'],
    68: ['Oakland', 'California', 'United States', 'North America'],
    69: ['Oslo', 'Oslo', 'Norway', 'Europe'],
    70: ['Ottawa', 'Ontario', 'Canada', 'United States'],
    71: ['Pacific Grove', 'California', 'United States', 'North America'],
    72: ['Paris', 'Ile-de-France', 'France', 'Europe'],
    73: ['Pays Basque', 'Pyrenees-Atlantiques', 'France', 'Europe'],
    74: ['Portland', 'Oregon', 'United States', 'North America'],
    75: ['Porto', 'Norte', 'Portugal', 'Europe'],
    76: ['Prague', 'Prague', 'Czech Republic', 'Europe'],
    77: ['Puglia', 'Puglia', 'Italy', 'Europe'],
    78: ['Quebec City', 'Quebec', 'Canada', 'North America'],
    79: ['Rhode Island', 'Rhode Island', 'United States', 'North America'],
    80: ['Riga', 'Riga', 'Latvia', 'Europe'],
    81: ['Rio de Janeiro', 'Rio de Janeiro', 'Brazil', 'South America'],
    82: ['Rochester', 'New York', 'United States', 'North America'],
    83: ['Rome', 'Lazio', 'Italy', 'Europe'],
    84: ['Rotterdam', 'South Holland', 'The Netherlands', 'Europe'],
    85: ['Salem', 'Oregon', 'United States', 'North America'],
    86: ['San Diego', 'California', 'United States', 'North America'],
    87: ['San Francisco', 'California', 'United States', 'North America'],
    88: ['San Mateo County', 'California', 'United States', 'North America'],
    89: ['Santa Clara Country', 'California', 'United States', 'North America'],
    90: ['Santa Cruz Country', 'California', 'United States', 'North America'],
    91: ['Santiago', 'Region Metropolitana de Santiago', 'Chile', 'Europe'],
    92: ['Seattle', 'Washington', 'United States', 'North America'],
    93: ['Sevilla', 'Andalucia', 'Spain', 'Europe'],
    94: ['Sicily', 'Sicilia', 'Italy', 'Europe'],
    95: ['Singapore', 'Singapore', 'Singapore', 'Asia'],
    96: ['South Aegean', 'South Aegean', 'Greece', 'Europe'],
    97: ['Stockholm', 'Stockholm Ian', 'Sweden', 'Europe'],
    98: ['Sunshine Coast', 'Queensland', 'Australia', 'Oceania'],
    99: ['Syndey', 'New South Wales', 'Australia', 'Oceania'],
    100: ['Taipei', 'Northern Taiwan', 'Taiwan', 'Asia'],
    101: ['Tasmania', 'Tasmania', 'Australia', 'Oceania'],
    102: ['The Hague', 'South Holland', 'The Netherlands', 'Europe'],
    103: ['Thessaloniki', 'Central Macedonia', 'Greece', 'Europe'],
    104: ['Tokyo', 'Kanto', 'Japan', 'Asia'],
    105: ['Toronto', 'Ontario', 'Canada', 'North America'],
    106: ['Trentino', 'Trentino-Alto Adige/Sudtirol', 'Italy', 'Europe'],
    107: ['Twin Cities', 'Minnesota', 'United States', 'North America'],
    108: ['Valencia', 'Valencia', 'Spain', 'Europe'],
    109: ['Vancouver', 'British Columbia', 'Canada', 'North America'],
    110: ['Vaud', 'Vaud', 'Switzerland', 'Europe'],
    111: ['Venice', 'Veneto', 'Italy', 'Europe'],
    112: ['Victoria', 'British Columbia', 'Canada', 'North America'],
    113: ['Vienna', 'Vienna', 'Austria', 'Europe'],
    114: ['Washington DC', 'District of Columbia', 'United States', 'North America'],
    115: ['Western Australia', 'Western Australia', 'Australia', 'Oceania'],
    116: ['Winnipeg', 'Manitoba', 'Canada', 'North America'],
    117: ['Zurich', 'Zurich', 'Switzerland', 'Europe']
}
locations = pd.DataFrame.from_dict(locations, orient='index', columns = ['city', 'region', 'country', 'continent'])
locations = locations.reset_index()
locations['location_id'] = locations['index']
locations.drop('index', axis=1)

main_columns = ["id", "host_id", "location_id", "neighbourhood", "neighbourhood_cleansed", "neighbourhood_group_cleansed", "latitude", "longitude", "property_type", "room_type", "accommodates", "bathrooms", "bedrooms", "beds", "price", "minimum_nights", "maximum_nights", "availability_30", "availability_60", "availability_90", "availability_365"]
main_dtypes = {"id": "int64", "host_id": "int64", "location_id": "int64", "neighbourhood": "object", "neighbourhood_cleansed": "object", "neighbourhood_group_cleansed": "object", "latitude": "float64", "longitude": "float64", "property_type": "object", "room_type": "object", "accommodates": "int64", "bathrooms": "float64", "bedrooms": "int64", "beds": "int64", "price": "float64", "minimum_nights": "int64", "maximum_nights": "int64", "availability_30": "int64", "availability_60": "int64", "availability_90": "int64", "availability_365": "int64"}
main = pd.DataFrame(columns = main_columns)
main.astype(main_dtypes).dtypes

hosts_columns = ["host_id", "host_name", "host_since", "host_location", "host_response_time", "host_response_rate", "host_acceptance_rate", "host_is_superhost", "host_neighbourhood", "host_listings_count", "host_total_listings_count", "host_verifications", "host_has_profile_pic", "host_identity_verified", "calculated_host_listings_count", "calculated_host_listings_count_entire_homes", "calculated_host_listings_count_private_rooms", "calculated_host_listings_count_shared_rooms"]
hosts_dtypes = {"host_id": "int64", "host_name":"object", "host_since": "datetime64[ns]", "host_location": "object", "host_response_time": "object", "host_response_rate": "object", "host_acceptance_rate": "object", "host_is_superhost": "object", "host_neighbourhood": "object", "host_listings_count": "int", "host_total_listings_count": "int", "host_verifications": "object", "host_has_profile_pic": "object", "host_identity_verified": "object", "calculated_host_listings_count": "int64", "calculated_host_listings_count_entire_homes": "int64", "calculated_host_listings_count_private_rooms": "int64", "calculated_host_listings_count_shared_rooms": "int64"}
hosts = pd.DataFrame(columns = hosts_columns)
hosts.astype(hosts_dtypes).dtypes

reviews_columns = ["id", "location_id", "number_of_reviews", "number_of_reviews_ltm", "number_of_reviews_l30d", "first_review", "last_review", "review_scores_rating", "review_scores_accuracy", "review_scores_cleanliness", "review_scores_checkin", "review_scores_communication", "review_scores_location", "review_scores_value"]
reviews_dtypes = {"id": "int64", "location_id": "int64", "number_of_reviews": "int64", "number_of_reviews_ltm": "int64", "number_of_reviews_l30d": "int64", "first_review": "datetime64[ns]", "last_review": "datetime64[ns]", "review_scores_rating": "float64", "review_scores_accuracy": "float64", "review_scores_cleanliness": "float64", "review_scores_checkin": "float64", "review_scores_communication": "float64", "review_scores_location": "float64", "review_scores_value": "float64"}
reviews = pd.DataFrame(columns = reviews_columns)
reviews.astype(reviews_dtypes).dtypes

for i in range(len(csvs)):
    try:
        df = pd.DataFrame(pd.read_csv(csvs[i]))
        df["location_id"] = i
        i_main = df[main_columns]
        main = pd.concat([main, i_main], ignore_index = True)
        i_hosts = df[hosts_columns]
        hosts = pd.concat([hosts, i_hosts], ignore_index = True)        
        i_reviews = df[reviews_columns]
        reviews = pd.concat([reviews, i_reviews], ignore_index = True)
    except Exception as e:
        print(f"issue loading location_id {i}")
        continue

# db = mysconnect.connect(host = 'localhost', user = 'root', password = '')
# cursor = db.cursor()
# cursor.execute('DROP DATABASE IF EXISTS airbnb;')
# cursor.execute('CREATE DATABASE airbnb;')

# db = mysconnect.connect(host = 'localhost', user = 'root', password = '', database = 'airbnb')
# cursor = db.cursor()
# cursor.execute('DROP TABLE IF EXISTS listings;')
# cursor.execute('''
#     CREATE TABLE listings (
#         `id` int())

