import pandas as pd
import os
import mysql.connector as mysconnect

os.chdir("/Users/jake/Downloads/ab/airbnb-datasets") # Mac
os.chdir("C:/Users/jleaf/OneDrive - Albany Beck/airbnb/datasets") # Windows

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
    50: 'manchester.csv', 51: 'melbourne.csv', 52: 'menorca.csv', 53: 'mexico_city.csv', 
    54: 'mid_north_coast.csv', 55: 'milan.csv', 56: 'montreal.csv', 57: 'mornington_peninsula.csv', 
    58: 'munich.csv', 59: 'naples.csv', 60: 'nashville.csv', 61: 'new_brunswick.csv', 
    62: 'new_orleans.csv', 63: 'new_york_city.csv', 64: 'new_zealand.csv', 65: 'newark.csv', 
    66: 'northern_rivers.csv', 67: 'oakland.csv', 68: 'oslo.csv', 69: 'ottawa.csv', 
    70: 'pacific_grove.csv', 71: 'paris.csv', 72: 'pays_basque.csv', 73: 'portland.csv', 
    74: 'porto.csv', 75: 'prague.csv', 76: 'puglia.csv', 77: 'quebec_city.csv', 78: 'rhode_island.csv', 
    79: 'riga.csv', 80: 'rio_de_janeiro.csv', 81: 'rochester.csv', 82: 'rome.csv', 83: 'rotterdam.csv', 
    84: 'salem.csv', 85: 'san_diego.csv', 86: 'san_francisco.csv', 87: 'san_mateo_county.csv', 
    88: 'santa_clara_country.csv', 89: 'santa_cruz_country.csv', 90: 'santiago.csv', 91: 'seattle.csv', 
    92: 'sevilla.csv', 93: 'sicily.csv', 94: 'singapore.csv', 95: 'south_aegean.csv', 
    96: 'stockholm.csv', 97: 'sunshine_coast.csv', 98: 'syndey.csv', 99: 'taipei.csv', 
    100: 'tasmania.csv', 101: 'the_hague.csv', 102: 'thessaloniki.csv', 103: 'tokyo.csv', 
    104: 'toronto.csv', 105: 'trentino.csv', 106: 'twin_cities.csv', 107: 'valencia.csv', 
    108: 'vancouver.csv', 109: 'vaud.csv', 110: 'venice.csv', 111: 'victoria.csv', 
    112: 'vienna.csv', 113: 'washington_dc.csv', 114: 'western_australia.csv', 115: 'winnipeg.csv', 
    116: 'zurich.csv'
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
    50: ['Manchester', 'England', 'United Kingdom', 'Europe'],
    51: ['Melbourne', 'Victoria', 'Australia', 'Oceania'],
    52: ['Menorca', 'Islas Baleares', 'Spain', 'Europe'],
    53: ['Mexico City', 'Distrito Federal', 'Mexico', 'North America'],
    54: ['Mid North Coast', 'New South Wales', 'Australia', 'Oceania'],
    55: ['Milan', 'Lombardy', 'Italy', 'Europe'],
    56: ['Montreal', 'Quebec', 'Canada', 'North America'],
    57: ['Mornington Peninsula', 'Victoria', 'Austrlia', 'Oceania'],
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
    69: ['Ottawa', 'Ontario', 'Canada', 'United States'],
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
}

locations = pd.DataFrame.from_dict(locations, orient='index', columns = ['city', 'region', 'country', 'continent'])
locations = locations.reset_index()
locations['location_id'] = locations['index']
locations.drop('index', axis=1)

all_columns = [
    "location_id",
    "id",
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
    "availability_365",
    "number_of_reviews",
    "number_of_reviews_ltm",
    "number_of_reviews_l30d",
    "first_review",
    "last_review",
    "review_scores_rating",
    "review_scores_accuracy",
    "review_scores_cleanliness",
    "review_scores_checkin",
    "review_scores_communication",
    "review_scores_location",
    "review_scores_value",
    "calculated_host_listings_count",
    "calculated_host_listings_count_entire_homes",
    "calculated_host_listings_count_private_rooms",
    "calculated_host_listings_count_shared_rooms"
]

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
    "latitude": "float64",
    "longitude": "float64",
    "property_type": "object",
    "room_type": "object",
    "accommodates": "Int64",
    "bathrooms": "float64",
    "bedrooms": "float64",
    "beds": "float64",
    "price": "float64",
    "minimum_nights": "Int64",
    "maximum_nights": "Int64",
    "availability_30": "Int64",
    "availability_60": "Int64",
    "availability_90": "Int64",
    "availability_365": "Int64"
}
main = pd.DataFrame(columns = main_columns)
main.astype(main_dtypes).dtypes

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
    "host_response_rate": "object",
    "host_acceptance_rate": "object",
    "host_is_superhost": "object",
    "host_neighbourhood": "object",
    "host_listings_count": "int",
    "host_total_listings_count": "int",
    "host_verifications": "object",
    "host_has_profile_pic": "object",
    "host_identity_verified": "object",
    "calculated_host_listings_count": "Int64",
    "calculated_host_listings_count_entire_homes": "Int64",
    "calculated_host_listings_count_private_rooms": "Int64",
    "calculated_host_listings_count_shared_rooms": "Int64"
}
hosts = pd.DataFrame(columns = hosts_columns)
hosts.astype(hosts_dtypes).dtypes

reviews_columns = [
    "id",
    "location_id",
    "number_of_reviews",
    "number_of_reviews_ltm",
    "number_of_reviews_l30d",
    "first_review",
    "last_review",
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
    "first_review": "datetime64[ns]",
    "last_review": "datetime64[ns]",
    "review_scores_rating": "float64",
    "review_scores_accuracy": "float64",
    "review_scores_cleanliness": "float64",
    "review_scores_checkin": "float64",
    "review_scores_communication": "float64",
    "review_scores_location": "float64",
    "review_scores_value": "float64"
}
reviews = pd.DataFrame(columns = reviews_columns)
reviews.astype(reviews_dtypes).dtypes

for i in range(len(csvs)):
    try:
        df = pd.DataFrame(pd.read_csv(csvs[i]))
        df["location_id"] = i
        df['price'] = df['price'].str[1:]
        df['price'] = df['price'].to_numeric()
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

main_columns = ["id", "host_id", "location_id", "neighbourhood", "neighbourhood_cleansed", "neighbourhood_group_cleansed", "latitude", "longitude", "property_type", "room_type", "accommodates", "bathrooms", "bedrooms", "beds", "price", "minimum_nights", "maximum_nights", "availability_30", "availability_60", "availability_90", "availability_365"]
main_dtypes = {"id": "Int64", "host_id": "Int64", "location_id": "Int64", "neighbourhood": "object", "neighbourhood_cleansed": "object", "neighbourhood_group_cleansed": "object", "latitude": "float64", "longitude": "float64", "property_type": "object", "room_type": "object", "accommodates": "Int64", "bathrooms": "float64", "bedrooms": "Int64", "beds": "Int64", "price": "float64", "minimum_nights": "Int64", "maximum_nights": "Int64", "availability_30": "Int64", "availability_60": "Int64", "availability_90": "Int64", "availability_365": "Int64"}
main = pd.DataFrame(columns = main_columns)
