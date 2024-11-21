import pandas as pd
import os
import mysql.connector as mysconnect
import numpy as np

os.chdir("/Users/jake/Library/CloudStorage/OneDrive-AlbanyBeck/airbnb/datasets") # Mac
# os.chdir("C:/Users/jleaf/OneDrive - Albany Beck/airbnb/datasets") # Windows

locations = {
0 : ['Albany', 'New York', 'United States', 'North America', 1.0]
1 : ['Amsterdam', 'North Holland', 'The Netherlands', 'Europe', 0.9506]
2 : ['Antwerp', 'Flemish Region', 'Belgium', 'Europe', 0.9506]
3 : ['Asheville', 'North Carolina', 'United States', 'North America', 1.0]
4 : ['Athens', 'Attica', 'Greece', 'Europe', 0.9506]
5 : ['Austin', 'Texas', 'United States', 'North America', 1.0]
6 : ['Bangkok', 'Central Thailand', 'Thailand', 'Asia', 34.6341]
7 : ['Barcelona', 'Catalonia', 'Spain', 'Europe', 0.9506]
8 : ['Barossa Valley', 'South Australia', 'Australia', 'Oceania', 1.4011]
9 : ['Barwon South West', 'Victoria', 'Australia', 'Oceania', 1.4011]
10 : ['Belize', 'Belize', 'Belize', 'North America', 2.0]
11 : ['Bergamo', 'Belize', 'Belize', 'North America', 2.0]
12 : ['Berlin', 'Berlin', 'Germany', 'Europe', 0.9506]
13 : ['Bologna', 'Emiglia-Romagna', 'Italy', 'Europe', 0.9506]
14 : ['Bordeaux', 'Bouvelle-Aquitaine', 'France', 'Europe', 0.9506]
15 : ['Boston', 'Massachusetts', 'United States', 'North America', 1.0]
16 : ['Bozeman', 'Montana', 'United States', 'North America', 1.0]
17 : ['Brisbane', 'Queensland', 'Australia', 'Oceania', 1.4011]
18 : ['Bristol', 'England', 'United Kingdom', 'Europe', 0.7905]
19 : ['Broward County', 'Florida', 'United States', 'North America', 1.0]
20 : ['Brussels', 'Brussels', 'Belgium', 'Europe', 0.9506]
21 : ['Buenos Aires', 'Ciudad Autonoma de Buenos Aires', 'Argentina', 'South America', 350.0]
22 : ['Cambridge', 'Massachusetts', 'United States', 'North America', 1.0]
23 : ['Cape Town', 'Western Cape', 'South Africa', 'Africa', 18.1078]
24 : ['Chicago', 'Illinois', 'United States', 'North America', 1.0]
25 : ['Clark County', 'Nevada', 'United States', 'North America', 1.0]
26 : ['Columbus', 'Ohio', 'United States', 'North America', 1.0]
27 : ['Copenhagen', 'Hovedstaden', 'Denmark', 'Europe', 7.14]
28 : ['Crete', 'Crete', 'Greece', 'Europe', 0.9506]
29 : ['Dallas', 'Texas', 'United States', 'North America', 1.0]
30 : ['Denver', 'Colorado', 'United States', 'North America', 1.0]
31 : ['Dublin', 'Leinster', 'Ireland', 'Europe', 0.9506]
32 : ['Edinburgh', 'Scotland', 'United Kingdom', 'Europe', 0.7905]
33 : ['Euskadi', 'Euskadi', 'Spain', 'Europe', 0.9506]
34 : ['Florence', 'Toscana', 'Italy', 'Europe', 0.9506]
35 : ['Fort Worth', 'Texas', 'United States', 'North America', 1.0]
36 : ['Geneva', 'Geneva', 'Switzerland', 'Europe', 0.8855]
37 : ['Ghent', 'Flemish Region', 'Belgium', 'Europe', 0.9506]
38 : ['Girona', 'Catalonia', 'Spain', 'Europe', 0.9506]
39 : ['Hawaii', 'Hawaii', 'United States', 'North America', 1.0]
40 : ['Hong Kong', 'Hong Kong', 'China', 'Asia', 7.21]
41 : ['Istanbul', 'Marmara', 'Turkey', 'Asia', 34.4653]
42 : ['Jersey City', 'New Jersey', 'United States', 'North America', 1.0]
43 : ['Lisbon', 'Lisbon', 'Portugal', 'Europe', 0.9506]
44 : ['London', 'England', 'United Kingdom', 'Europe', 0.7905]
45 : ['Los Angeles', 'California', 'United States', 'North America', 1.0]
46 : ['Lyon', 'Auvergne-Rhone-Alpes', 'France', 'Europe', 0.9506]
47 : ['Madrid', 'Comunidad de Madrid', 'Spain', 'Europe', 0.9506]
48 : ['Malaga', 'Andalucia', 'Spain', 'Europe', 0.9506]
49 : ['Mallorca', 'Islas Baleares', 'Spain', 'Europe', 0.9506]
50 : ['Manchester', 'England', 'United Kingdom', 'Europe', 0.7905]
51 : ['Melbourne', 'Victoria', 'Australia', 'Oceania', 1.4011]
52 : ['Menorca', 'Islas Baleares', 'Spain', 'Europe', 0.9506]
53 : ['Mexico City', 'Distrito Federal', 'Mexico', 'North America', 20.2313]
54 : ['Mid North Coast', 'New South Wales', 'Australia', 'Oceania', 1.4011]
55 : ['Milan', 'Lombardy', 'Italy', 'Europe', 0.9506]
56 : ['Montreal', 'Quebec', 'Canada', 'North America', 1.2651]
57 : ['Mornington Peninsula', 'Victoria', 'Australia', 'Oceania', 1.4011]
58 : ['Munich', 'Bavaria', 'Germany', 'Europe', 0.9506]
59 : ['Naples', 'Campania', 'Italy', 'Europe', 0.9506]
60 : ['Nashville', 'Tennessee', 'United States', 'North America', 1.0]
61 : ['New Brunswick', 'New Brunswick', 'Canada', 'North America', 1.2651]
62 : ['New Orleans', 'Louisiana', 'United States', 'North America', 1.0]
63 : ['New York City', 'New York', 'United States', 'North America', 1.0]
64 : ['New Zealand', 'New Zealand', 'New Zealand', 'Oceania', 1.7031]
65 : ['Newark', 'New Jersey', 'United States', 'North America', 1.0]
66 : ['Northern Rivers', 'New South Wales', 'Australia', 'Oceania', 1.4011]
67 : ['Oakland', 'California', 'United States', 'North America', 1.0]
68 : ['Oslo', 'Oslo', 'Norway', 'Europe', 11.0716]
69 : ['Ottawa', 'Ontario', 'Canada', 'North America', 1.2651]
70 : ['Pacific Grove', 'California', 'United States', 'North America', 1.0]
71 : ['Paris', 'Ile-de-France', 'France', 'Europe', 0.9506]
72 : ['Pays Basque', 'Pyrenees-Atlantiques', 'France', 'Europe', 0.9506]
73 : ['Portland', 'Oregon', 'United States', 'North America', 1.0]
74 : ['Porto', 'Norte', 'Portugal', 'Europe', 0.9506]
75 : ['Prague', 'Prague', 'Czech Republic', 'Europe', 22.13]
76 : ['Puglia', 'Puglia', 'Italy', 'Europe', 0.9506]
77 : ['Quebec City', 'Quebec', 'Canada', 'North America', 1.2651]
78 : ['Rhode Island', 'Rhode Island', 'United States', 'North America', 1.0]
79 : ['Riga', 'Riga', 'Latvia', 'Europe', 0.9506]
80 : ['Rio de Janeiro', 'Rio de Janeiro', 'Brazil', 'South America', 5.2]
81 : ['Rochester', 'New York', 'United States', 'North America', 1.0]
82 : ['Rome', 'Lazio', 'Italy', 'Europe', 0.9506]
83 : ['Rotterdam', 'South Holland', 'The Netherlands', 'Europe', 0.9506]
84 : ['Salem', 'Oregon', 'United States', 'North America', 1.0]
85 : ['San Diego', 'California', 'United States', 'North America', 1.0]
86 : ['San Francisco', 'California', 'United States', 'North America', 1.0]
87 : ['San Mateo County', 'California', 'United States', 'North America', 1.0]
88 : ['Santa Clara Country', 'California', 'United States', 'North America', 1.0]
89 : ['Santa Cruz Country', 'California', 'United States', 'North America', 1.0]
90 : ['Santiago', 'Region Metropolitana de Santiago', 'Chile', 'Europe', 900.0]
91 : ['Seattle', 'Washington', 'United States', 'North America', 1.0]
92 : ['Sevilla', 'Andalucia', 'Spain', 'Europe', 0.9506]
93 : ['Sicily', 'Sicilia', 'Italy', 'Europe', 0.9506]
94 : ['Singapore', 'Singapore', 'Singapore', 'Asia', 1.3445]
95 : ['South Aegean', 'South Aegean', 'Greece', 'Europe', 0.9506]
96 : ['Stockholm', 'Stockholm Ian', 'Sweden', 'Europe', 11.0473]
97 : ['Sunshine Coast', 'Queensland', 'Australia', 'Oceania', 1.4011]
98 : ['Syndey', 'New South Wales', 'Australia', 'Oceania', 1.4011]
99 : ['Taipei', 'Northern Taiwan', 'Taiwan', 'Asia', 32.54]
100 : ['Tasmania', 'Tasmania', 'Australia', 'Oceania', 1.4011]
101 : ['The Hague', 'South Holland', 'The Netherlands', 'Europe', 0.9506]
102 : ['Thessaloniki', 'Central Macedonia', 'Greece', 'Europe', 0.9506]
103 : ['Tokyo', 'Kanto', 'Japan', 'Asia', 155.51]
104 : ['Toronto', 'Ontario', 'Canada', 'North America', 1.2651]
105 : ['Trentino', 'Trentino-Alto Adige/Sudtirol', 'Italy', 'Europe', 0.9506]
106 : ['Twin Cities', 'Minnesota', 'United States', 'North America', 1.0]
107 : ['Valencia', 'Valencia', 'Spain', 'Europe', 0.9506]
108 : ['Vancouver', 'British Columbia', 'Canada', 'North America', 1.2651]
109 : ['Vaud', 'Vaud', 'Switzerland', 'Europe', 0.8855]
110 : ['Venice', 'Veneto', 'Italy', 'Europe', 0.9506]
111 : ['Victoria', 'British Columbia', 'Canada', 'North America', 1.2651]
112 : ['Vienna', 'Vienna', 'Austria', 'Europe', 0.9506]
113 : ['Washington DC', 'District of Columbia', 'United States', 'North America', 1.0]
114 : ['Western Australia', 'Western Australia', 'Australia', 'Oceania', 1.4011]
115 : ['Winnipeg', 'Manitoba', 'Canada', 'North America', 1.2651]
116 : ['Zurich', 'Zurich', 'Switzerland', 'Europe', 0.8855]
}
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
}

locations = pd.DataFrame.from_dict(locations, orient='index', columns = ['city', 'region', 'country', 'continent', 'exchange'])
locations = locations.reset_index()
locations['location_id'] = locations['index']
locations = locations.drop('index', axis=1)

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

for i in range(len(xls)):
    try:
        df = pd.DataFrame(pd.read_excel(xls[i]))
        df["location_id"] = i
        df['host_since']=df['host_since'].dt.date
        i_main = df[main_columns]
        main = pd.concat([main, i_main], ignore_index = True)
        i_hosts = df[hosts_columns]
        hosts = pd.concat([hosts, i_hosts], ignore_index = True)        
        i_reviews = df[reviews_columns]
        reviews = pd.concat([reviews, i_reviews], ignore_index = True)
        print(f"location_id {i} loaded successfully")
    except Exception as e:
        print(f"issue loading location_id {i}")
        continue

main['price'] = main['price'].str.replace(r'[^\d.]', '', regex=True).astype(float)
hosts['host_since'] = pd.to_datetime(hosts['host_since'], format='%y/%m/%d %H:%M:%S')

bool_cols = ['host_is_superhost', 'host_has_profile_pic', 'host_identity_verified']
for i in bool_cols:
    hosts[i] = hosts[i].replace('t', True)
    hosts[i] = hosts[i].replace('f', False)

main = main.replace({np.nan: None})
main.astype(main_dtypes).dtypes

hosts = hosts.replace({np.nan: None})
hosts.astype(hosts_dtypes).dtypes

reviews = reviews.replace({np.nan: None})
reviews.astype(reviews_dtypes).dtypes
