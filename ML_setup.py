import os
import mysql.connector
import pandas as pd
import fastparquet

os.chdir("/Users/jake/Library/CloudStorage/OneDrive-AlbanyBeck/airbnb") # Mac

db = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "",
    database = "airbnb"
)

cursor = db.cursor()

cursor.execute('SELECT * FROM listings')
listings = cursor.fetchall()
listings = pd.DataFrame(listings)

cursor.execute('SELECT * FROM locations;')
locations = cursor.fetchall()
locations = pd.DataFrame(locations)

cursor.execute('SELECT * FROM hosts;')
hosts = cursor.fetchall()
hosts = pd.DataFrame(hosts)

cursor.execute('SELECT * FROM reviews;')
reviews = cursor.fetchall()
reviews = pd.DataFrame(reviews)

listings.columns = [
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
    "availability_365",
    "price_std"
]
listings_dtypes = {
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
    "availability_365": "Int64",
    "price_std": "Float64"    
}
listings.astype(listings_dtypes).dtypes

locations.columns = [
    "city",
    "region",
    "country",
    "continent",
    "location_id",
    "exchange"
]
locations_dtypes = {
    "city":"object",
    "region":"object",
    "country":"object",
    "continent":"object",
    "location_id":"Int64",
    "exchange":"Float64"
}
locations.astype(locations_dtypes).dtypes

hosts.columns = [
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
hosts.astype(hosts_dtypes).dtypes

reviews.columns = [
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
reviews.astype(reviews_dtypes).dtypes


locations.to_parquet('locations.parquet')
listings.to_parquet('listings.parquet')
hosts.to_parquet('hosts.parquet')
reviews.to_parquet('reviews.parquet')
