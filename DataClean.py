import numpy as np
import pandas as pd
import os
import re
import csv
import unicodedata
from mrjob.job import MRJob
from mrjob.step import MRStep

DEFINE_CITY = ["Las Vegas", "Phoenix", "Toronto", \
"Charlotte", "Scottsdale", "Pittsburgh", "Mesa", \
"Montreal", "Henderson", "Tempe", "Chandler", "Edinburgh"\
"Cleveland", "Madison", "Glendale", "Gilbert", "Mississauga"]

def new_data():

    dta = pd.read_csv("business&&.csv")

    cols = ["attributes.RestaurantsTakeOut", "attributes.GoodForKids", "business_id", "name",\
    "attributes.RestaurantsReservations", "attributes.Ambience.casual",\
    "state", "hours.Tuesday", "hours.Thursday", "attributes.GoodForDancing",\
    "attributes.BYOB", "attributes.Music.live", "attributes.BusinessAcceptsCreditCards",\
    "attributes.RestaurantsGoodForGroups", "is_open", "categories",\
    "attributes.Ambience.trendy", "attributes.DietaryRestrictions.vegan",\
    "longitude", "neighborhood", "attributes.DietaryRestrictions.vegetarian",\
    "address", "attributes.OutdoorSeating", "attributes.GoodForMeal.brunch",\
    "attributes.GoodForMeal.latenight", "attributes.ByAppointmentOnly",\
    "attributes.RestaurantsDelivery", "attributes.GoodForMeal.dessert",\
    "review_count", "hours.Wednesday", "attributes.Ambience.romantic",\
    "attributes.RestaurantsCounterService", "attributes.Music.dj",\
    "attributes.Ambience.upscale", "latitude", "hours.Monday", "attributes.Alcohol",\
    "attributes.Ambience.classy", "attributes.RestaurantsPriceRange2",\
    "hours.Sunday", "attributes.NoiseLevel", "attributes.DietaryRestrictions.dairy-free",\
    "hours.Friday", "attributes.RestaurantsTableService", "attributes.Music.background_music",\
    "attributes.Open24Hours", "city", "attributes.DietaryRestrictions.gluten-free",\
    "attributes.RestaurantsAttire", "hours.Saturday", "attributes.HappyHour",\
    "attributes.GoodForMeal.dinner", "attributes.GoodForMeal.lunch",\
    "attributes.WheelchairAccessible", "attributes.AgesAllowed",\
    "stars", "attributes.WiFi", "postal_code", "attributes.GoodForMeal.breakfast",\
    "attributes.DogsAllowed"]

    dta = dta[cols]
    dta["restaurant"] = [1 if re.findall('Restaurants', s) else 0 for s in dta["categories"]]
    dta = dta[dta["restaurant"] == 1]
    dta.to_csv("all_restaurant.csv", sep=",")

def rest_dta():
    dta = pd.read_csv("restaurant.csv")
    dta.to_csv("restaurant|.csv", sep="|")

def unicode_to_string(line):

    for i in range(len(line)):
        line[i] = line[i].encode('utf-8')
    return line


class MRDataClean(MRJob):

    def mapper_init(self):
        self.cols = cols

    def mapper(self, _, line):

        line = line.split('|')
        line = np.array(line[self.cols])
        if "Toronto" in city:
            restaurant = re.findall('Restaurants', category)
            if restaurant:
                yield postal_code, 1


    def combiner(self, postal_code, counts):

        yield postal_code, sum(counts)

    def reducer(self, postal_code, counts):

        yield postal_code, sum(counts)


if __name__ == '__main__':

    cols = select_cols()

    MRDataClean.run()
