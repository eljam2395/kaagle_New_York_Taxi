from pyspark.sql.functions import mean, udf
from math import *


def haversine_dist(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r


def mean_speed(df):
    udf_distance = udf(haversine_dist)
    distance = df.select(df.id, df.trip_duration.cast('float'), df.pickup_longitude.cast('float'),
                         df.pickup_latitude.cast('float'), df.dropoff_longitude.cast('float'),
                         df.dropoff_latitude.cast('float'))

    distance = distance.withColumn('trip_duration', distance.trip_duration / 3600.0)
    distance = distance.withColumn('dist', udf_distance(distance.pickup_longitude, distance.pickup_latitude,
                                                        distance.dropoff_longitude, distance.dropoff_latitude))

    df_mean_speed = distance.withColumn('meanSpeed(km/h)', distance.dist / distance.trip_duration)

    columns_to_drop = ["pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "dist",
                       "trip_duration"]
    df_mean_speed = df_mean_speed.drop(*columns_to_drop)

    return df_mean_speed
