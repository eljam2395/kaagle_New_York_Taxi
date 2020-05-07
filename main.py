from pyspark.sql.functions import mean, udf, date_format
from math import *


def haversine_dist(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on earth
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    # Radius of earth in kilometers. Use 3956 for miles
    r = 6371
    return c * r


def mean_speed(df):
    """ Caculate the mean speed on each drive.
        in param:
            dataframe that contains the id, the trip duration and the pickup/dropoff coordinates
        out param:
            dataframe that contains the id and the mean speed of the drive"""
    udf_distance = udf(haversine_dist)

    # conversion to hour
    distance = df.withColumn('trip_duration', df.trip_duration.cast('float') / 3600.0)

    distance = distance.withColumn('dist', udf_distance(df.pickup_longitude.cast('float'),
                                                        df.pickup_latitude.cast('float'),
                                                        df.dropoff_longitude.cast('float'),
                                                        df.dropoff_latitude.cast('float')).cast('float'))

    df_mean_speed = distance.withColumn('meanSpeed(km/h)', distance.dist / distance.trip_duration)

    return df_mean_speed.select("id", "meanSpeed(km/h)")


def get_nb_drive_per_day(df):
    """ calculate the number of drives per day of week.
    in param:
            dataframe that contains the pickup_datetime
        out param:
            dataframe that contains the day of week and the number of drives """

    day = df.withColumn('pickup_datetime', date_format(df.pickup_datetime, "E"))
    return day.groupBy('pickup_datetime').count()


def get_nb_drive_per_slice_day(df):
    """ calculate the number of drives per slice of day. A slice of day corresponds to six hours.
    in param:
            dataframe that contains the pickup_datetime
        out param:
            dictionnary that contains the drives per slice of day """

    hour = df.withColumn('pickup_datetime', date_format(df.pickup_datetime, "H"))

    zero_to_six = hour.filter(hour.pickup_datetime.between(0, 6)).count()
    six_to_twelve = hour.filter(hour.pickup_datetime.between(7, 12)).count()
    twelve_to_eighteen = hour.filter(hour.pickup_datetime.between(13, 18)).count()
    eighteen_to_twenty_for = hour.filter(hour.pickup_datetime.between(19, 26)).count()

    return {"0-6": zero_to_six, "6-12": six_to_twelve, "12-18": twelve_to_eighteen, "18-24": eighteen_to_twenty_for}


def get_km_per_day(df):
    """ calculate the number of km done per day.
    in param:
            dataframe that contains the pickup_datetime and pickup/dropoff coordinates
        out param:
            dataframe that contains the km per day """

    udf_distance = udf(haversine_dist)

    km_by_day = df.withColumn("dist", udf_distance(df.pickup_longitude.cast('float'),
                                                   df.pickup_latitude.cast('float'),
                                                   df.dropoff_longitude.cast('float'),
                                                   df.dropoff_latitude.cast('float')).cast('float'))

    km_by_day = km_by_day.withColumn('pickup_datetime', date_format(km_by_day.pickup_datetime, "E"))
    return km_by_day.groupBy('pickup_datetime').sum()
