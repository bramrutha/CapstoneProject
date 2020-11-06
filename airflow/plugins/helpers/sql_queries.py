class SqlQueries:
    dates_create_table = ("""CREATE TABLE dates (
                                date VARCHAR PRIMARY KEY,
                                year INTEGER,
                                month INTEGER,
                                day   INTEGER,
                                dayofweek INTEGER,
                                dayofyear INTEGER)""")

    immigration_create_table = (""" CREATE TABLE immigration (
                                        cicid INTEGER PRIMARY KEY,
                                        i94yr INTEGER,
                                        i94mo INTEGER,
                                        i94cit INTEGER,
                                        i94res INTEGER,
                                        i94port VARCHAR,
                                        i94visa INTEGER,
                                        i94mode INTEGER,
                                        i94bir INTEGER,
                                        arrdate DATE,
                                        depdate DATE,
                                        airline VARCHAR,
                                        fltno VARCHAR,
                                        visatype VARCHAR,
                                        gender VARCHAR,
                                        i94addr VARCHAR
    """)

    weather_create_table = ("""CREATE TABLE weather (
                                    Country_code INTEGER PRIMARY KEY,
                                    Country VARCHAR,
                                    TEMPERATURE FLOAT,
                                    LATITUDE   VARCHAR,
                                    LONGITUDE VARCHAR
    """)

    state_table_create = ("""CREATE TABLE states(
                                    State_code VARCHAR PRIMARY KEY,
                                    Total_population INTEGER,
                                    Female_population INTEGER,
                                    Median_age FLOAT,
                                    Male_population INTEGER,
                                    State VARCHAR,
                                    AmericanIndian_and_AlaskaNative INTEGER,
                                    Asian INTEGER,
                                    Black_or_AfricanAmerican INTEGER,
                                    Hispanic_or_Latino INTEGER,
                                    White INTEGER    
    """)

    airport_table_create = ("""CREATE TABLE airports(
                                        ident VARCHAR PRIMARY KEY,
                                        type VARCHAR,
                                        name VARCHAR,
                                        iso_country VARCHAR,
                                        iata_code VARCHAR
        """)