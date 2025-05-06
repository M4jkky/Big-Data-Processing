from pyspark.sql import SparkSession

def integration():
    # Vytvorenie SparkSession
    spark = SparkSession.builder.appName("integration").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Načítanie dát
    df_accidents = spark.read.csv("../data.tmp/CarAccidents/Accidents.csv", header=True, inferSchema=True)
    df_casualties = spark.read.csv("../data.tmp/CarAccidents/Casualties.csv", header=True, inferSchema=True)
    df_vehicles = spark.read.csv("../data.tmp/CarAccidents/Vehicles.csv", header=True, inferSchema=True)
    df_vehicles = df_vehicles.drop("Vehicle_Reference")

    # Spojenie dát podľa Accident_Index
    df = df_accidents.join(df_casualties, ["Accident_Index"], "full")
    df = df.join(df_vehicles, ["Accident_Index"], "full")

    # Vytvorenie vzorky 10% dát stratifikovane
    df_sampled = df.sampleBy("Accident_Severity", fractions={1: 0.1, 2: 0.1, 3: 0.1}, seed=1234)

    # Rozdelenie dát na trénovaciu a testovaciu množinu
    df_train, df_test = df_sampled.randomSplit([0.70, 0.30], seed=1234)

    return df_train, df_test