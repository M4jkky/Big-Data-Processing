import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath("__file__"))))

from pyspark.ml.feature import StringIndexer, OneHotEncoder, Bucketizer, VectorAssembler, StandardScaler
from pyspark.sql.functions import col, log2, sum, count, when, median
from integration.integration import integration
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("preprocessing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def fillna_median(df, include=None):
    if include is None:
        include = set()
    medians = df.agg(*(median(x).alias(x) for x in df.columns if x in include))
    return df.fillna(medians.first().asDict())

def preprocess_data():
    df_train, df_test = integration()
    
    df_train = df_train.drop("Accident_Index", "Date", "Time", "Longitude", "Latitude", "LSOA_of_Accident_Location")
    df_test = df_test.drop("Accident_Index", "Date", "Time", "Longitude", "Latitude", "LSOA_of_Accident_Location")
    
    df_train = fillna_median(df_train, ['Location_Easting_OSGR', 'Location_Northing_OSGR'])
    df_test = fillna_median(df_test, ['Location_Easting_OSGR', 'Location_Northing_OSGR'])
    
    categorical_attributes = [x for x, y in df_train.dtypes if y == 'string']
    if categorical_attributes:
        index_output_cols = [x + "_SI" for x in categorical_attributes]
        ohe_output_cols = [x + "_OHE" for x in categorical_attributes]
        
        indexer = StringIndexer(inputCols=categorical_attributes, outputCols=index_output_cols, handleInvalid="skip")
        df_train = indexer.fit(df_train).transform(df_train)
        df_test = indexer.fit(df_test).transform(df_test)
        
        ohe = OneHotEncoder(inputCols=index_output_cols, outputCols=ohe_output_cols)
        df_train = ohe.fit(df_train).transform(df_train)
        df_test = ohe.fit(df_test).transform(df_test)
        
        df_train = df_train.drop(*index_output_cols)
        df_train = df_train.drop(*categorical_attributes)
        df_test = df_test.drop(*index_output_cols)
        df_test = df_test.drop(*categorical_attributes)
    
    driver_age_splits = [-float("inf"), 18, 25, 40, 60, float("inf")]
    driver_age_bucketizer = Bucketizer(splits=driver_age_splits, inputCol="Age_of_Driver", 
                                      outputCol="Age_of_Driver_Cat", handleInvalid="skip")
    
    vehicle_age_splits = [-float("inf"), 0, 2, 6, 11, float("inf")]
    vehicle_age_bucketizer = Bucketizer(splits=vehicle_age_splits, inputCol="Age_of_Vehicle", 
                                       outputCol="Age_of_Vehicle_Cat", handleInvalid="skip")
    
    engine_capacity_splits = [-float("inf"), -1, 0, 1000, 2000, 3000, 5000, 8000, float("inf")]
    engine_bucketizer = Bucketizer(splits=engine_capacity_splits, inputCol="Engine_Capacity_(CC)", 
                                  outputCol="Engine_Size_Cat", handleInvalid="skip")
    
    vehicle_type_splits = [-float("inf"), 0, 1.5, 2.5, 3.5, 4.5, 5.5, 7, 8.5, 9.5, 10.5, 11.5, 
                          16.5, 17.5, 18.5, 19.5, 20.5, 21.5, 90.5, float("inf")]
    vehicle_type_bucketizer = Bucketizer(splits=vehicle_type_splits, inputCol="Vehicle_Type", 
                                        outputCol="Vehicle_Type_Cat", handleInvalid="keep")
    
    for col_name, bucketizer in [
        ("Age_of_Driver", driver_age_bucketizer),
        ("Age_of_Vehicle", vehicle_age_bucketizer),
        ("Engine_Capacity_(CC)", engine_bucketizer),
        ("Vehicle_Type", vehicle_type_bucketizer)
    ]:
        if col_name in df_train.columns:
            df_train = bucketizer.transform(df_train)
            df_test = bucketizer.transform(df_test)
            
    columns_to_drop = ["Age_of_Driver", "Age_of_Vehicle", "Engine_Capacity_(CC)", "Vehicle_Type"]
    for col_name in columns_to_drop:
        if col_name in df_train.columns:
            df_train = df_train.drop(col_name)
        if col_name in df_test.columns:
            df_test = df_test.drop(col_name)
            
    numerical_cols_to_scale = [col_name for col_name, dtype in df_train.dtypes 
                              if dtype in ['int', 'double'] 
                              and col_name not in ['Age_of_Driver_Cat', 'Age_of_Vehicle_Cat', 
                                                 'Engine_Size_Cat', 'Vehicle_Type_Cat']]
    
    if numerical_cols_to_scale:
        assembler_numerical = VectorAssembler(
            inputCols=numerical_cols_to_scale,
            outputCol="numerical_features_vec",
            handleInvalid="skip"
        )
        df_train = assembler_numerical.transform(df_train)
        df_test = assembler_numerical.transform(df_test)
        
        scaler = StandardScaler(
            inputCol="numerical_features_vec",
            outputCol="scaled_numerical_features",
            withStd=True,
            withMean=True
        )
        
        scaler_model = scaler.fit(df_train)
        df_train = scaler_model.transform(df_train)
        df_test = scaler_model.transform(df_test)
    
    low_ig_cols = [
        "Day_of_Week", "1st_Road_Class", "Road_Type", "Pedestrian_Crossing-Human_Control",
        "Pedestrian_Crossing-Physical_Facilities", "Light_Conditions", "Weather_Conditions",
        "Road_Surface_Conditions", "Special_Conditions_at_Site", "Carriageway_Hazards",
        "Casualty_Class", "Sex_of_Casualty", "Age_of_Casualty", "Age_Band_of_Casualty",
        "Pedestrian_Location", "Pedestrian_Movement", "Car_Passenger", "Bus_or_Coach_Passenger",
        "Pedestrian_Road_Maintenance_Worker", "Casualty_Home_Area_Type", "Towing_and_Articulation",
        "Vehicle_Location-Restricted_Lane", "Skidding_and_Overturning", "Hit_Object_in_Carriageway",
        "Hit_Object_off_Carriageway", "Was_Vehicle_Left_Hand_Drive?", "Journey_Purpose_of_Driver",
        "Sex_of_Driver", "Age_of_Driver", "Age_Band_of_Driver", "Propulsion_Code", "Age_of_Vehicle",
        "Driver_IMD_Decile", "Driver_Home_Area_Type", "Age_of_Driver_Cat", "Age_of_Vehicle_Cat",
        "Engine_Size_Cat"
    ]
    
    for col_name in low_ig_cols:
        if col_name in df_train.columns:
            df_train = df_train.drop(col_name)
        if col_name in df_test.columns:
            df_test = df_test.drop(col_name)
    
    exclude_cols = ["Accident_Severity", "numerical_features_vec", "scaled_numerical_features", 
                    "Local_Authority_(Highway)", "Local_Authority_(Highway)_SI",
                    "Local_Authority_(Highway)_OHE"]
    
    feature_cols = [col for col in df_train.columns if col not in exclude_cols]
    
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip"
    )
    
    df_train_model = assembler.transform(df_train)
    df_test_model = assembler.transform(df_test)
    
    return df_train_model, df_test_model