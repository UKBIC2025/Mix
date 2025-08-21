from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone
import re
import os
from bisect import bisect_left
import pytz
import polars as pl
from flatten_json import flatten
 
# Set working directory
os.chdir(r'C:\Users\Nivedidha.Kumaravelu\OneDrive - UKBIC LTD\Documents')
 
# Connect to MongoDB
client = MongoClient(
    "mongodb://readuser:Zci5G%23%25b200Q@10.100.80.24:27017/"
    "?retryWrites=true&serverSelectionTimeoutMS=5000&connectTimeoutMS=10000"
    "&authSource=admin&authMechanism=SCRAM-SHA-256"
)
 
# Load Commissioning Tracker
CommissioningTracker = (
    "C:/Users/Nivedidha.Kumaravelu/UKBIC LTD/Operations Department Site - "
    "Released Commissioning Material/Commissioning Product Tracker NEW.xlsx"
)
xl = pl.read_excel(
    CommissioningTracker,
    sheet_name='Product Tracker Power BI',
    has_header=True,
    read_options={"header_row": 2}
)
xl_mix = xl.filter(
    xl["Part Number"].str.contains("SLUR", literal=True).fill_null(False)
    | xl["Functional Description of Part"].str.contains("Slur", literal=True).fill_null(False)
)
 
# Check MongoDB connection
for db_name in client.list_databases():
    print(db_name)
db = client['TRACE']
 
# Query TraceData
collection = db['TraceData']
query = {
    "workplaceId": {"$in": [578352]},  # Anode + Cathode Mixing
    "params.19.pVO": 1
}
project = {
    "operationContext.materialNumber": 1,
    "workplaceId": 1,
    "params": 1
}
query_result = list(collection.find(query, projection=project).sort("created", -1))
df_norm = pd.json_normalize(query_result)  # not used later but harmless
 
# Flatten query
dic_flattened = [flatten(d) for d in query_result]
for d in dic_flattened:
    for k in (
        "params_17_pVO", "params_10_pVO", "params_11_pVO", "params_12_pVO",
        "params_13_pVO", "params_14_pVO", "params_15_pVO", "params_16_pVO"
    ):
        if k in d and d[k] is not None:
            d[k] = float(str(d[k]))
    if "params_18_pVO" in d:
        d["params_18_pVO"] = str(d["params_18_pVO"])
 
df = pl.DataFrame(dic_flattened)
 
# Filter for mixes
if "params_17_pVO" in df.columns:
    df = df.filter(pl.col("params_17_pVO").cast(pl.Float64) > 140)
else:
    print("WARNING: 'params_17_pVO' not found in TraceData results. Exiting early.")
    df = pl.DataFrame([])
 
# pNID mapping
collection = db['ProcessData']
mapping = {
    1: "Main Rotor Speed (m/s)",
    2: "Main Vessel Power (kW)",
    3: "Main Vessel Torque (nM)",
    4: "Main Vessel Current (A)",
    5: "Pan Speed (m/s)",
    6: "Main Rotor Power (kW)",
    7: "Main Rotor Torque (nM)",
    8: "Main Rotor Current (A)",
    9: "Main Vessel Slurry Temp (C)",
    10: "Chilled Water Feed Temp (C)",
    11: "Chilled Water Return Temp (C)",
    12: "Nitrogen Flow Rate l/min",
    13: "Viscosity",
    14: "Transfer Vessel Supply Vacuum (mB)",
    15: "Slurry Feedrate Mixer to Vessel (Kg/min)",
    16: "Transfer Vessel Slurry Temp (C)",
    17: "Degas pump output Vacuum",
    18: "Water NMP Temp (C)",
    19: "Step Number"
}
mapping_str_keys = {str(k): v for k, v in mapping.items()}
 
full_mixing_data = pl.DataFrame()
 
# Loop over mixes
# ...existing code...
for i in range(len(df)):
    # After creating your DataFrame 'df' from dic_flattened:
    if "created" in df.columns and "modified" in df.columns:
        df = df.with_columns([
        pl.col("created").str.strptime(pl.Datetime, fmt="%Y-%m-%dT%H:%M:%S%.fZ", strict=False),
        pl.col("modified").str.strptime(pl.Datetime, fmt="%Y-%m-%dT%H:%M:%S%.fZ", strict=False)
    ])
        start_time = df['created'][i]
        end_time = df['modified'][i]
        workplaceId = df['workplaceId'][i]
 
        pipeline = [
            {
                '$match': {
                    "workplaceId": int(workplaceId),
                    'tsMin': {
                        '$gte': start_time,
                        '$lte': end_time + timedelta(hours=1)
                    }
                }
            },
            {'$unwind': '$values'},
            {
                '$project': {
                    'pNID': 1,
                    'tsMin': 1,
                    'values': 1,
                    'workplaceId': 1,
                    '_id': 0
                }
            }
        ]
    result = list(collection.aggregate(pipeline))
    if not result:
        print(f"Iteration {i} — no documents returned; skipping")
        print(df['created'].head(3))
        print(df['modified'].head(3))
        continue
 
    # Flatten results
    dic_flat = [flatten(d) for d in result]
    for dct in dic_flat:
        if "values_pOV" in dct and dct["values_pOV"] is not None:
            dct["values_pOV"] = float(str(dct["values_pOV"]))
        if "values_pV" in dct and dct["values_pV"] is not None:
            dct["values_pV"] = float(str(dct["values_pV"]))
 
    df_pd = pl.DataFrame(dic_flat)
 
    # pNID mapping
    if "pNID" in df_pd.columns:
        unmapped = set(df_pd["pNID"].cast(str).unique().to_list()) - set(mapping_str_keys.keys())
        if unmapped:
            print(f"Iteration {i} — WARNING: Unmapped pNIDs: {unmapped}")
        df_pd = df_pd.with_columns(pl.col("pNID").cast(str).replace(mapping_str_keys))
    else:
        print(f"Iteration {i} — No 'pNID' column; skipping mapping")
 
    # Ensure timestamps
    if "values_ts" not in df_pd.columns:
        print(f"Iteration {i} — No 'values_ts' column; skipping")
        continue
    df_pd = df_pd.with_columns(pl.from_epoch(pl.col("values_ts"), time_unit="ms"))
 
    # Add metadata columns
    def safe_lit(col, alias):
        if col in df.columns:
            return pl.lit(df[col][i]).alias(alias)
        return pl.lit(None).alias(alias)
 
    df_pd = df_pd.with_columns([
        safe_lit("operationContext_materialNumber", "Slurry"),
        safe_lit("params_3_pVO", "Active_ID"),
        safe_lit("params_4_pVO", "Binder_ID"),
        safe_lit("params_5_pVO", "Conductive_ID"),
        safe_lit("params_6_pVO", "Additive_ID"),
        safe_lit("params_8_pVO", "Liquid_Binder_ID"),
        safe_lit("params_10_pVO", "Active_weight"),
        safe_lit("params_11_pVO", "Binder_weight"),
        safe_lit("params_12_pVO", "Conductive_weight"),
        safe_lit("params_13_pVO", "Additive_weight"),
        safe_lit("params_14_pVO", "Solvent_weight"),
        safe_lit("params_15_pVO", "Liquid_Binder_weight"),
        safe_lit("params_17_pVO", "Mix_weight"),
        safe_lit("params_18_pVO", "MES Lot Number"),
    ])
 
    # Add Mix_ID from tracker
    if "MES Lot Number" in df_pd.columns and df_pd.height > 0:
        mes_lot_number = df_pd['MES Lot Number'][0]
        match = xl_mix.filter(pl.col('LN/ MES LOT Number') == mes_lot_number)
        if match.height > 0:
            df_pd = df_pd.with_columns(pl.lit(match['Mix ID 1 # '][0]).alias("Mix_ID"))
        else:
            df_pd = df_pd.with_columns(pl.lit(f'NA{i}').alias("Mix_ID"))
    else:
        df_pd = df_pd.with_columns(pl.lit(f'NA{i}').alias("Mix_ID"))
 
    # Assign step numbers
    if "pNID" in df_pd.columns and df_pd.filter(pl.col("pNID") == "Step Number").height > 0:
        list_of_steps = df_pd.filter(pl.col('pNID') == 'Step Number')
        if (
            list_of_steps.height > 1
            and "values_ts" in list_of_steps.columns
            and "values_pOV" in list_of_steps.columns
        ):
            step_times = list(reversed(list_of_steps['values_ts']))[:-1]
            step_values = list(reversed(list_of_steps['values_pOV']))[:-1]
            if step_values:
                df_pd = df_pd.with_columns(
                    df_pd["values_ts"].map_elements(
                        lambda x: step_values[max(0, bisect_left(step_times, x) - 1)]
                    ).alias("step")
                )
            else:
                df_pd = df_pd.with_columns(pl.lit(0.0).alias("step"))
        else:
            df_pd = df_pd.with_columns(pl.lit(0.0).alias("step"))
    else:
        df_pd = df_pd.with_columns(pl.lit(0.0).alias("step"))
 
    # Append to full dataset
    full_mixing_data = pl.concat([full_mixing_data, df_pd], how="vertical")
    print(f"Processed row {i}")
 
# Save results
full_mixing_data.write_csv('mixingAuto.csv')
print("Saved mixingAuto3.csv")
 
