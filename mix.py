from pymongo import MongoClient
from bson.decimal128 import Decimal128
from datetime import datetime, timedelta
from bisect import bisect_left
import os
import polars as pl
from flatten_json import flatten

# -------------------------
# Set working directory
# -------------------------
os.chdir(r'C:\Users\Nivedidha.Kumaravelu\OneDrive - UKBIC LTD\Documents')

# -------------------------
# Connect to MongoDB
# -------------------------
client = MongoClient(
    "mongodb://readuser:Zci5G%23%25b200Q@10.100.80.24:27017/"
    "?retryWrites=true&serverSelectionTimeoutMS=5000&connectTimeoutMS=10000"
    "&authSource=admin&authMechanism=SCRAM-SHA-256"
)
db = client['TRACE']

# -------------------------
# Load Commissioning Tracker
# -------------------------
CommissioningTracker = (
    "C:/Users/Nivedidha.Kumaravelu/UKBIC LTD/"
    "Operations Department Site - Released Commissioning Material/Commissioning Product Tracker NEW.xlsx"
)
xl = pl.read_excel(
    CommissioningTracker,
    sheet_name='Product Tracker Power BI',
    has_header=True,
    read_options={"header_row": 2}
)

xl_mix = xl.filter(
    xl["Part Number"].str.contains("SLUR", literal=True).fill_null(False) |
    xl["Functional Description of Part"].str.contains("Slur", literal=True).fill_null(False)
)

# -------------------------
# TraceData query: only 2025+
# -------------------------
start_date = datetime(2024, 1, 1)

trace_query = {
    "workplaceId": {"$in": [578352, 578351]},
    "params.19.pVO": 1,
    "created": {"$gte": start_date}  # only 2025+
}

projection = {
    "operationContext.materialNumber": 1,
    "workplaceId": 1,
    "params": 1
}

trace_result = list(db['TraceData'].find(trace_query, projection=projection))

# Flatten TraceData
flat_trace = [flatten(d) for d in trace_result]

# Convert numeric params to float, handle Decimal128
for d in flat_trace:
    for key in ["params_10_pVO", "params_11_pVO", "params_12_pVO", "params_13_pVO",
                "params_14_pVO", "params_15_pVO", "params_16_pVO", "params_17_pVO"]:
        if key in d and d[key] is not None:
            if isinstance(d[key], Decimal128):
                d[key] = float(d[key].to_decimal())
            else:
                d[key] = float(d[key])
    if "params_18_pVO" in d:
        d["params_18_pVO"] = str(d["params_18_pVO"])

df = pl.DataFrame(flat_trace)
df = df.filter(df["params_17_pVO"].cast(pl.Float64) > 140)

# -------------------------
# ProcessData setup
# -------------------------
process_mapping = {
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

full_mixing_data = pl.DataFrame()

# -------------------------
# Loop over filtered TraceData
# -------------------------
for i in range(df.height):
    start_time = df['params_1_pVO'][i]
    end_time = df['params_2_pVO'][i]
    workplaceId = df['workplaceId'][i]

    # Skip rows before 2025
    if start_time < start_date:
        continue

    # ProcessData aggregation pipeline
    pipeline = [
        {"$match": {
            "workplaceId": int(workplaceId),
            "tsMin": {"$gte": start_time, "$lte": end_time + timedelta(hours=1)}
        }},
        {"$unwind": "$values"},
        {"$project": {"pNID": 1, "tsMin": 1, "values": 1, "workplaceId": 1, "_id": 0}}
    ]

    result = db['ProcessData'].aggregate(pipeline)
    flat_proc = [flatten(d) for d in result]

    # Convert Decimal128 to float
    for d in flat_proc:
        for key in ["values_pOV", "values_pV"]:
            if key in d and d[key] is not None:
                if isinstance(d[key], Decimal128):
                    d[key] = float(d[key].to_decimal())
                else:
                    d[key] = float(d[key])

    df_pd = pl.DataFrame(flat_proc)

    # Map pNID names
    if df_pd.height > 0 and "pNID" in df_pd.columns:
        df_pd = df_pd.with_columns(
            pl.col("pNID").cast(str).replace(process_mapping)
        )

    # Safe conversion of values_ts to datetime
    if "values_ts" in df_pd.columns and df_pd.height > 0:
        df_pd = df_pd.with_columns(pl.col("values_ts").cast(pl.Int64))
        df_pd = df_pd.with_columns(pl.from_epoch(pl.col("values_ts"), time_unit="ms").alias("values_time"))
    else:
        df_pd = df_pd.with_columns(pl.lit(None).alias("values_time"))

    # Add TraceData metadata
    df_pd = df_pd.with_columns([
        pl.lit(df["operationContext_materialNumber"][i]).alias("Slurry"),
        pl.lit(df["params_3_pVO"][i]).alias("Active_ID"),
        pl.lit(df["params_4_pVO"][i]).alias("Binder_ID"),
        pl.lit(df["params_5_pVO"][i]).alias("Conductive_ID"),
        pl.lit(df["params_6_pVO"][i]).alias("Additive_ID"),
        pl.lit(df["params_8_pVO"][i]).alias("Liquid_Binder_ID"),
        pl.lit(df["params_10_pVO"][i]).alias("Active_weight"),
        pl.lit(df["params_11_pVO"][i]).alias("Binder_weight"),
        pl.lit(df["params_12_pVO"][i]).alias("Conductive_weight"),
        pl.lit(df["params_13_pVO"][i]).alias("Additive_weight"),
        pl.lit(df["params_14_pVO"][i]).alias("Solvent_weight"),
        pl.lit(df["params_15_pVO"][i]).alias("Liquid_Binder_weight"),
        pl.lit(df["params_17_pVO"][i]).alias("Mix_weight"),
        pl.lit(str(df["params_18_pVO"][i])).alias("MES Lot Number"),
    ])

    # Assign Mix_ID from commissioning tracker or NA
    mes_lot_number = df_pd['MES Lot Number'][0]
    xl_filter = xl_mix.filter(pl.col('LN/ MES LOT Number') == mes_lot_number)
    if xl_filter.height > 0:
        df_pd = df_pd.with_columns(pl.lit(xl_filter['Mix ID 1 # '][0]).alias("Mix_ID"))
    else:
        df_pd = df_pd.with_columns(pl.lit('NA' + str(i)).alias("Mix_ID"))

    # Assign step numbers
    list_of_steps = df_pd.filter(pl.col('pNID') == 'Step Number')
    step_times = list(reversed(list_of_steps['values_ts']))[:-1]
    step_values = list(reversed(list_of_steps['values_pOV']))[:-1]

    if len(step_values) > 0:
        df_pd = df_pd.with_columns(
            df_pd["values_ts"].map_elements(lambda x: step_values[max(0, bisect_left(step_times, x) - 1)]).alias("step")
        )
    else:
        df_pd = df_pd.with_columns(pl.lit(float(0)).alias("step"))

    # Concatenate
    full_mixing_data = pl.concat([full_mixing_data, df_pd])
    print(f"Processed row {i}")

# -------------------------
# Write to CSV
# -------------------------
full_mixing_data.write_csv('AmixingAuto_2025.csv')
print("Done! CSV written with 2025+ data.")
# ...existing code...
# Save results to Desktop
desktop_path = r"C:\Users\Nivedidha.Kumaravelu\OneDrive - UKBIC LTD\Desktop\mixingAuto8.csv"
full_mixing_data.write_csv(desktop_path)
print(f"Saved mixing CSV to: {desktop_path}")
# ...existing
