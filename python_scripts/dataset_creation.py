import pandas as pd

def run_metadata_generation():
    # Imports
    metadata_V107_w_inh = pd.read_excel("data/rawdata/CC_longitudinal_data_standardized_v220250603.xlsx", sheet_name = "CC_longitudinal_metadata_w_inh")
    metadata_E975_w_inh = pd.read_excel("data/rawdata/E100051975_BR_longitudinal_metadata_dev1_v220250603.xlsx", sheet_name = "BR_longitudinal_metadata_w_inh")

    # Target columns
    extract = ["sample_id", "pt_identifier". "response (R = responder, NR =nonresponder; inside parenthesis is previous classification)"]

    # Extracting
    meta_V107 = metadata_V107_w_inh[extract]
    meta_E975 = metadata_E975_w_inh[extract]

    # Data cleaning
    target_column = "response (R = responder, NR =nonresponder; inside parenthesis is previous classification)"

    meta_V107.loc[:,target_column] = meta_V107.loc[:,target_column].str.replace(r"\s.*|\(.*", "", regex=True)
    meta_E975.loc[:,target_column] = meta_E975.loc[:,target_column].str.replace(r"\s.*|\(.*", "", regex=True)

    # Grouping based on response
    V107_res = metadata_V107_inh.groupby(target_column)
    E975_res = metadataE975_inh.groupby(target_column)

    V107_res_df = {}
    E975_res_df = {}

    for res, df in V107_res:
        V107_res_df[res] = df

    for res, df in E975_res:
        E975_res_df[res] = df
        
    return [V107_res_df, E975_res_df]
    
    
# Handling raw data imports
def raw_data_handling(raw_df, V107_res_df, E975_res_df):
    
    data_V107 = raw_df["V107"].set_index("taxon_name")
    data_E975 = raw_df["E975"].set_index("taxon_name")
    
    
    V107_df = {}

    for res, df in V107_res_df.items():

        extracted_df = data_V107.loc[:, data_V107.columns.isin(df["sample_id"])]

        V107_df[res] = extracted_df
    

    E975_df = {}

    for res, df in E975_res_df.items():

        extracted_df = data_E975.loc[:, data_E975.columns.isin(df["sample_id"])]

        E975_df[res] = extracted_df
        
        
    return [V107_df, E975_df]


# Week separation

def run_week_separation(V107_df, E975_df):

    metadata_V107_w_inh = pd.read_excel("data/rawdata/CC_longitudinal_data_standardized_v220250603.xlsx", sheet_name = "CC_longitudinal_metadata_w_inh")
    metadata_E975_w_inh = pd.read_excel("data/rawdata/E100051975_BR_longitudinal_metadata_dev1_v220250603.xlsx", sheet_name = "BR_longitudinal_metadata_w_inh")

    V107_week_info = metadata_V107_w_inh[["sample_id", "sample_description"]]
    E975_week_info = metadata_E975_w_inh[["sample_id", "sample_description"]]

    # Extracting based on weeks (first and last week/experiment period)
    V107_wk0 = V107_week_info.loc[V107_week_info["sample_description"] == "fece_wk0_a"]
    V107_wk1 = V107_week_info.loc[V107_week_info["sample_description"] == "cul_wk1_a"]
    V107_wk2 = V107_week_info.loc[V107_week_info["sample_description"] == "cul_wk2_a"]
    V107_wk3 = V107_week_info.loc[V107_week_info["sample_description"] == "cul_wk3_a"]
    V107_wk4 = V107_week_info.loc[V107_week_info["sample_description"] == "cul_wk4_a"]
    V107_wk5 = V107_week_info.loc[V107_week_info["sample_description"] == "cul_wk5_a"]
    V107_wk6 = V107_week_info.loc[V107_week_info["sample_description"] == "cul_wk6_a"]

    E975_fs = E975_week_info.loc[E975_week_info["sample_description"] == "fecal_slurry"]
    E975_day4 = E975_week_info.loc[E975_week_info["sample_description"] == "cul_Day4"]
    E975_day8 = E975_week_info.loc[E975_week_info["sample_description"] == "cul_Day8"]
    E975_day12 = E975_week_info.loc[E975_week_info["sample_description"] == "cul_Day12"]

    def extract_weekly(main_df, week_samples_df):

        result = {}

        for res, df in main_df.items():

            # Extracting samples that only exists in the particular week
            extracted = df.loc[:, df.columns.isin(week_samples_df["sample_id"])]

            # Copying, avoid inplace modification
            extracted_cpy = extracted.copy()

            # Summing all samples up
            extracted_cpy["count"] = extracted_cpy.sum(axis=1)

            # Dropping empty rows
            extracted_cpy = extracted_cpy[extracted_cpy["count"] != 0]

            # Store
            result[res] = extracted_cpy[["count"]]

        return result

    V107_week0_res = extract_weekly(V107_df, V107_wk0)
    V107_week1_res = extract_weekly(V107_df, V107_wk1)
    V107_week2_res = extract_weekly(V107_df, V107_wk2)
    V107_week3_res = extract_weekly(V107_df, V107_wk3)
    V107_week4_res = extract_weekly(V107_df, V107_wk4)
    V107_week5_res = extract_weekly(V107_df, V107_wk5)
    V107_week6_res = extract_weekly(V107_df, V107_wk6)

    E975_day0_res = extract_weekly(E975_df, E975_fs)
    E975_day4_res = extract_weekly(E975_df, E975_day4)
    E975_day8_res = extract_weekly(E975_df, E975_day8)
    E975_day12_res = extract_weekly(E975_df, E975_day12)
    
    # Creating time series data

    def create_time_series(*df):

        df_R = []
        df_NR = []

        for data in df:

            for res, dataset in data.items():

                if res == "R":

                    df_R.append(dataset)

                else:

                    df_NR.append(dataset)

        return [df_R, df_NR]



    V107_R, V107_NR = create_time_series(V107_week0_res, V107_week1_res, V107_week2_res, V107_week3_res, V107_week4_res, V107_week5_res, V107_week6_res)
    E975_R, E975_NR = create_time_series(E975_day0_res, E975_day4_res, E975_day8_res, E975_day12_res)
    
    # Renaming columns
    for i, df in enumerate(V107_R):

        df.reset_index(inplace = True)
        df.rename(columns = {"count": f"week{i}"}, inplace=True)

    for i, df in enumerate(V107_NR):

        df.reset_index(inplace = True)
        df.rename(columns = {"count": f"week{i}"}, inplace=True)

    days = ["0", "4", "8", "12"]

    for i in range(len(E975_R)):

        E975_R[i].reset_index(inplace = True)
        E975_R[i].rename(columns = {"count": f"day{days[i]}"}, inplace = True)

    for i in range(len(E975_NR)):

        E975_NR[i].reset_index(inplace = True)
        E975_NR[i].rename(columns = {"count": f"day{days[i]}"}, inplace = True)
        
        
    # Merging them
    def merger(datasets):

        merged_df = datasets[0]

        for df in datasets[1:]:
            merged_df = pd.merge(merged_df, df, on = "taxon_name", how = "outer")

        merged_df = merged_df.fillna(0)
        merged_df.set_index("taxon_name", inplace = True)

        return merged_df


    ts_V107_R = merger(V107_R)
    ts_V107_NR = merger(V107_NR)
    ts_E975_R = merger(E975_R)
    ts_E975_NR = merger(E975_NR)
    
    
    # Creating a phylum column
    def level_filtering(df, filtering):

        # Resetting index
        df = df.reset_index(names = "taxon_name")

        # Level Map
        level_map = {
            "kingdom": "k__",
            "phylum": "p__",
            "class": "c__",
            "order": "o__",
            "family": "f__",
            "genus": "g__",
            "species": "s__"
            }

        level = level_map.get(filtering.lower())

        # Perform filtering based on passed in parameter mapped to Level Map and name the column
        phylum_new_column = df["taxon_name"].str.extract(fr'{level}([^|]+)')

        # Creating another column, genus
        genus_new_column = df["taxon_name"].str.extract(r"g__([^|]+)")[0]

        # Inserting new column
        df.insert(0, filtering, phylum_new_column)

        # Inserting genus column
        df.insert(1, "genus", genus_new_column)

        # Dropping NaN columns
        filtered_df = df.dropna(subset=["taxon_name"])

        return df
    
    
    