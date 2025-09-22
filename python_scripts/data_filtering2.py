import pandas as pd
import importlib


def filter_data(main_dataset):

    import_str = "/ceph/projects/179_Oncdon/shawn.loo/workspace/"
    
    # Importing Datasets
    metadata_V107 = pd.read_excel(f"{import_str}data/rawdata/CC_longitudinal_data_standardized_v220250603.xlsx", sheet_name = "CC_longitudinal_metadata")
    metadata_V142 = pd.read_excel(f"{import_str}data/rawdata/V350218142_batchC_CC_longitudinal_metadata_dev1_v220250603.xlsx")

    # Creating dataset including duplicates
    V107_sup_df = pd.concat([main_dataset[0], main_dataset[1][["179supB00069b", "179supB00610b"]]], axis = 1)
    V107_sup_df = V107_sup_df[sorted(V107_sup_df.columns)]

    V142_fece_df = pd.concat([main_dataset[2], main_dataset[3]["179fece00015b"]], axis = 1)
    V142_fece_df = V142_fece_df[sorted(V142_fece_df.columns)]

    E975_br_df = main_dataset[4].copy()

    # Retrieving patient id from V142
    pt_ids_142 = metadata_V142["pt_identifier"].unique()

    # Retrieving sample name corresponding to patient id

    # Removing duplicates
    metadata_V107 = metadata_V107[metadata_V107["sample_description"].str.endswith("a")]

    # Retrieving sample name
    samples_remove = metadata_V107.loc[metadata_V107["pt_identifier"].isin(pt_ids_142)]["sample_id"]

    # Dropping them
    V107_sup_df = V107_sup_df.drop(columns = samples_remove, errors = "ignore")
    
    
    return [V107_sup_df, V142_fece_df, E975_br_df]