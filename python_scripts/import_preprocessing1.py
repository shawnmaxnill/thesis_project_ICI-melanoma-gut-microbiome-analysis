# Imports
import pandas as pd
import numpy as np
import copy

# Call this function to run the script
def load_and_process_data():
    
    import_str = "/ceph/projects/179_Oncdon/shawn.loo/workspace/"
    
    # Main raw data import
    df = pd.read_table(f"{import_str}data/rawdata/all_samples.metaphlan.txt")

    # Metadata import
    # Importing metadata
    metadata_V107_w_inh = pd.read_excel(f"{import_str}data/rawdata/CC_longitudinal_data_standardized_v220250603.xlsx", sheet_name = "CC_longitudinal_metadata_w_inh")
    metadata_V107 = pd.read_excel(f"{import_str}data/rawdata/CC_longitudinal_data_standardized_v220250603.xlsx", sheet_name = "CC_longitudinal_metadata")
    metadata_V142 = pd.read_excel(f"{import_str}data/rawdata/V350218142_batchC_CC_longitudinal_metadata_dev1_v220250603.xlsx")
    metadata_E975_w_inh = pd.read_excel(f"{import_str}data/rawdata/E100051975_BR_longitudinal_metadata_dev1_v220250603.xlsx", sheet_name = "BR_longitudinal_metadata_w_inh")
    metadata_E975 = pd.read_excel(f"{import_str}data/rawdata/E100051975_BR_longitudinal_metadata_dev1_v220250603.xlsx", sheet_name = "BR_longitudinal_metadata")

    # Storing as list
    metadata_sets_inh = [metadata_V107_w_inh, metadata_E975_w_inh]
    metadata_sets = [metadata_V107, metadata_V142, metadata_E975]

    # Storing and sorting all_samples into unique flowcell ids
    dataset = {}

    flowcells = ["V350218107", "V350218142", "E100051975"]

    for i in flowcells:
        dataset[i] = pd.concat([df["taxon_name"], df.loc[:, df.columns.str.contains(i)]], axis=1)


    # Storing each duplicates in samples with separate variables

    # For sample V107
    meta_V107_a = metadata_V107[metadata_V107["sample_description"].str.endswith("a")]["sample_id"]
    meta_V107_b = metadata_V107[metadata_V107["sample_description"].str.endswith("b")]["sample_id"]

    # For samaple V142
    meta_V142_a = metadata_V142[metadata_V142["sample_description"].str.endswith("a")]["sample_id"]
    meta_V142_b = metadata_V142[metadata_V142["sample_description"].str.endswith("b")]["sample_id"]

    # Sample E975 have no duplicates
    meta_E975 = metadata_E975["sample_id"]


    # Function to retrieve data from all_samples based on metadata samples names
    def filterOut_nonMetadata(df, metadata_samples):

        # Setting up regex filter
        metadata_samples = '|'.join(metadata_samples)

        # Setting up boolean value to filter out from main dataset (masking)
        boolean_mask = pd.Series(df.columns).str.contains(metadata_samples, regex = True)

        # Applying filter
        filtered_columns = pd.Series(df.columns)[boolean_mask]

        # Return filtered dataset
        return df[filtered_columns].copy()


    # Applying function and building a new dataset just for duplicates
    # Building dataset by just concatenating different columns

    # For V107
    V107_dup_A_df = pd.concat([dataset["V350218107"]["taxon_name"], filterOut_nonMetadata(dataset["V350218107"], meta_V107_a)], axis = 1)
    V107_dup_B_df = pd.concat([dataset["V350218107"]["taxon_name"], filterOut_nonMetadata(dataset["V350218107"], meta_V107_b)], axis = 1)

    # For V142
    V142_dup_A_df = pd.concat([dataset["V350218142"]["taxon_name"], filterOut_nonMetadata(dataset["V350218142"], meta_V142_a)], axis = 1)
    V142_dup_B_df = pd.concat([dataset["V350218142"]["taxon_name"], filterOut_nonMetadata(dataset["V350218142"], meta_V142_b)], axis = 1)

    # For E975
    E975_df = pd.concat([dataset["E100051975"]["taxon_name"], filterOut_nonMetadata(dataset["E100051975"], meta_E975)], axis = 1)

    duplicated_datasets_lst = [V107_dup_A_df, V107_dup_B_df, V142_dup_A_df, V142_dup_B_df, E975_df]


    # Setting taxon_name as index
    for i in range(len(duplicated_datasets_lst)):

        duplicated_datasets_lst[i] = duplicated_datasets_lst[i].set_index("taxon_name")

    # Shortening sample name and removing duplicate indicator eg: a and b (the last word in a string)
    for df in duplicated_datasets_lst:

        # Split by underscores and retrieve unique id
        df.columns = [sample_names.split('_')[3] for sample_names in list(df.columns)]

    # Handling special cases, renaming samples
    same_col_df = duplicated_datasets_lst[2]

    # Find indexes of duplicate columns
    col_indexes = [i for i, col in enumerate(same_col_df.columns) if col == "179fece00032a"]

    # Rename them manually using their index
    same_col_df.columns.values[col_indexes[0]] = "179fece00032a_1"
    same_col_df.columns.values[col_indexes[1]] = "179fece00032a_2"

    main_dataset = copy.deepcopy(duplicated_datasets_lst)

    # Handling FF samples and removing last character
    for i in range(len(duplicated_datasets_lst) - 1):

        new_columns = []

        for colnames in duplicated_datasets_lst[i].columns:

            # Case handling
            if colnames.startswith("FF"):
                new_columns.append(colnames)

            elif colnames.startswith("179fece00032a_"):
                new_columns.append(colnames)

            else:
                new_columns.append(colnames[:-1])  # remove last character

        duplicated_datasets_lst[i].columns = new_columns


    # Storing E975 from downstream processing
    df_E975 = duplicated_datasets_lst.pop()


    # Sorting columns for uniform vectorized operation
    duplicated_datasets_lst = [df.sort_index(axis = 1) for df in duplicated_datasets_lst]

    # Case Handling for FF samples (the last 6 columns in the first 2 datasets)
    FF_samples = [duplicated_datasets_lst[i].iloc[:,-6:] for i in range(2)]

    # Dropping them from sorted dataset and grouping them together based on flow cell ID
    sup_df = []
    fece_df = []

    for i in range(4):

        # first two samples (107)
        if i <= 1:
            duplicated_datasets_lst[i] = duplicated_datasets_lst[i].drop(columns = FF_samples[i].columns) # dropping FF samples
            sup_df.append(duplicated_datasets_lst[i])

        # last two samples (142)
        else:
            fece_df.append(duplicated_datasets_lst[i])
    
    return main_dataset