import yaml
import pandas as pd 

# Function to run the script
def run_genus_level_filtering(microbiome_filtered_df):
    
    import_str = "/ceph/projects/179_Oncdon/shawn.loo/workspace/"
    # Importing data
    metadata_V107 = pd.read_excel(f"{import_str}data/rawdata/CC_longitudinal_data_standardized_v220250603.xlsx", sheet_name = "CC_longitudinal_metadata")
    metadata_V142 = pd.read_excel(f"{import_str}data/rawdata/V350218142_batchC_CC_longitudinal_metadata_dev1_v220250603.xlsx")
    metadata_E975 = pd.read_excel(f"{import_str}data/rawdata/E100051975_BR_longitudinal_metadata_dev1_v220250603.xlsx", sheet_name = "BR_longitudinal_metadata")

    metadata_set = [metadata_V107, metadata_V142, metadata_E975]
    
    
    def level_filtering(df, filtering):

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
        new_column = df["taxon_name"].str.extract(fr'{level}([^|]+)')

        # Inserting new column
        df.insert(0, filtering, new_column)

        # Dropping NaN columns
        filtered_df = df.dropna(subset=["taxon_name"])

        return filtered_df

    
    # Applying filter and handling edge cases
    temp_df = {}

    for flowcell, df in microbiome_filtered_df.items():

        # Applying filter
        temp_df[flowcell] = level_filtering(df, "phylum")

        # Creating genus column
        temp_df[flowcell]["genus"] = temp_df[flowcell]["taxon_name"].str.extract(r"g__([^|]+)")[0]

        # Dropping NA
        temp_df[flowcell] = temp_df[flowcell].dropna(subset=["genus"])

        # Grouping by genus
        grouped_genus = temp_df[flowcell].groupby("genus").sum()

        # Retrieving phylum col value for each genus
        phylum_under_genus = temp_df[flowcell].groupby("genus")["phylum"].first()

        # Add phylum back as a column
        grouped_genus["phylum"] = phylum_under_genus

        # Reset index to get taxon_name back as a column
        temp_df[flowcell] = grouped_genus.reset_index()

        # Dropping unused column
        temp_df[flowcell].drop("taxon_name", axis=1, inplace=True)
        
        
    # Grouping based on phylum
    flowcell_id = ["V107", "V142", "E975"]
    genus_df = {}

    for ids in flowcell_id:

        flowcell = {}

        phylum = temp_df[ids]["phylum"].unique()

        for phylum_name in phylum:

            flowcell[phylum_name] = temp_df[ids][temp_df[ids]["phylum"] == phylum_name].copy()

            # Dropping unused column
            flowcell[phylum_name].drop("phylum", axis=1, inplace=True)

        genus_df[ids] = flowcell
        
        
    # Handling metadata ------------------------------------------------------------
    flowcell_ids = ["V107", "V142", "E975"]

    samples_by_week = {}

    for idx in range(len(metadata_set)):

        # Getting rows in metadata based on the sample_names in our current data
        filtered_df = metadata_set[idx][metadata_set[idx]["sample_id"].isin(microbiome_filtered_df[flowcell_ids[idx]].columns)]

        # Extracting needed columns
        filtered_df = filtered_df[["sample_id", "sample_description"]]

        # Grouping them based on weeks
        filtered_df = filtered_df.groupby("sample_description")["sample_id"].apply(list).to_dict()

        # Storing
        samples_by_week[flowcell_ids[idx]] = filtered_df


    # Handling duplicates to be included into the same week
    samples_by_week["V107"]["cul_wk6_a"].extend(samples_by_week["V107"]["cul_wk6_b"])

    del samples_by_week["V107"]["cul_wk6_b"]

    # Repeat for other duplicate
    samples_by_week["V142"]["cul_wk1_a"].extend(samples_by_week["V142"]["cul_wk1_b"])
    del samples_by_week["V142"]["cul_wk1_b"]
    
    
    # Sorting data into weeks -----------------------------------------------------
    main_dataset = {}

    for flowcell, weeks in samples_by_week.items():

        main_dataset[flowcell] = {}

        for week, sample_id in weeks.items():

            main_dataset[flowcell][week] = microbiome_filtered_df[flowcell][sample_id]

    # Resorting duplicates dataset
    main_dataset["V107"]["cul_wk6_a"] = main_dataset["V107"]["cul_wk6_a"].reindex(sorted(main_dataset["V107"]["cul_wk6_a"].columns), axis=1)
    main_dataset["V142"]["cul_wk1_a"] = main_dataset["V142"]["cul_wk1_a"].reindex(sorted(main_dataset["V142"]["cul_wk1_a"].columns), axis=1)


    # Reordering them
    # For V107
    colnames = list(main_dataset["V107"].items())
    reordered = [colnames[-1]] + colnames[:-1]
    main_dataset["V107"] = dict(reordered)

    # For E975
    E975_order = ['fecal_slurry', 'cul_Day4', 'cul_Day8', 'cul_Day12']
    main_dataset["E975"] = {key: main_dataset["E975"][key] for key in E975_order}
    
    
    # Grouping genus level dataframe into weeks ------------------------------------
    genus_main_dataset = {}

    for flowcell, weeks in main_dataset.items():

        genus_main_dataset[flowcell] = {}
        genera = genus_df.get(flowcell, {})

        for week, df in weeks.items():

            genus_main_dataset[flowcell][week] = {}

            for genus, g_df in genera.items():

                # Choosing what to keep
                cols_to_keep = ["genus"] + [col for col in g_df.columns if col in df.columns]

                completed_df = g_df[cols_to_keep]
                
                # Setting taxa as index
                completed_df  = completed_df.set_index("genus")

                # Storing
                genus_main_dataset[flowcell][week][genus] = completed_df

    return genus_main_dataset
    