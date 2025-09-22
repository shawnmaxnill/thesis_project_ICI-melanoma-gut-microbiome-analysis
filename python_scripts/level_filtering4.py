import yaml
import pandas as pd 


def run_level_filtering(df, config):
    
    level_filter = config["level_filter"]
    
    # Variable handling
    flowcell_df = df
    flowcell_ids = ["V107", "V142", "E975"]


    # Importing data
    import_str = "/ceph/projects/179_Oncdon/shawn.loo/workspace/"
    
    metadata_V107 = pd.read_excel(f"{import_str}data/rawdata/CC_longitudinal_data_standardized_v220250603.xlsx", sheet_name = "CC_longitudinal_metadata")
    metadata_V142 = pd.read_excel(f"{import_str}data/rawdata/V350218142_batchC_CC_longitudinal_metadata_dev1_v220250603.xlsx")
    metadata_E975 = pd.read_excel(f"{import_str}data/rawdata/E100051975_BR_longitudinal_metadata_dev1_v220250603.xlsx", sheet_name = "BR_longitudinal_metadata")
    
    metadata_set = [metadata_V107, metadata_V142, metadata_E975]
    
    def level_filtering(filtering, df):

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
        df["taxon_name"] = df["taxon_name"].str.extract(fr'{level}([^|]+)')

        # Dropping NaN columns
        filtered_df = df.dropna(subset=["taxon_name"])

        # Group up the rows
        result = filtered_df.groupby("taxon_name").sum()

        return result

    # Applying filter
    level_filtered_df = {}

    for flowcell_id, data in flowcell_df.items():

        level_filtered_df[flowcell_id] = level_filtering(level_filter, data)

    # Grouping data based on weeks --------------------------------------------------------
    samples_by_week = {}

    for idx in range(len(metadata_set)):

        # Getting rows in metadata based on the sample_names in our current data
        filtered_df = metadata_set[idx][metadata_set[idx]["sample_id"].isin(level_filtered_df[flowcell_ids[idx]].columns)]

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


    main_dataset = {}

    for flowcell, weeks in samples_by_week.items():

        main_dataset[flowcell] = {}

        for week, sample_id in weeks.items():

            main_dataset[flowcell][week] = level_filtered_df[flowcell][sample_id]

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
    
    return main_dataset