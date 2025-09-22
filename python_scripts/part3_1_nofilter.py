import part2_dataFilter
import part1_duplicateAnalysis
import pandas as pd

V107_sup_df = part2_dataFilter.V107_sup_df
V142_fece_df = part2_dataFilter.V142_fece_df
E975_br_df = part2_dataFilter.E975_br_df
metadata_set = part1_duplicateAnalysis.metadata_sets

# Reseting index
V107_sup_df = V107_sup_df.reset_index()
V142_fece_df = V142_fece_df.reset_index()
E975_br_df = E975_br_df.reset_index()

# Creating dictionary style dataset
flowcell_df = {
    "V107": V107_sup_df,
    "V142": V142_fece_df,
    "E975": E975_br_df
}

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

# Applying level simplification
level_filtered_df = {}

for flowcell_id, data in flowcell_df.items():
    
    level_filtered_df[flowcell_id] = level_filtering("phylum", data)

    
# Grouping based on weeks
samples_by_week = {}
flowcell_ids = ["V107", "V142", "E975"]

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


# Storing them
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