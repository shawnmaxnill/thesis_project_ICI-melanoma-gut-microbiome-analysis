#import part2_dataFilter
#import part1_duplicateAnalysis
 
from pathlib import Path
import pandas as pd 

# Loading configs
def run_microbiome_filtering(dataset, config):
    
    # Loading in configs
    taxa_config = config["taxa_class"]
    prevalence_threshold = config["prevalence_threshold"]
    abundance_proportion = config["abundance_proportion"]
    
    # Filtering function
    def filtering(df, taxa_class = taxa_config, threshold = prevalence_threshold, proportion = abundance_proportion):

        # Taxonomic Filtering
        # Setting pattern
        pattern = f"/|{taxa_class}_"

        # Filter to species level by default
        df = df[df["taxon_name"].str.contains(pattern)]


        # Prevalence Filtering
        # Getting total taxa across sample
        prevalence = (df.iloc[:,1:] > 0).sum(axis = 1)

        # Retrieving total samples
        total_samples = df.iloc[:,1:].shape[1]

        # Setting filter threshold at 25%
        filter_threshold = threshold * total_samples

        # Filter
        df = df[prevalence >= filter_threshold]


        # Low Abundance Filtering
        # Retrieving taxa-wise total abundance
        rowwise_abd = df.iloc[:, 1:].sum(axis=1)

        # Getting lowest 10%
        threshold_abd = rowwise_abd.quantile(proportion)

        # Filtering it
        cleaned_data = df[rowwise_abd >= threshold_abd]

        return cleaned_data

    # Applying filter
    flowcell_ids = ["V107", "V142", "E975"]
    flowcell_df = {}

    for i in range(len(flowcell_ids)):

        dataset[i] = dataset[i].reset_index()
        flowcell_df[flowcell_ids[i]] = filtering(dataset[i])
        
    return flowcell_df

def standard_filter(dataset, config):
    
    # Loading in configs
    taxa_config = config["taxa_class"]
    prevalence_threshold = config["prevalence_threshold"]
    abundance_proportion = config["abundance_proportion"]

    def filtering(df, taxa_class = taxa_config, threshold = prevalence_threshold, proportion = abundance_proportion):

        # Taxonomic Filtering
        # Setting pattern
        pattern = f"/|{taxa_class}_"

        # Filter to species level by default
        df = df[df["taxon_name"].str.contains(pattern)]


        # Prevalence Filtering
        # Getting total taxa across sample
        prevalence = (df.iloc[:,1:] > 0).sum(axis = 1)

        # Retrieving total samples
        total_samples = df.iloc[:,1:].shape[1]

        # Setting filter threshold at 25%
        filter_threshold = threshold * total_samples

        # Filter
        df = df[prevalence >= filter_threshold]


        # Low Abundance Filtering
        # Retrieving taxa-wise total abundance
        rowwise_abd = df.iloc[:, 1:].sum(axis=1)

        # Getting lowest 10%
        threshold_abd = rowwise_abd.quantile(proportion)

        # Filtering it
        cleaned_data = df[rowwise_abd >= threshold_abd]

        return cleaned_data
    
    return cleaned_data