import pandas as pd
import numpy as np

def run_metadata_handling():
    
    # Importing data
    metadata_V107_w_inh = pd.read_excel("../data/rawdata/CC_longitudinal_data_standardized_v220250603.xlsx", sheet_name = "CC_longitudinal_metadata_w_inh")
    metadata_E975_w_inh = pd.read_excel("../data/rawdata/E100051975_BR_longitudinal_metadata_dev1_v220250603.xlsx", sheet_name = "BR_longitudinal_metadata_w_inh")
    
    # Extracting required columns
    columns_to_extract = ["sample_id", 
                      "pt_identifier",
                      "diagnosis", 
                      "lines_of_therapy",
                      "response (R = responder, NR =nonresponder; inside parenthesis is previous classification)",
                      "best_response (PD=progressive disease, SD=stable, PR=partial response, CR=complete response, MR=mixed response)",
                      "PFS_since_1stICI (progression free survival in months)",
                      "OS_since_1stICI (overall survival in months)"]

    metadata_V107_inh = metadata_V107_w_inh[columns_to_extract]
    metadataE975_inh = metadata_E975_w_inh[columns_to_extract]
    
    
    # Column cleaning
    target_column = "response (R = responder, NR =nonresponder; inside parenthesis is previous classification)"

    metadata_V107_inh.loc[:,target_column] = metadata_V107_inh.loc[:,target_column].str.replace(r"\s.*|\(.*", "", regex=True)
    metadataE975_inh.loc[:,target_column] = metadataE975_inh.loc[:,target_column].str.replace(r"\s.*|\(.*", "", regex=True)
    
    
    # Grouping based on response
    V107_res = metadata_V107_inh.groupby(target_column)
    E975_res = metadataE975_inh.groupby(target_column)
    
    V107_res_df = {}
    E975_res_df = {}

    for res, df in V107_res:
        V107_res_df[res] = df

    for res, df in V107_res:
        E975_res_df[res] = df
        
        
    return V107_res_df, E975_res_df
    
    
    
    
    
    
    
    
    
    
    
    