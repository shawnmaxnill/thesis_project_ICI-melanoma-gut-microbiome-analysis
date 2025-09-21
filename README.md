# thesis_project_ICI-melanoma-gut-microbiome-analysis
Repo for data cleaning and handling along with diversity metrics calculation for raw OTU data

## Description

This repository shows how data handling and cleaning was performed for raw OTU data from culture samples retrieved from fecal sample of patients undergoing ICI treatment for melanoma.

## How does it run

Data was retrieved from the output of https://github.com/ctmrbio/stag-mwc.

## Workflow
Below is how the entire workflow should run:
1. Scripts folder contains the pipeline of data handling with number denoting which step should be ran in order.
  - The `data_handling` folder in the `notebook folder` provides explanation in the form of Jupyter Notebooks
  - `run_pipeline.ipynb` in `notebook folder` shows how the scripts are connected together and executed.  
2. `run_pipeline.iypnb` contains code for main data cleaning, configured based on `config.yaml` selection and graph generation across 3 samples of V107, V142 and E975
3. `metadata_handling.ipynb` contains code for generating significant taxa GM plots across time points and the creation of a few important dataset for diversity metrics calculation
4. `alpha_diversity.ipynb` contains code for alpha diversity metrics and plot generation.
5. `beta_diversity.ipynb` contains code for data preparation for beta diversity as it is to be done in R.
6. `beta_analysis_statisticalTest.Rmd` contains code for beta diversity metrics and its plots.
7. `differential_testing.Rmd` contains code for differential abundance calculations and plot generation.

## Generated files
- `data` folder contains all the data used for this analysis.
- `figures_python` contains all the plots generated for the paper.
- `maaslin2` outputs MaAslin2 objects from differential analysis.
- `data` folder are all the data used in R for analysis.
- `figures` R contains all the figures generated in R for the analysis.
