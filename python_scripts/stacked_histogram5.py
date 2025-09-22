# Imports
import matplotlib.pyplot as plt 

def main_plot(main_dataset_dict):

    # Plotting function
    def plot_stacked_hist(dataset):

        # Normalizing values
        normalized_data = dataset.div(dataset.sum(axis=0), axis=1)

        # Sorting the columns for consistency
        normalized_data = normalized_data[sorted(normalized_data.columns)]

        # Transpose so samples are on x-axis
        df = normalized_data.T

        # Plot a stacked histogram
        df.plot(kind = 'bar', stacked = True, figsize = (12, 6), colormap = 'tab20')

        plt.title('Phylum Level Abundance Across Samples')
        plt.xlabel('Samples')
        plt.xticks(fontsize = 7)
        plt.xticks(rotation = 45, ha = 'right')
        plt.ylabel('Phylum')
        plt.legend(bbox_to_anchor = (1.05, 1), loc = 'upper left')
        plt.tight_layout()
        plt.show()


    # Plotting all at once
    for flowcell, weeks_dict in main_dataset_dict.items():
        for week, df in weeks_dict.items():
            plot_stacked_hist(df)