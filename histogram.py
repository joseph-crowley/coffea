import matplotlib.pyplot as plt
from coffea import hist
import os

def save_csv(deepflav_hist, deepcsv_hist, dataset_name, flavour, working_point):
    # Convert the coffea histograms to numpy arrays
    deepflav_np = deepflav_hist.values()[()]
    deepcsv_np = deepcsv_hist.values()[()]

    # calculate the bin edges for eta 
    deepflav_np = np.column_stack((deepflav_hist.axes()[0].edges()[:-1], deepflav_np))
    deepcsv_np = np.column_stack((deepcsv_hist.axes()[0].edges()[:-1], deepcsv_np))

    # calculate the bin edges for pt
    deepflav_pt_vals = deepflav_hist.axes()[1].edges()[:-1]
    deepcsv_pt_vals = deepcsv_hist.axes()[1].edges()[:-1]

    # Convert the numpy arrays to pandas DataFrames
    deepflav_df = pd.DataFrame(deepflav_np)
    deepcsv_df = pd.DataFrame(deepcsv_np)

    # name columns 1-101 with the pt bin edges
    deepflav_df.columns = np.append('eta', deepflav_pt_vals)
    deepcsv_df.columns = np.append('eta', deepcsv_pt_vals)

    # Save DataFrames as CSV files
    deepflav_csv_filename = f"csv/{dataset_name}_deepflav_pt_eta_{flavour}_btag_{working_point}.csv"
    deepcsv_csv_filename = f"csv/{dataset_name}_deepcsv_pt_eta_{flavour}_btag_{working_point}.csv"

    # make the eta and pt values the index
    deepflav_df.set_index('eta', inplace=True)
    deepcsv_df.set_index('eta', inplace=True)
    
    # Save the CSV files
    deepflav_df.to_csv(deepflav_csv_filename)
    deepcsv_df.to_csv(deepcsv_csv_filename)

def plot_from_csv(csv_filename):
    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv('csv/'+csv_filename)

    # get the eta values from the first column
    eta_vals = df.iloc[1:,0].values

    # get the pt bin edges from the first row
    pt_vals = df.columns[1:].values

    # make a 2D numpy array of the counts 
    counts = df.iloc[1:,1:].values
    
    # make a coffea histogram from the numpy arrays
    histo = hist.Hist(
                'Counts',
                hist.Bin('eta', 'eta', 50, -2.5, 2.5),
                hist.Bin('pt', 'pT', 100, 0, 300),
                )
    
    histo.fill(eta=eta_vals, pt=pt_vals, weight=df['counts'].values)

    # set up the plot
    fig, ax = plt.subplots()
    hist.plot2d(histo, xaxis='eta', xoverflow='all', ax=ax)
    ax.set_title(csv_filename[:-4])
    
    # Save the plot as an image file
    output_dir = 'plots'    
    plt.savefig(os.path.join(output_dir, csv_filename[:-4]+'_test.png'), dpi=300)
    plt.close(fig)


def plot_hist(histo, output_dir, filename, title):
    fig, ax = plt.subplots()
    hist.plot2d(histo, xaxis='eta', xoverflow='all', ax=ax)
    ax.set_title(title)

    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(os.path.join(output_dir, filename), dpi=300)
    plt.close(fig)
