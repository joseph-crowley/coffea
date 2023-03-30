import matplotlib.pyplot as plt
from coffea import hist
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def save_csv(histo, dataset_name, flavour, working_point, btag_algo):
    # Convert the coffea histograms to numpy arrays
    histo_np = histo.values()[()]

    # calculate the bin edges for eta 
    histo_np = np.column_stack((histo.axes()[0].edges()[:-1], histo_np))

    # calculate the bin edges for pt
    histo_pt_vals = histo.axes()[1].edges()[:-1]

    # Convert the numpy arrays to pandas DataFrames
    histo_df = pd.DataFrame(histo_np)

    # name columns 1-101 with the pt bin edges
    histo_df.columns = np.append('eta', histo_pt_vals)

    # Save DataFrames as CSV files
    histo_csv_filename = f"csv/{dataset_name}_{btag_algo}_pt_eta_{flavour}_btag_{working_point}.csv"

    # make the eta and pt values the index
    histo_df.set_index('eta', inplace=True)
    
    # Save the CSV files
    histo_df.to_csv(histo_csv_filename)

def plot_from_csv(csv_filename):
    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv('csv/'+csv_filename)

    # get the eta values from the first column
    eta_vals = df.iloc[:,0].values.astype(np.float64)

    # get the pt bin edges from the first row
    pt_vals = df.iloc[0,1:].values.astype(np.float64)

    # make a 2D numpy array of the counts 
    counts = df.iloc[1:,1:].values.astype(np.float64)
    
    # make a coffea histogram from the numpy arrays
    histo = hist.Hist(
                'Events',
                hist.Bin('eta', 'eta', 50, -2.5, 2.5),
                hist.Bin('pt', 'pT', 100, 0, 300),
                )
    
    # fill the histogram with the counts
    for i in range(len(eta_vals) - 1):
        for j in range(len(pt_vals)):
            histo.fill(eta=eta_vals[i], pt=pt_vals[j], weight=counts[i,j])

    # set up the plot
    fig, ax = plt.subplots()
    hist.plot2d(histo, xaxis='eta', xoverflow='all', ax=ax)
    ax.set_title(csv_filename[:-4])
    
    # Save the plot as an image file
    output_dir = 'plots'    
    plt.savefig(os.path.join(output_dir, csv_filename[:-4]+'_test.png'), dpi=300)
    plt.close(fig)


def plot_hist(histo, dataset_name, flavour, working_point, btag_algo, zlabel):
    filename = f"{dataset_name}_{btag_algo}_pt_eta_{flavour}_btag_{working_point}.png"
    title = f"{dataset_name} {btag_algo} {flavour} {working_point}"
    fig, ax = plt.subplots()
    hist.plot2d(histo, xaxis='pt', xoverflow='all', ax=ax)

    ax.set_xlabel('p_{T} [GeV]')
    ax.set_ylabel('#eta')
    ax.set_title(title)
    ax.collections[0].colorbar.set_label(zlabel)

    output_dir = 'plots'
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(os.path.join(output_dir, filename), dpi=300)
    plt.close(fig)

def hist_ratio(histo1, histo2, dataset_name, flavour, working_point, btag_algo):
    # Calculate the ratio
    ratio = histo1.values()[()] / histo2.values()[()]

    # Replace NaN and inf values with zeros
    ratio = np.nan_to_num(ratio, nan=0.0, posinf=0.0, neginf=0.0)

    # Plot the ratio using matplotlib
    filename = f"{dataset_name}_{btag_algo}_pt_eta_{flavour}_btag_{working_point}_ratio.png"
    title = f"{dataset_name} {btag_algo} {flavour} {working_point}"

    fig, ax = plt.subplots()
    im = ax.imshow(ratio.T, origin='lower', aspect='auto',
                   extent=[histo1.axis('pt').edges()[0], histo1.axis('pt').edges()[-1],
                           histo1.axis('eta').edges()[0], histo1.axis('eta').edges()[-1]])
    cbar = fig.colorbar(im, ax=ax)
    cbar.ax.set_ylabel('Efficiency')
    ax.set_xlabel('pt')
    ax.set_ylabel('eta')
    ax.set_title(title)

    output_dir = 'plots'
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(os.path.join(output_dir, filename), dpi=300)
    plt.close(fig)

    # return the ratio as a coffea histogram
    ratio_hist = hist.Hist(
                'Efficiency',   
                hist.Bin('eta', 'eta', 50, -2.5, 2.5),
                hist.Bin('pt', 'pT', 100, 0, 300),
                )
    
    # fill the histogram with the ratio
    for i in range(len(ratio)):
        for j in range(len(ratio[i])):
            ratio_hist.fill(eta=histo1.axes('eta')[0].edges()[i], pt=histo1.axes('pt')[0].edges()[j], weight=ratio[i,j])
    
    return ratio_hist
