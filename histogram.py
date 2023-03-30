import matplotlib.pyplot as plt
from coffea import hist
import os
import numpy as np
import uproot
from ROOT import TH2F

def plot_hist(histo, dataset_name, flavour, working_point, btag_algo, zlabel):
    filename = f"{dataset_name}_{btag_algo}_pt_eta_{flavour}_btag_{working_point}.png"
    title = f"{dataset_name} {btag_algo} {flavour} {working_point}"

    fig, ax = plt.subplots()
    hist.plot2d(histo, xaxis='pt', xoverflow='all', ax=ax)

    ax.set_xlabel('p_{T} [GeV]')
    ax.set_ylabel('#eta')
    ax.set_title(title)
    ax.collections[0].colorbar.set_label(zlabel)

    # calculate the min and max for the colorbar
    zmin = histo.values()[()].min()
    zmax = histo.values()[()].max()
    ax.collections[0].set_clim(zmin, zmax)

    output_dir = 'plots'
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(os.path.join(output_dir, filename), dpi=300)
    plt.close(fig)

def plot_histogram_ratio(hist1, hist2, dataset_name, flavour, working_point, btag_algo, ztitle):
    # Convert the histograms to NumPy arrays
    data1 = hist1.to_numpy()
    data2 = hist2.to_numpy()

    # Calculate the ratio of the histograms
    ratio = np.divide(data1[0], data2[0], out=np.zeros_like(data1[0]), where=data2[0] != 0)

    # Convert bin edges to bin centers for the X and Y axes
    x_centers = (data1[1][:-1] + data1[1][1:]) / 2
    y_centers = (data1[2][:-1] + data1[2][1:]) / 2

    # Create a new 2D histogram for the ratio
    filename = f"{dataset_name}_{btag_algo}_pt_eta_{flavour}_btag_{working_point}_ratio.png"
    title = f"{dataset_name} {btag_algo} {flavour} {working_point}"

    fig, ax = plt.subplots()
    c = ax.pcolormesh(x_centers, y_centers, ratio.T, cmap="viridis")
    ax.set_xlabel("Pt (GeV/c)")
    ax.set_ylabel("Eta")
    ax.set_title(title)
    fig.colorbar(c, ax=ax, label=ztitle)

    # Save the plot to a file
    plt.savefig(filename, dpi=300)

    # save the ratio histogram as TH2F using uproot
    nbins_x = len(x_centers)
    nbins_y = len(y_centers)
    th2f_name = f"{dataset_name}_{btag_algo}_pt_eta_{flavour}_btag_{working_point}_{ztitle.replace(' ', '').lower()}"
    th2f_title = title
    th2f = TH2F(th2f_name, th2f_title, nbins_x, data1[1][0], data1[1][-1], nbins_y, data1[2][0], data1[2][-1])

    for ix in range(nbins_x):
        for iy in range(nbins_y):
            th2f.SetBinContent(ix + 1, iy + 1, ratio[ix, iy])

    root_filename = "existing_file.root"
    with uproot.open(root_filename, mode="r+") as f:
        # Delete the existing TH2F with the same name, if it exists
        if th2f_name in f:
            del f[th2f_name]

        # Write the new TH2F
        f[th2f_name] = uproot.to_writable(th2f)
