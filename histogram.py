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
    hist.plot2d(histo, xaxis='eta', xoverflow='all', ax=ax)

    ax.set_xlabel('#eta')
    ax.set_ylabel('p_{T} [GeV]')
    ax.set_title(title)
    ax.collections[0].colorbar.set_label(zlabel)

    # calculate the min and max for the colorbar
    zmin = histo.values()[()].min()
    zmax = histo.values()[()].max()
    ax.collections[0].set_clim(zmin, 10*zmax)

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
    ax.set_xlabel("#eta")
    ax.set_ylabel("p_{T} (GeV/c)")
    ax.set_title(title)
    fig.colorbar(c, ax=ax, label=ztitle)

    # to set the colorbar range
    zmin = ratio.min()
    zmax = ratio.max()
    zrange = zmax - zmin

    # set the colorbar range to be 10% larger than the min and max values
    plot_min = max([zmin - 0.1*zrange,0.01])
    plot_max = min([zmax + 0.1*zrange,1])

    c.set_clim(plot_min, plot_max)

    # Save the plot to a file
    output_dir = 'plots'
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(os.path.join(output_dir, filename), dpi=300)

    # save the ratio histogram as TH2F using uproot
    nbins_x = len(x_centers)
    nbins_y = len(y_centers)
    th2f_name = f"{dataset_name}_{btag_algo}_pt_eta_{flavour}_btag_{working_point}_{ztitle.replace(' ', '').lower()}"
    th2f_title = title
    th2f = TH2F(th2f_name, th2f_title, nbins_x, data1[1][0], data1[1][-1], nbins_y, data1[2][0], data1[2][-1])

    for ix in range(nbins_x):
        for iy in range(nbins_y):
            th2f.SetBinContent(ix + 1, iy + 1, ratio[ix, iy])

    # get a ztitle with only alphanumeric characters
    ztitle_alphanumeric = ztitle.lower()
    for c in ztitle_alphanumeric:
        if not c.isalnum():
            ztitle_alphanumeric = ztitle_alphanumeric.replace(c, '')

    root_filename = f'root/{ztitle_alphanumeric}histograms_{dataset_name}_{working_point}.root'
    os.makedirs(os.path.dirname(root_filename), exist_ok=True)

    with uproot.recreate(root_filename) as f:
        f[th2f_name] = th2f
        
    plt.close(fig)