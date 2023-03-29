import uproot
import os
import glob
from coffea import processor
from config import BTAG_WP, hadron_flavours, branches
from histogram import save_csv, plot_from_csv, plot_hist
from processor import MyProcessor, MySchema
from file_utils import dataframe_to_csv, csv_to_dataframe

def process_dataset(dataset_name, file_list):
    single_fileset = {dataset_name: file_list}
    result = processor.run_uproot_job(
        single_fileset,
        treename="SkimTree",
        processor_instance=MyProcessor(),
        executor=processor.futures_executor,
        executor_args={'schema': MySchema, 'workers': 4}, # Adjust the number of workers as needed
        chunksize=5000,
        maxchunks=None,
    )

    # Create a ROOT file to save the histograms
    working_point = ["NONE",'Loose', "Medium","Tight"][BTAG_WP] 
    # Loop over each jet flavour
    for flavour in hadron_flavours.values():
        deepflav_hist = result['deepflav_btag_pt_eta'].integrate('flavour', flavour)
        deepcsv_hist = result['deepcsv_btag_pt_eta'].integrate('flavour', flavour)

        # Save the histograms as CSV files
        save_csv(deepflav_hist, deepcsv_hist, dataset_name, flavour, working_point)
        
        # Plot the histograms
        plot_hist(deepflav_hist, dataset_name, flavour, working_point, 'deepflavour')
        plot_hist(deepcsv_hist, dataset_name, flavour, working_point, 'deepcsv')

    return result

if __name__ == '__main__':
    fileset = {
        'tt_2l2nu': ['/ceph/cms/store/group/tttt/Worker/usarica/output/SimJetEffs/230309/2018/TT_2l2nu_141_of_291.root']
    }

    for dataset_name, file_list in fileset.items():
        print(f"Processing dataset: {dataset_name}")
        process_dataset(dataset_name, file_list)

    for csvfile in glob.glob('csv/*.csv'):
        plot_from_csv(csvfile.replace('csv/', ''))
