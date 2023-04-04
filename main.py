import glob
from coffea import processor
from config import BTAG_WP, hadron_flavours, ROOT_OUTPUT_DIR, fileset
from histogram import plot_hist, plot_histogram_ratio
from processor import MyProcessor, MySchema
import uproot
#from concurrent.futures import ProcessPoolExecutor

def process_dataset_wrapper(args):
    return process_dataset(*args)

def process_dataset(dataset_name, file_list, result = None):
    print(f"Processing dataset: {dataset_name}")


    single_fileset = {dataset_name: file_list}
    if result is None:
        result = processor.run_uproot_job(
            single_fileset,
            treename="SkimTree",
            processor_instance=MyProcessor(output_file=ROOT_OUTPUT_FILE),
            executor=processor.futures_executor,
            executor_args={'schema': MySchema, 'workers': 24}, # Adjust the number of workers as needed
            chunksize=50000,
            maxchunks=None,
        )

    # Loop over each jet flavour
    for flavour in hadron_flavours.values():
        deepflav_hist = result['deepflav_btag_pt_eta'].integrate('flavour', flavour)
        deepcsv_hist = result['deepcsv_btag_pt_eta'].integrate('flavour', flavour)
        all_hist = result['no_btag_pt_eta'].integrate('flavour', flavour)

        # Plot the histograms
        plot_hist(deepflav_hist, dataset_name, flavour, working_point, 'deepflavour', 'Number of b Jets')
        plot_hist(deepcsv_hist, dataset_name, flavour, working_point, 'deepcsv', 'Number of b Jets')
        plot_hist(all_hist, dataset_name, flavour, working_point, 'no_btag', 'Number of b Jets')

    return result

def plot_ratio(file_path, hist1_name, hist2_name, dataset_name, flavour, working_point, btag_algo, ztitle):
    # Open the ROOT file and get the histograms
    file = uproot.open(file_path)
    hist1 = file[hist1_name + '_'+ flavour]
    hist2 = file[hist2_name + '_'+ flavour]

    plot_histogram_ratio(hist1, hist2, dataset_name, flavour, working_point, btag_algo, ztitle)

def run():
    global ROOT_OUTPUT_FILE
    global working_point
    
    working_point = ["NONE",'Loose', "Medium","Tight"][BTAG_WP] 

    # Loop over each dataset
    #tasks = []
    results = []
    root_output_files = []
    for dataset_name, file_glob_list in fileset.items():
        ROOT_OUTPUT_FILE = ROOT_OUTPUT_DIR + f"/histograms_{dataset_name}_{working_point}.root"
        root_output_files.append(ROOT_OUTPUT_FILE)

        # get the list of files for this dataset
        file_list = []
        for file_glob in file_glob_list:
            file_list.extend(glob.glob(file_glob))

        # Process the dataset
        results.append(process_dataset(dataset_name, file_list))

        for flavour in hadron_flavours.values():
            plot_ratio(ROOT_OUTPUT_FILE, 'deepflav_btag_pt_eta', 'no_btag_pt_eta', dataset_name, flavour, working_point, 'deepflavour', 'B-tagging Efficiency')
            plot_ratio(ROOT_OUTPUT_FILE, 'deepcsv_btag_pt_eta', 'no_btag_pt_eta', dataset_name, flavour, working_point, 'deepcsv', 'B-tagging Efficiency')

if __name__ == "__main__":
    run()
