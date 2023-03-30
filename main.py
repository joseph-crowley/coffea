import glob
from coffea import processor
from config import BTAG_WP, hadron_flavours, ROOT_OUTPUT_DIR
from histogram import save_csv, plot_hist, hist_ratio
from processor import MyProcessor, MySchema, save_accumulator_as_root
import uproot
#from concurrent.futures import ProcessPoolExecutor

def process_dataset_wrapper(args):
    return process_dataset(*args)

def process_dataset(dataset_name, file_list, result = None):
    print(f"Processing dataset: {dataset_name}")

    # Create a ROOT file to save the histograms
    working_point = ["NONE",'Loose', "Medium","Tight"][BTAG_WP] 

    single_fileset = {dataset_name: file_list}
    if result is None:
        result = processor.run_uproot_job(
            single_fileset,
            treename="SkimTree",
            processor_instance=MyProcessor(output_file=ROOT_OUTPUT_DIR + f"/histograms_{dataset_name}_{working_point}.root"),
            executor=processor.futures_executor,
            executor_args={'schema': MySchema, 'workers': 24}, # Adjust the number of workers as needed
            chunksize=50000,
            maxchunks=None,
        )

        #save_accumulator_as_root(result, f"histograms_{dataset_name}_{working_point}.root")

    # Loop over each jet flavour
    for flavour in hadron_flavours.values():
        deepflav_hist = result['deepflav_btag_pt_eta'].integrate('flavour', flavour)
        deepcsv_hist = result['deepcsv_btag_pt_eta'].integrate('flavour', flavour)
        all_hist = result['no_btag_pt_eta'].integrate('flavour', flavour)

        ## Save the histograms as CSV files
        #save_csv(deepflav_hist, dataset_name, flavour, working_point, 'deepflavour')
        #save_csv(deepcsv_hist, dataset_name, flavour, working_point, 'deepcsv')
        #save_csv(all_hist, dataset_name, flavour, working_point, 'no_btag')
        
        # Plot the histograms
        plot_hist(deepflav_hist, dataset_name, flavour, working_point, 'deepflavour', 'Number of b Jets')
        plot_hist(deepcsv_hist, dataset_name, flavour, working_point, 'deepcsv', 'Number of b Jets')
        plot_hist(all_hist, dataset_name, flavour, working_point, 'no_btag', 'Number of b Jets')

        # take the ratio of the deepflavour and no_btag histograms to get the efficiency
        deepflav_eff = hist_ratio(deepflav_hist, all_hist, dataset_name, flavour, working_point, 'deepflavour_eff')
        deepcsv_eff = hist_ratio(deepcsv_hist, all_hist, dataset_name, flavour, working_point, 'deepcsv_eff')

        # take the ratio of the efficiency histograms to get the scale factor
        deepflav_deepcsv_sf = hist_ratio(deepflav_eff, deepcsv_eff, dataset_name, flavour, working_point, 'deepflavour_sf')
        
        ## Save the efficiency histograms as CSV files
        #save_csv(deepflav_eff, dataset_name, flavour, working_point, 'deepflavour_eff')
        #save_csv(deepcsv_eff, dataset_name, flavour, working_point, 'deepcsv_eff')
        #save_csv(deepflav_deepcsv_sf, dataset_name, flavour, working_point, 'deepflavour_deepcsv_ratio')
        
        # Plot the efficiency histograms
        plot_hist(deepflav_eff, dataset_name, flavour, working_point, 'deepflavour_eff', 'B-tagging Efficiency')
        plot_hist(deepcsv_eff, dataset_name, flavour, working_point, 'deepcsv_eff', 'B-tagging Efficiency')
        plot_hist(deepflav_deepcsv_sf, dataset_name, flavour, working_point, 'deepflavour_deepcsv_ratio', 'DeepFlavour/DeepCSV Scale Factor')

    return result

if __name__ == '__main__':
    fileset = {
        "ttW": [
            "/ceph/cms/store/group/tttt/Worker/usarica/output/SimJetEffs/230309/2018/TTW_*.root"
        ],
        "tt_2l2nu": [
            "/ceph/cms/store/group/tttt/Worker/usarica/output/SimJetEffs/230309/2018/TT_2l2nu_*.root"
        ],
        "DY": [
            "/ceph/cms/store/group/tttt/Worker/usarica/output/SimJetEffs/230309/2018/DY_*.root"
        ]
    }

#    # add an "all_samples" dataset that combines all the other datasets
#    fileset["all_samples"] = []
#    for dataset_name, file_glob_list in fileset.items():
#        if dataset_name == "all_samples":
#            continue
#        for file_glob in file_glob_list:
#            fileset["all_samples"].append(file_glob)

    # Loop over each dataset
    #tasks = []
    results = []
    all_files = []
    for dataset_name, file_glob_list in fileset.items():
        # get the list of files for this dataset
        file_list = []
        for file_glob in file_glob_list:
            file_list.extend(glob.glob(file_glob))
        all_files.extend(file_list)

        # Process the dataset
        results.append(process_dataset(dataset_name, file_list))
#        tasks.append((dataset_name, file_list))
#
#    # Process the datasets in parallel
#    with ProcessPoolExecutor() as executor:
#        results = executor.map(process_dataset_wrapper, tasks)

    # add the results
    total_result = results[0]
    for result in results[1:]:
        total_result += result
        
    # Process the combined dataset
    process_dataset("all_samples", all_files, total_result)
