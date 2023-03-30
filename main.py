import glob
from coffea import processor
from config import BTAG_WP, hadron_flavours
from histogram import save_csv, plot_hist, hist_ratio
from processor import MyProcessor, MySchema
from concurrent.futures import ProcessPoolExecutor

def process_dataset_wrapper(args):
    return process_dataset(*args)

def process_dataset(dataset_name, file_list):
    single_fileset = {dataset_name: file_list}
    result = processor.run_uproot_job(
        single_fileset,
        treename="SkimTree",
        processor_instance=MyProcessor(),
        executor=processor.futures_executor,
        executor_args={'schema': MySchema, 'workers': 24}, # Adjust the number of workers as needed
        chunksize=50000,
        maxchunks=None,
    )

    # Create a ROOT file to save the histograms
    working_point = ["NONE",'Loose', "Medium","Tight"][BTAG_WP] 
    # Loop over each jet flavour
    for flavour in hadron_flavours.values():
        deepflav_hist = result['deepflav_btag_pt_eta'].integrate('flavour', flavour)
        deepcsv_hist = result['deepcsv_btag_pt_eta'].integrate('flavour', flavour)
        all_hist = result['no_btag_pt_eta'].integrate('flavour', flavour)

        # Save the histograms as CSV files
        save_csv(deepflav_hist, dataset_name, flavour, working_point, 'deepflavour')
        save_csv(deepcsv_hist, dataset_name, flavour, working_point, 'deepcsv')
        save_csv(all_hist, dataset_name, flavour, working_point, 'no_btag')
        
        # Plot the histograms
        plot_hist(deepflav_hist, dataset_name, flavour, working_point, 'deepflavour')
        plot_hist(deepcsv_hist, dataset_name, flavour, working_point, 'deepcsv')
        plot_hist(all_hist, dataset_name, flavour, working_point, 'no_btag')

        # take the ratio of the deepflavour and no_btag histograms to get the efficiency
        deepflav_eff = hist_ratio(deepflav_hist, all_hist, dataset_name, flavour, working_point, 'deepflavour_eff')
        deepcsv_eff = hist_ratio(deepcsv_hist, all_hist, dataset_name, flavour, working_point, 'deepcsv_eff')

        # take the ratio of the efficiency histograms to get the scale factor
        deepflav_deepcsv_sf = hist_ratio(deepflav_eff, deepcsv_eff, dataset_name, flavour, working_point, 'deepflavour_sf')
        
        # Save the efficiency histograms as CSV files
        save_csv(deepflav_eff, dataset_name, flavour, working_point, 'deepflavour_eff')
        save_csv(deepcsv_eff, dataset_name, flavour, working_point, 'deepcsv_eff')
        save_csv(deepflav_deepcsv_sf, dataset_name, flavour, working_point, 'deepflavour_deepcsv_ratio')
        
        # Plot the efficiency histograms
        plot_hist(deepflav_eff, dataset_name, flavour, working_point, 'deepflavour_eff')
        plot_hist(deepcsv_eff, dataset_name, flavour, working_point, 'deepcsv_eff')
        plot_hist(deepflav_deepcsv_sf, dataset_name, flavour, working_point, 'deepflavour_deepcsv_ratio')

    return result

if __name__ == '__main__':
    fileset = {
        "tt_2l2nu": [
            "/ceph/cms/store/group/tttt/Worker/usarica/output/SimJetEffs/230309/2018/TT_2l2nu_*.root"
        ],
        "ttW": [
            "/ceph/cms/store/group/tttt/Worker/usarica/output/SimJetEffs/230309/2018/TTW_*.root"
        ],
        "DY": [
            "/ceph/cms/store/group/tttt/Worker/usarica/output/SimJetEffs/230309/2018/DY_*.root"
        ]
    }

    # add an "all_samples" dataset that combines all the other datasets
    fileset["all_samples"] = []
    for dataset_name, file_glob_list in fileset.items():
        if dataset_name == "all_samples":
            continue
        for file_glob in file_glob_list:
            fileset["all_samples"].append(file_glob)


    # Loop over each dataset
    #tasks = []
    results = []
    for dataset_name, file_glob_list in fileset.items():
        print(f"Processing dataset: {dataset_name}")

        # get the list of files for this dataset
        file_list = []
        for file_glob in file_glob_list:
            file_list.extend(glob.glob(file_glob))

        # Process the dataset
        results.append(process_dataset(dataset_name, file_list))
#        tasks.append((dataset_name, file_list))
#
#    # Process the datasets in parallel
#    with ProcessPoolExecutor() as executor:
#        results = executor.map(process_dataset_wrapper, tasks)

    # Print the results
    for result in results:
        print(result)

