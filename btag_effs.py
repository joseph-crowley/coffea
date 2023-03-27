import uproot
import numpy as np
import matplotlib.pyplot as plt
from coffea import processor
from coffea.nanoevents import BaseSchema


# TODO: change these imports for coffea update
from coffea import hist 
#import hist
#from hist import Hist

import os

# Loose = 1, Medium = 2, Tight = 3
BTAG_WP = 1

# hadron flavours
hadron_flavours = {0:"udsg", 4:"c", 5:"b"}

branches = [
    "nleptons_fakeable_Nominal",
    "nleptons_tight_Nominal",
    "nleptons_loose_Nominal",
    "event_wgt_Nominal",
    "event_wgt_SimOnly_Nominal",
    "ak4jets_btagcat_Nominal",
    "ak4jets_hadronFlavour_Nominal",
    "ak4jets_partonFlavour_Nominal",
    "ak4jets_genjet_phi_Nominal",
    "ak4jets_genjet_mass_Nominal",
    "ak4jets_eta_Nominal",
    "ak4jets_pt_Nominal",
    "ak4jets_genjet_eta_Nominal",
    "ak4jets_mass_Nominal",
    "ak4jets_phi_Nominal",
    "ak4jets_genjet_pt_Nominal",
]

class MySchema(BaseSchema):
    def __init__(self, base_form):
        base_form["contents"] = {
            k: v
            for k, v in base_form["contents"].items() 
            if k in branches
        }
        super().__init__(base_form)

class MyProcessor(processor.ProcessorABC):
    def __init__(self):
        self._accumulator = processor.dict_accumulator({
            'deepflav_btag_pt_eta': hist.Hist(
                'Counts',
                hist.Cat('flavour', 'Jet Flavour'),
                hist.Bin('eta', 'eta', 50, -5, 5),
                hist.Bin('pt', 'pT', 100, 0, 500),
            ),
            'deepcsv_btag_pt_eta': hist.Hist(
                'Counts',
                hist.Cat('flavour', 'Jet Flavour'),
                hist.Bin('eta', 'eta', 50, -5, 5),
                hist.Bin('pt', 'pT', 100, 0, 500),
            ),
        })

    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        output = self.accumulator.identity()
    
        # Filter events
        selected_events = events[events.nleptons_tight_Nominal > 0]
    
        # Loop over jets in each event
        ctr = 0
        for event in selected_events:
         ctr += 1
         if ctr > 1000: break
         # Loop over each jet
         for jet in range(len(event.ak4jets_pt_Nominal)):
             # Check if the jet passes the b-tagging working point
             deep_flavour_lmt = event.ak4jets_btagcat_Nominal[jet] % 4
             deep_csv_lmt = (event.ak4jets_btagcat_Nominal[jet] // 4) % 4

             # Fill the histogram
             if deep_flavour_lmt >= BTAG_WP:
                 output['deepflav_btag_pt_eta'].fill(
                     flavour=hadron_flavours[event.ak4jets_hadronFlavour_Nominal[jet]],
                     eta=event.ak4jets_eta_Nominal[jet],
                     pt=event.ak4jets_pt_Nominal[jet],
                 )

             if deep_csv_lmt >= BTAG_WP:
                 output['deepcsv_btag_pt_eta'].fill(
                     flavour=hadron_flavours[event.ak4jets_hadronFlavour_Nominal[jet]],
                     eta=event.ak4jets_eta_Nominal[jet],
                     pt=event.ak4jets_pt_Nominal[jet],
                 )
             
        return output
    
    def postprocess(self, accumulator):
        return accumulator

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

    # Loop over each jet flavour
    for flavour in hadron_flavours.values():
        # set up the deep flavour plot
        fig, ax = plt.subplots()
        hist.plot2d(result['deepflav_btag_pt_eta'].integrate('flavour', flavour), xaxis='eta', xoverflow='all', ax=ax)
        ax.set_title(f"{dataset_name} - {flavour}")
    
        # Save the plot as an image file
        output_dir = 'plots'
        os.makedirs(output_dir, exist_ok=True)
        working_point = ["NONE",'Loose', "Medium","Tight"][BTAG_WP] 
        plt.savefig(os.path.join(output_dir, f"{dataset_name}_{flavour}_deepflavour_{working_point}_plot.png"), dpi=300)
        plt.close(fig)

        # set up the deep csv plot
        fig, ax = plt.subplots()
        hist.plot2d(result['deepcsv_btag_pt_eta'].integrate('flavour', flavour), xaxis='eta', xoverflow='all', ax=ax)
        ax.set_title(f"{dataset_name} - {flavour}")
        
        # Save the plot as an image file
        output_dir = 'plots'
        os.makedirs(output_dir, exist_ok=True)  
        working_point = ["NONE",'Loose', "Medium","Tight"][BTAG_WP]
        plt.savefig(os.path.join(output_dir, f"{dataset_name}_{flavour}_deepcsv_{working_point}_plot.png"), dpi=300)
        plt.close(fig)

    return result

if __name__ == '__main__':
    #fileset = {'tt_2l2nu': ['/ceph/cms/store/group/tttt/Worker/usarica/output/SimJetEffs/230309/2018/TT_2l2nu_*.root'], 'ttW':['/ceph/cms/store/group/tttt/Worker/usarica/output/SimJetEffs/230309/2018/TTW_*.root'], 'DY':['/ceph/cms/store/group/tttt/Worker/usarica/output/SimJetEffs/230309/2018/DY_*.root']}
    fileset = {'tt_2l2nu': ['/ceph/cms/store/group/tttt/Worker/usarica/output/SimJetEffs/230309/2018/TT_2l2nu_141_of_291.root']}
    
    # Loop over each dataset in the fileset
    for dataset_name, file_list in fileset.items():
        print(f"Processing dataset: {dataset_name}")
        process_dataset(dataset_name, file_list)
