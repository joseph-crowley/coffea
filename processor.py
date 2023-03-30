from coffea import processor
from coffea.nanoevents import BaseSchema
from coffea import hist
from config import BTAG_WP, hadron_flavours, branches
import awkward as ak

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
                hist.Bin('eta', 'eta', 50, -2.5, 2.5),
                hist.Bin('pt', 'pT', 100, 0, 300),
            ),
            'deepcsv_btag_pt_eta': hist.Hist(
                'Counts',
                hist.Cat('flavour', 'Jet Flavour'),
                hist.Bin('eta', 'eta', 50, -2.5, 2.5),
                hist.Bin('pt', 'pT', 100, 0, 300),
            ),
            'no_btag_pt_eta': hist.Hist(
                'Counts',
                hist.Cat('flavour', 'Jet Flavour'),
                hist.Bin('eta', 'eta', 50, -2.5, 2.5),
                hist.Bin('pt', 'pT', 100, 0, 300),
            ),
        })

    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        output = self.accumulator.identity()

        # Filter events
        selected_events = events[events.nleptons_tight_Nominal > 0]

        # Create arrays for jets and their properties
        jets_pt = ak.Array(selected_events.ak4jets_pt_Nominal)
        jets_eta = ak.Array(selected_events.ak4jets_eta_Nominal)
        jets_btagcat = ak.Array(selected_events.ak4jets_btagcat_Nominal)
        jets_hadronFlavour = ak.Array(selected_events.ak4jets_hadronFlavour_Nominal)
        event_weights = ak.Array(selected_events.event_wgt_SimOnly_Nominal)
        
        # Broadcast event weights to the same shape as jets_pt
        event_weights = ak.broadcast_arrays(event_weights, jets_pt)[0]

        # Create masks for each jet flavour
        cjet_mask = jets_hadronFlavour == 4
        bjets_mask = jets_hadronFlavour == 5
        udsg_mask = jets_hadronFlavour == 0
        
        # Calculate btag limits
        deep_flavour_lmt = jets_btagcat % 4
        deep_csv_lmt = (jets_btagcat // 4) % 4


        # Fill histograms
        output['no_btag_pt_eta'].fill(
            flavour = "b",
            eta=ak.flatten(jets_eta),
            pt=ak.flatten(jets_pt),
            weight=ak.flatten(event_weights),
        )
        
        output['no_btag_pt_eta'].fill(
            flavour = "c",
            eta=ak.flatten(jets_eta),
            pt=ak.flatten(jets_pt),
            weight=ak.flatten(event_weights),
        )
        
        output['no_btag_pt_eta'].fill(
            flavour = "udsg",
            eta=ak.flatten(jets_eta),
            pt=ak.flatten(jets_pt),
            weight=ak.flatten(event_weights),
        )
        
        deepflav_btag_mask = deep_flavour_lmt >= BTAG_WP
        output['deepflav_btag_pt_eta'].fill(
            flavour = "b",            
            eta=ak.flatten(jets_eta[bjets_mask & deepflav_btag_mask]),
            pt=ak.flatten(jets_pt[bjets_mask & deepflav_btag_mask]),
            weight=ak.flatten(event_weights[bjets_mask & deepflav_btag_mask]),
        )
        
        output['deepflav_btag_pt_eta'].fill(
            flavour = "c",
            eta=ak.flatten(jets_eta[cjet_mask & deepflav_btag_mask]),
            pt=ak.flatten(jets_pt[cjet_mask & deepflav_btag_mask]),
            weight=ak.flatten(event_weights[cjet_mask & deepflav_btag_mask]),
        )
        
        output['deepflav_btag_pt_eta'].fill(
            flavour = "udsg",
            eta=ak.flatten(jets_eta[udsg_mask & deepflav_btag_mask]),
            pt=ak.flatten(jets_pt[udsg_mask & deepflav_btag_mask]),
            weight=ak.flatten(event_weights[udsg_mask & deepflav_btag_mask]),
        )
        
        deepcsv_btag_mask = deep_csv_lmt >= BTAG_WP
        output['deepcsv_btag_pt_eta'].fill(
            flavour = "b",
            eta=ak.flatten(jets_eta[bjets_mask & deepcsv_btag_mask]),
            pt=ak.flatten(jets_pt[bjets_mask & deepcsv_btag_mask]),
            weight=ak.flatten(event_weights[bjets_mask & deepcsv_btag_mask]),
        )

        output['deepcsv_btag_pt_eta'].fill(
            flavour = "c",
            eta=ak.flatten(jets_eta[cjet_mask & deepcsv_btag_mask]),
            pt=ak.flatten(jets_pt[cjet_mask & deepcsv_btag_mask]),
            weight=ak.flatten(event_weights[cjet_mask & deepcsv_btag_mask]),
        )
        
        output['deepcsv_btag_pt_eta'].fill(
            flavour = "udsg",
            eta=ak.flatten(jets_eta[udsg_mask & deepcsv_btag_mask]),
            pt=ak.flatten(jets_pt[udsg_mask & deepcsv_btag_mask]),
            weight=ak.flatten(event_weights[udsg_mask & deepcsv_btag_mask]),
        )

        return output

    def postprocess(self, accumulator):
        return accumulator
