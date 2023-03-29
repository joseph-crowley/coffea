from coffea import processor
from coffea.nanoevents import BaseSchema
from coffea import hist 
from config import BTAG_WP, hadron_flavours, branches

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
    
        # Loop over jets in each event
        ctr = 0
        for event in selected_events:
            ctr += 1
            if ctr > 100: break
            # Loop over each jet
            for jet in range(len(event.ak4jets_pt_Nominal)):
                # Check if the jet passes the b-tagging working point
                deep_flavour_lmt = event.ak4jets_btagcat_Nominal[jet] % 4
                deep_csv_lmt = (event.ak4jets_btagcat_Nominal[jet] // 4) % 4

                # Fill the histograms
                output['no_btag_pt_eta'].fill(
                    flavour=hadron_flavours[event.ak4jets_hadronFlavour_Nominal[jet]],
                    eta=event.ak4jets_eta_Nominal[jet],
                    pt=event.ak4jets_pt_Nominal[jet],
                )

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

    