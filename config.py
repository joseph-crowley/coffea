BTAG_WP = 3
hadron_flavours = {0: "udsg", 4: "c", 5: "b"}
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

ROOT_OUTPUT_DIR = "/home/users/crowley/code/coffea/root"

# set configs for histograms
hist_configs = {
    'eta': {
        'title': 'eta',
        'bins': 26,
        'range': (-2.5, 2.5),
    },
    'pt': {
        'title': 'pT',
        'bins': 11,
        'range': (20, 200),
    }
}

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