BTagging Efficiency Analyzer
============================

This repository contains a collection of Python scripts to analyze b-tagging efficiency in simulated particle collision events. It uses the [coffea](https://github.com/CoffeaTeam/coffea) library to efficiently process ROOT files and produce detailed histograms of b-tagging efficiency for different jet flavors and working points.

Table of Contents
-----------------

*   [Functionality](#functionality)
*   [Installation](#installation)
*   [Usage](#usage)
*   [File Structure](#file-structure)
*   [License](#license)

Functionality
-------------

The BTagging Efficiency Analyzer is designed to process and analyze particle collision event data stored in ROOT files. Specifically, it calculates the efficiency of different b-tagging algorithms (e.g., DeepFlavour and DeepCSV) for various jet flavors (e.g., b, c, and light-flavored jets) and working points (e.g., Loose, Medium, and Tight).

The main functionalities of the BTagging Efficiency Analyzer are as follows:

1.  Process ROOT files containing simulated particle collision event data.
2.  Extract relevant information about jets and their b-tagging properties.
3.  Create histograms of b-tagging efficiency as a function of jet transverse momentum (pT) and pseudorapidity (eta) for different jet flavors and working points.
4.  Save the histograms as CSV files.
5.  Generate 2D plots of the histograms and save them as image files (PNG format).
6.  Calculate the ratio of b-tagging efficiency between different algorithms or working points.
7.  Generate 2D plots of the ratio histograms and save them as image files (PNG format).

Installation
------------

1.  Clone this repository:
    
    `git clone https://github.com/joseph-crowley/coffea.git && cd coffea`
    
2.  Create a virtual environment and activate it:
    
    `python -m venv venv source venv/bin/activate`
    
3.  Install the required dependencies:
    
    `pip install -r requirements.txt`
    

Usage
-----

To run the BTagging Efficiency Analyzer, simply execute the `main.py` script:


`python main.py`

This script will process the specified ROOT files, create histograms and plots of b-tagging efficiency, and save the results in the `csv` and `plots` directories.

You can customize the input dataset, jet flavors, b-tagging algorithms, and working points by modifying the `config.py` file.

File Structure
--------------

*   `config.py`: Configuration file containing settings related to input datasets, jet flavors, b-tagging algorithms, and working points.
*   `file_utils.py`: Utility functions for reading and writing CSV files.
*   `histogram.py`: Functions to create and manipulate histograms, generate plots, and calculate efficiency ratios.
*   `main.py`: Main script to run the BTagging Efficiency Analyzer.
*   `processor.py`: Custom `coffea` processor to process ROOT files and extract relevant jet and b-tagging information.
*   `requirements.txt`: List of required Python packages.
*   `LICENSE`: License file.

License
-------

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
