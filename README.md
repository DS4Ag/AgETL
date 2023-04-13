[![Wang lab logo](https://static.wixstatic.com/media/c544bf_0e3064b159ae42238c83dca23bc352e8~mv2.png/v1/crop/x_0,y_0,w_1918,h_2080/fill/w_91,h_100,al_c,q_85,usm_0.66_1.00_0.01,enc_auto/lab_icon_3.png)](https://www.dianewanglab.com/)



[![Python version](https://img.shields.io/pypi/pyversions/pandas)](https://www.python.org/)
[![JupyterLab](https://img.shields.io/badge/Jupyter-lab-orange)](https://jupyter.org/)
[![Jupyter Notebook](https://img.shields.io/badge/Jupyter-Notebook-orange)](https://jupyter.org/)
[![YAML 1.2](https://img.shields.io/badge/YAML-1.2-success)](https://yaml.org/)



Note: to open links in new tab use CTRL+click (Windows and Linux) or CMD+click (MacOS). 

### What is AgETL?

**Ag**ronomic **E**xtraction, **T**ransformation, and **L**oad  is a set of functions written in python that allow you to process data files from different agricultural and plant science experiments and aggregate them into a standard database table in a central repository to make data available for different variety of data analyses. 

The execution of functions for this step is divided into two notebook files and configuration files. 

- **Extraction and Transformation processes:** 

Runs the Extraction and Transformation processes, and the user gets a CSV file where the data from different source files are aggregated and standardized into a single format.

    Notebook file: extract-transform.ipynb

    Configuration file: config_extract-transform.yml
    
- **Load processes** 

Loads the data into a single table in a data warehouse

    Notebook file: load.ipynb

    Configuration file: config_load.yml
    

## How to run AgETL?

- Option 1
  - You should make a simple **[installation](https://jupyter.org/install "jupyter.org")** of either **JupyterLab** or **Jupyter Notebook**, or you also can install an environment management such as [conda](https://docs.conda.io/en/latest/), [mamba](https://mamba.readthedocs.io/), or [pipenv](https://pipenv.pypa.io/).
  
- Option 2
  - Using a [Jupyter Hub](https://jupyter.org/try) enviroment.

## Prerequisites

Install the requiered libraries using the [pip package installer](https://pypi.org/project/pip/) for Python.

- [PyYAML](https://pypi.org/project/PyYAML/)
    ```sh
        pip install pyyaml
    
    ```
- [Pandas](https://pypi.org/project/pandas/)
    ```sh
        pip install pandas
    
    ```    
- [psycopg2](https://pypi.org/project/pandas/)
    ```sh
        pip install psycopg2 
    
    ```    

## Clone or download AgTC from the GitHub repository
    
- [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) option
    1. Open a new Jupyter Notebook Terminal
    
    New > Terminal 
    
    2. Clone the GitHub repository 
    
    ```sh
        git clone https://github.com/Purdue-LuisVargas/agETL.git
    
    ```
-  **Download** option

    1. [Download](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) **AgETL** from the **Github** repository: [https://github.com/Purdue-LuisVargas/agETL](https://github.com/Purdue-LuisVargas/agETL).
    2. Unzip the entire folder, then copy (if running Jupyter locally) or upload the downloaded files (if using the Jupyter Hub environment) in your Jupyter Notebook directory.
    
## Contact

Diane Wang - [drwang@purdue.edu](drwang@purdue.edu)


Luis Vargas Rojas - [lvargasr@purdue.edu](lvargasr@purdue.edu)


Purdue University, Wang Lab [dianewanglab.com](https://www.dianewanglab.com/)
