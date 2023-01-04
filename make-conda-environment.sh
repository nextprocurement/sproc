#!/bin/bash

MANAGER="mamba"

NAME=sproc

LIBRARIES=(
    jupyterlab jupyter_console
    nbdev">"2.3
    colorama
    python-magic
    pyyaml
    html2text
    lxml
    pandas
    pyarrow
    openpyxl
	ruamel.yaml
	# rich ipywidgets
    tqdm ipywidgets
)

CHANNELS=(
    defaults
    conda-forge
    fastai # nbdev
)

# ---

COLOR="\033[40m\033[32m"
UNCOLOR="\033[0m"


# ------------ setup

# only required if "anaconda" is not in the path
source $HOME/$MY_CONDA_INSTALLATION/etc/profile.d/conda.sh

# from https://stackoverflow.com/a/9429887/3967334
LIBRARIES_CONCATENATED=$(IFS=" " ; echo "${LIBRARIES[*]}")

# from https://stackoverflow.com/a/17841619/3967334
function join_by { local d=${1-} f=${2-}; if shift 2; then printf %s "$f" "${@/#/$d}"; fi; }

CHANNELS_CONCATENATED=$(join_by ' -c ' "${CHANNELS[@]}")

# ------------ installation

$MANAGER create --yes -n $NAME $LIBRARIES_CONCATENATED -c $CHANNELS_CONCATENATED

# ------------ pip

conda activate $NAME

# pip stuff here....

# ------------ nbdev

# so that git is aware of nbdev/notebooks (only required once, when creating the repository, not through re-installs of conda)
nbdev_install_hooks

# this library is installed "live"
pip install -e .

# ------------

echo -e new environment is \"$COLOR$NAME$UNCOLOR\"

# # the environment is exported into a yaml file
# conda env export --no-builds --from-history -f environment.yml
