#!/bin/bash

cd /gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master
myMount='/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master'

singularity exec --bind $myMount:$myMount physlite-experiments_dask.sif python -m physlite_experiments.scripts.run_analysis_example_dask /gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/files_200

