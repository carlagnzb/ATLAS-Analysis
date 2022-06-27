#!/bin/bash

cd /gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master
myMount='/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master'

singularity exec --bind $myMount:$myMount physlite-experiments.sif python -m physlite_experiments.scripts.run_analysis_example_v2 DAOD_PHYSLITE.22958105._001022.parquet
