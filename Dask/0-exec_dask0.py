def submit_master(directori, master_file, num_jobs):
    print('Crear master submit file')
    with open(master_file, 'w') as mt:
        mt.write("#!/bin/bash \n")
        mt.write("#SBATCH --workdir=/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/0-dask0 \n")
        mt.write("#SBATCH --error=error_file_dask0.err \n")
        mt.write("#SBATCH --output=%s \n" %file_out)
        mt.write("#SBATCH --nodes=1 \n")
        mt.write("#SBATCH --cpus-per-task=48 \n")
        mt.write("#SBATCH --mail-type=all \n")
        mt.write("#SBATCH --mail-user=cgonzalez@pic.es \n")
        mt.write("#SBATCH --qos=class_a \n")
        mt.write("#SBATCH --array=0-%s \n" %num_jobs)

        mt.write("module load singularity \n")
        mt.write("cd /gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master \n")
        mt.write("myMount='/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master' \n")
        mt.write("singularity exec --bind $myMount:$myMount physlite-experiments_dask.sif python -m physlite_experiments.scripts.run_analysis_example_dask %s" %directori)

    print('Submit del master')
    cmd="sbatch --wait %s" %(master_file)
    status, jobId=commands.getstatusoutput(cmd)
    print(status, jobId)
    return status, jobId

def submit_merge(status, jobId, total_files, file_out):
    # Crear submit del merge
    with open("submit_merge_dask.sub", 'w') as mt:
        mt.write("#!/bin/bash \n")
        mt.write("#SBATCH --workdir=/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/0-dask0 \n")
        mt.write("#SBATCH --error=error_merge_dask.err \n")
        mt.write("#SBATCH --output=output_merge_dask.out \n")
        mt.write("#SBATCH --nodes=1 \n")
        mt.write("#SBATCH --mail-type=all \n")
        mt.write("#SBATCH --mail-user=cgonzalez@pic.es \n")
        mt.write("#SBATCH --qos=class_a \n")
        mt.write("#SBATCH --depend=afterany:%s \n" %(jobId.split()[-1]))
        mt.write("cd /gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master \n")
        mt.write("python dask_merge.py %s " %file_out )
        mt.write("%s " %total_files)

    # Enviar a la cua submit del merge
    cmd_m='sbatch %s ' %("submit_merge_dask.sub")
    status_m, jobId_m=commands.getstatusoutput(cmd_m)
    return status_m, jobId_m

if __name__ == "__main__":
    import commands, os
    import sys
    import subprocess
    import glob
    import time

    # -- Files a analitzar --
    files = glob.glob('/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/files_200/*.parquet')

    directori = "/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/files_200"

    # -- On guardem jobs a executar --
    job_file = "/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/0-dask0/exec_dask0.sh"

    # -- File dels outputs de l'analisis de cada file --
    file_out = "/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/0-dask0/output_file_dask0.out"

    # -- File master
    master_file = "/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/0-dask0/master_submit_dask0.sub"

    num_jobs = 24

    total_files = len(files)*(num_jobs+1)

    start = time.time()

    # -- Creacio del master file
    status, jobId = submit_master(directori, master_file, num_jobs)

    # -- Creacio del submit merge --
    status_m, jobId_m = submit_merge(status, jobId, total_files, file_out)
    
    if (status_m == 0):
        stop = time.time()
    
    print('IT TOOK: ', str(stop-start))
    temps = str(round(stop-start, 4))
