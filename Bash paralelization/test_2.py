
def execution_files(files):
    # Crear els jobs
    for i, file in enumerate(files):
        job_file = job_directory_execution + ("/exec_%s.sh" %i)
        with open(job_file, 'w') as fh:
            fh.write("#!/bin/bash \n")
            fh.write("module load singularity \n")
            fh.write("cd /gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master \n")
            fh.write("myMount='/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master' \n")
            fh.write("singularity exec --bind $myMount:$myMount physlite-experiments.sif python -m physlite_experiments.scripts.run_analysis_example_v2 %s \n" %file)
            # Permisos d'execució
            subprocess.Popen(['chmod +x %s ' %(job_file)], shell=True)
    return

def submit_master(master_file, fi_array):
    # Crear master submit file
    with open(master_file, 'w') as mt:
        mt.write("#!/bin/bash \n")
        mt.write("#SBATCH --workdir=/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master \n")
        mt.write("#SBATCH --error=error_file_v2.err \n")
        mt.write("#SBATCH --output=%s \n" %file_out)
        mt.write("#SBATCH --nodes=1 \n")
        mt.write("#SBATCH --mail-type=all \n")
        mt.write("#SBATCH --mail-user=cgonzalez@pic.es \n")
        mt.write("#SBATCH --qos=class_a \n")
        mt.write("#SBATCH --array=0-%s \n" %fi_array)
        mt.write("cd /gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/execution \n")
        mt.write("CASE_NUM=exec_${SLURM_ARRAY_TASK_ID}.sh \n")
        mt.write("./$CASE_NUM")

    # Enviar a la cua el submit del master
    cmd="sbatch --wait %s" %(master_file)
    status, jobId=commands.getstatusoutput(cmd)
    return status, jobId

def submit_merge(status, jobId, files, file_out):
     # Crear el submit del merge
    if (status == 0):
        with open("submit_merge_v2.sub", 'w') as mt:
            mt.write("#!/bin/bash \n")
            mt.write("#SBATCH --workdir=/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master \n")
            mt.write("#SBATCH --error=error_merge_v2.err \n")
            mt.write("#SBATCH --output=output_merge_v2.out \n")
            mt.write("#SBATCH --nodes=1 \n")
            mt.write("#SBATCH --mail-type=all \n")
            mt.write("#SBATCH --mail-user=cgonzalez@pic.es \n")
            mt.write("#SBATCH --qos=class_a \n")
            mt.write("#SBATCH --depend=afterany:%s \n" %(jobId.split()[-1]))
            mt.write("cd /gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master \n")
            mt.write("python exec_merge.py %s " %file_out )
            mt.write("%s " %(len(files)))

    # Enviar a la cua el submit del merge
    cmd_m='sbatch %s ' %("submit_merge_v2.sub")
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
 
    # -- On guardem jobs a executar --
    job_directory_execution = "/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/exec_50"
 
   # -- File dels outputs de l'analisis de cada file --
    file_out = "/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/output_file_test_2.out"

    # -- File master
    master_file = "/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/master_submit_test_2.sub"

    fi_array = str(len(files) -1)

    start = time.time()

    # -- Creacio de files execucio --
    execution_files(files)

    # -- Creacio del master file
    status, jobId = submit_master(master_file, fi_array)

    # -- Creacio del submit merge --
    status_m, jobId_m = submit_merge(status, jobId, files, file_out)
    
    if (status_m == 0):
        stop = time.time()
        
        print('IT TOOK: ', str(stop-start))





