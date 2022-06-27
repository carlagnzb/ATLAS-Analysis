
def submit_files(files):
    # Crear submit dels files per cada fitxer
    count = 0
    for file in files:
        f = os.path.split(file)
        spl = f[-1].split('.')
        job_file = job_directory_submit + ("/%s.sub" %spl[0])
        with open(job_file, 'w') as fh:
            fh.write("#!/bin/bash \n")
            fh.write("#SBATCH --workdir=/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master \n")
            fh.write("#SBATCH --error=error_df50_v1.err \n")
            fh.write("#SBATCH --output=%s \n" %file_out)
            fh.write("#SBATCH --nodes=1 \n")
            fh.write("#SBATCH --mail-type=all \n")
            fh.write("#SBATCH --mail-user=cgonzalez@pic.es \n")
            fh.write("#SBATCH --qos=class_a \n")
            fh.write("module load singularity \n")
            fh.write("cd /gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master \n")
            fh.write("myMount='/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master' \n")
            fh.write("singularity exec --bind $myMount:$myMount physlite-experiments.sif python -m physlite_experiments.scripts.run_analysis_example_v2 %s \n" %file)
            count += 1
            # Enviar a la cua el submit i emmagatzemar id
            subprocess.Popen(['sbatch --wait %s >> submit_id.txt' %(job_file)], shell=True)
    return

def check_submits():
    # Esperar submit de tots
    lines = []
    fi_array = len(files)
    while len(lines) <= fi_array:
        with open('submit_id.txt') as f:
            lines = [line.rstrip()[-8:] for line in f]
        if len(lines) == fi_array:
            string =','.join(lines)
            break
    return string

def merge(files_dependency):
    # Definir merge amb les dependencies
    with open(file_merge_sub, 'w') as mt:
        mt.write("#!/bin/bash \n")
        mt.write("#SBATCH --workdir=/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master \n")
        mt.write("#SBATCH --error=error_merge_df50.err \n")
        mt.write("#SBATCH --output=output_merge_df50__v1.out \n")
        mt.write("#SBATCH --nodes=1 \n")
        mt.write("#SBATCH --mail-type=all \n")
        mt.write("#SBATCH --mail-user=cgonzalez@pic.es \n")
        mt.write("#SBATCH --qos=class_a \n")
        mt.write("#SBATCH --depend=afterany:%s \n" %files_dependency)
        mt.write("cd /gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master \n")
        mt.write("python exec_merge.py %s " %file_out )
        mt.write("%s " %(len(files)))

    # Submit del merge i enviar a la cua
    cmd_m='sbatch --wait %s ' %(file_merge_sub)
    status, jobId=commands.getstatusoutput(cmd_m)
    return status, jobId

if __name__ == "__main__":
    import commands, os
    import sys
    import subprocess
    import glob
    import time
    from os.path import exists

    #Llistat  dels fitxers a analitzar
    files = glob.glob('/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/files_200/*.parquet')

    #On s'emmagatzemen els submits
    job_directory_submit = "/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/submits_v1"
    
    file_out = "/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/output_test_1.out"
    file_merge_sub = "/gpfs/scratch/ifae96/ifae96807/20-physlite-experiments-master/merge_test_1.sub"

    start = time.time()  
    
    # -- Crear submits + submit per cada file
    submit_files(files)

    # -- Check si estan a la cua, retorn dels id dels submits 
    files_dependency = check_submits()

    # -- merge
    status, jobId = merge(files_dependency)
    

    if (status == 0):
        stop = time.time()
        print('IT TOOK: ', str(stop-start))

