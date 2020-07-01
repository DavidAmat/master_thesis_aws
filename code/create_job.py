job_example = """
#!/usr/bin/env bash
sudo -i -u root bash << EOF
/root/.local/share/virtualenvs/scrap-9ZJEulFg/bin/python /home/ec2-user/scrap/code/main_scrap.py 0 10
EOF
"""

import os
import numpy as np

# Create the folder jobs if not exists
if "jobs" not in os.listdir("."):
    os.mkdir("jobs")

# Set the number of rows of the dataframe
n_rows = 83957 # starts by 0 and ends by 83956

# Number of songs to download per batch
n_samples_batch = 100

# Number of batches (each batch is a job and it's comprised by two 2inputs: initial row and final row)
n_batchs = int(np.ceil(n_rows / n_samples_batch))

# Fixed commands
bash_script = """#!/usr/bin/env bash
sudo -i -u root bash << EOF"""
end_bash = "\necho 'Fin' \nEOF"

# Save batch scripts
for i_batch, nb in enumerate(range(n_batchs)):
    initial_row = i_batch * n_samples_batch
    final_row = (i_batch+1) * n_samples_batch - 1 #avoid repeating the number that will be in the next batch as initial_row
    # If the final batch
    if final_row == n_rows:
        final_row = n_rows - 1 # avoid accessing an unexisting element in the df

    # Command to run (RUN IN THE COMPUTE NODE) (paths and executables must be referred to that node)
    run_str = f"""\n/root/.local/share/virtualenvs/scrap-9ZJEulFg/bin/python /home/ec2-user/scrap/code/main_scrap.py {initial_row} {final_row}"""

    # Generate full bash script
    full_script =  bash_script + run_str + end_bash

    # Save to the jobs/ folder
    with open(f"jobs/job_{i_batch}.sh", "w") as jf:
        jf.write(full_script)
    