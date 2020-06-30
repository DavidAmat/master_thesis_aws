# Set the number of rows of the dataframe
max_row = 83957

bash_script = """#!/usr/bin/env bash
sudo -i -u root bash << EOF"""

for row in range(1123,1130):
    run_str = f"""\n/root/.local/share/virtualenvs/scrap-9ZJEulFg/bin/python /home/ec2-user/scrap/code/main_scrap.py {row}"""
    bash_script += run_str

bash_script += "\necho 'Fin'"
bash_script += "\nEOF"
print(bash_script)