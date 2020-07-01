#!/usr/bin/env bash
sudo -i -u root bash << EOF
/root/.local/share/virtualenvs/scrap-9ZJEulFg/bin/python /home/ec2-user/scrap/code/main_scrap.py 4 7
echo 'Fin' 
EOF