#!/bin/bash
cd /home/administrador/sync-job/
source venv/bin/activate
python sync.py >> logcron-sync.log
