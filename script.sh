#!/bin/bash
cd /home/administrador/sync-job/
source venv/bin/activate
python job.py >> logcron.log
