#!/bin/bash
cd /home/administrador/sync-job/
source venv/bin/activate
python update-state.py >> logcron-update.log
