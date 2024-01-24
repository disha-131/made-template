#!/bin/bash
pip install --upgrade pip
pip install -r ./project/requirements.txt
python project/pipeline.py
python project/check_output_file.py