@echo off
:: Force the terminal to use UTF-8
chcp 65001
set PYTHONUTF8=1

cd /d "C:\Users\tmorg\Python\BiggestTeam2024\Cloud"
call .venv\Scripts\activate
python main_pipeline.py
pause