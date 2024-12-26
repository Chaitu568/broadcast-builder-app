#! /bin/bash
sudo chmod 777 source
cd source/
uvicorn fastapi_endpoint:app --port 8001 --workers 20 &
streamlit run blb_app_gui.py --server.port 8080 & 
streamlit run Broadcast_Estimator.py --server.port 8081 &
python generate_dependency_csvs.py
