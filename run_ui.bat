@echo off
REM HinglishDB Streamlit UI Launcher

echo Installing dependencies...
pip install -r requirements.txt

echo.
echo Starting Streamlit app...
echo.
echo The app will open at http://localhost:8501
echo Press Ctrl+C to stop the server
echo.

python -m streamlit run streamlit_ui.py
