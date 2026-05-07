# HinglishDB Streamlit UI - Quick Start Guide

## Setup

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Compile the C++ Engine (if not already compiled)

```bash
g++ -std=c++17 -o HinglishJoinEngine.exe main.cpp parser.cpp engine.cpp table.cpp
```

### 3. Run the Streamlit App

```bash
streamlit run streamlit_ui.py
python -m streamlit run streamlit_ui.py
```

The app will open at `http://localhost:8501`

## Features

✨ **Query Builder UI**
- Select tables, columns, and join type from dropdowns
- Automatically generates Hinglish commands
- One-click execution

📊 **Results Display**
- Beautiful tabular output with pandas DataFrame
- CSV download option
- Raw terminal output view
- Hinglish command reference

🔍 **Available Tables**
- Automatically detects `.tbl` files in `data/` folder
- Shows table schemas and columns

## How It Works

1. **Sidebar Query Builder**
   - Select left and right tables
   - Choose join columns
   - Pick INNER or LEFT join type
   - Click "Execute Query"

2. **Results Tab**
   - View results in formatted table
   - Download as CSV

3. **Command Tab**
   - See the actual Hinglish command generated
   - Learn the command syntax

4. **Raw Output Tab**
   - See full terminal output for debugging

## Supported Join Commands

### INNER JOIN
```
students aur marks ko students.id = marks.student_id par inner join karke dikha
```

### LEFT JOIN
```
students aur marks ko students.id = marks.student_id par left join karke dikha
```

## Troubleshooting

**If "HinglishJoinEngine.exe not found"**
- Compile the C++ program first in the HinglishDB directory

**If tables aren't showing**
- Ensure `.tbl` files are in the `data/` folder
- Check file format matches the documented schema

**If results won't display**
- Check the "Raw Output" tab to see terminal output
- Verify table names and columns are correct
