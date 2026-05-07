"""
Hinglish DB - Streamlit Web UI
A user-friendly interface for the HinglishDB join engine
"""

import streamlit as st
import subprocess
import pandas as pd
from pathlib import Path

# ============================================================================
# Configuration
# ============================================================================
st.set_page_config(
    page_title="HinglishDB UI",
    page_icon="H",
    layout="wide",
    initial_sidebar_state="expanded"
)

# App title and description
st.title("HinglishDB Join Engine")
st.markdown("""
A **file-based mini relational processor** that understands **Hinglish** (Hindi + English) commands
to perform database JOIN operations on pre-existing table files.
""")

# ============================================================================
# Helper Functions
# ============================================================================

def parse_table_file(filepath):
    """Parse a .tbl file to extract schema and data"""
    schema = {}
    data = []

    in_schema = False
    in_data = False

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()

                if line == '#schema:':
                    in_schema = True
                    in_data = False
                    continue
                elif line == '#data:':
                    in_schema = False
                    in_data = True
                    continue
                elif line.startswith('#'):
                    continue

                if in_schema and line:
                    parts = line.split()
                    col_name = parts[0]
                    col_type = parts[1] if len(parts) > 1 else 'UNKNOWN'
                    col_meta = {
                        "type": col_type,
                        "is_primary": "PRIMARY_KEY" in parts,
                        "is_foreign": "FOREIGN_KEY" in parts,
                        "ref_table": "",
                        "ref_column": "",
                    }
                    if "FOREIGN_KEY" in parts and "REFERENCES" in parts:
                        ref_idx = parts.index("REFERENCES") + 1
                        if ref_idx < len(parts):
                            ref = parts[ref_idx]
                            if "(" in ref and ")" in ref:
                                col_meta["ref_table"] = ref.split("(", 1)[0]
                                col_meta["ref_column"] = ref.split("(", 1)[1].rstrip(")")
                    schema[col_name] = col_meta

                if in_data and line:
                    data.append(line)

        return schema, data
    except Exception as e:
        st.error(f"Error parsing {filepath}: {e}")
        return {}, []


def get_available_tables():
    """Get list of all .tbl files in data/ folder"""
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data"
    tables = {}

    if data_dir.exists():
        for tbl_file in data_dir.glob("*.tbl"):
            table_name = tbl_file.stem
            schema, _ = parse_table_file(str(tbl_file))
            tables[table_name] = schema

    return tables


def primary_key_columns(tables, table_name):
    return [
        col for col, meta in tables.get(table_name, {}).items()
        if meta.get("is_primary")
    ]


def foreign_key_columns(tables, table_name, ref_table=None, ref_column=None):
    cols = []
    for col, meta in tables.get(table_name, {}).items():
        if not meta.get("is_foreign"):
            continue
        if ref_table and meta.get("ref_table") != ref_table:
            continue
        if ref_column and meta.get("ref_column") != ref_column:
            continue
        cols.append(col)
    return cols


def get_join_options(tables, left_table, right_table):
    """Return valid PK/FK join options in either table order."""
    options = []
    for left_col, left_meta in tables.get(left_table, {}).items():
        for right_col, right_meta in tables.get(right_table, {}).items():
            right_refs_left = (
                left_meta.get("is_primary")
                and right_meta.get("is_foreign")
                and right_meta.get("ref_table") == left_table
                and right_meta.get("ref_column") == left_col
            )
            left_refs_right = (
                right_meta.get("is_primary")
                and left_meta.get("is_foreign")
                and left_meta.get("ref_table") == right_table
                and left_meta.get("ref_column") == right_col
            )
            if right_refs_left or left_refs_right:
                options.append({
                    "left_col": left_col,
                    "right_col": right_col,
                    "label": f"{left_table}.{left_col} = {right_table}.{right_col}",
                })
    return options


def build_hinglish_command(table1, col1, table2, col2, join_type,
                           use_third=False, table3="", col2_second="", col3="",
                           use_agg=False, agg_table="", agg_col="", agg_func=""):
    """Build a Hinglish command from parameters"""
    join_keyword = "inner join" if join_type == "INNER" else "left join"
    if use_third:
        command = (
            f"{table1} aur {table2} aur {table3} ko "
            f"{table1}.{col1} = {table2}.{col2} aur "
            f"{table2}.{col2_second} = {table3}.{col3} par {join_keyword} karke"
        )
    else:
        command = f"{table1} aur {table2} ko {table1}.{col1} = {table2}.{col2} par {join_keyword} karke"
    if use_agg:
        command += f" {agg_table}.{agg_col} ka {agg_func.lower()} nikal kar dikha"
    else:
        command += " dikha"
    return command


def execute_command(command):
    """Execute a Hinglish command using the C++ engine"""
    script_dir = Path(__file__).parent
    exe_path = script_dir / "HinglishJoinEngine.exe"

    if not exe_path.exists():
        return None, f"Error: {exe_path} not found. Please compile the C++ program first."

    try:
        # Pass command as command-line argument to the executable
        process = subprocess.Popen(
            [str(exe_path), command],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(script_dir)  # Set working directory to script location
        )
        stdout, stderr = process.communicate(timeout=10)
        return stdout, stderr
    except subprocess.TimeoutExpired:
        return None, "Command execution timed out"
    except Exception as e:
        return None, f"Error executing command: {str(e)}"


def parse_output_to_dataframe(output):
    """Try to parse the terminal output into a DataFrame"""
    lines = output.split('\n')

    # Find the table data between separator lines
    data_start = -1
    data_end = -1

    for i, line in enumerate(lines):
        if '|' in line and 'students' in line.lower() or 'marks' in line.lower():
            if data_start == -1:
                data_start = i
            data_end = i

    if data_start == -1:
        return None

    # Extract header and rows
    try:
        header_line = lines[data_start]
        headers = [h.strip() for h in header_line.split('|')[1:-1]]

        rows = []
        for line in lines[data_start + 2:data_end + 1]:
            if '|' not in line or len(line.strip()) == 0:
                continue
            if '-' in line:  # Skip separator lines
                continue
            row = [cell.strip() for cell in line.split('|')[1:-1]]
            if len(row) == len(headers):
                rows.append(row)

        if rows:
            return pd.DataFrame(rows, columns=headers)
    except Exception as e:
        st.warning(f"Could not parse output as table: {e}")

def parse_aggregation_result(output):
    """Parse aggregation result from terminal output"""
    if not output:
        return None
    lines = output.split('\n')
    agg_start = -1
    for i, line in enumerate(lines):
        if '>> AGGREGATE ON JOIN:' in line:
            agg_start = i
            break
            
    if agg_start != -1:
        # Get the lines corresponding to the aggregation table block
        agg_lines = []
        for line in lines[agg_start:]:
            if line.strip() == "" and len(agg_lines) > 5:
                break
            agg_lines.append(line)
        return '\n'.join(agg_lines)
    return None

def load_csv_output(join_type, table1, table2, table3=""):
    """Load the CSV output file generated by the join"""
    # Use script directory as base, not current working directory
    script_dir = Path(__file__).parent
    if table3:
        csv_file = script_dir / "data" / f"output_{join_type.lower()}_join_3_{table1}_{table2}_{table3}.csv"
    else:
        csv_file = script_dir / "data" / f"output_{join_type.lower()}_join_{table1}_{table2}.csv"
    if csv_file.exists():
        return pd.read_csv(str(csv_file))
    return None


# ============================================================================
# Sidebar - Command Builder
# ============================================================================
with st.sidebar:
    st.header("Query Builder")

    # Get available tables
    tables = get_available_tables()

    if not tables:
        st.error("No tables found in ./data/ folder")
    else:
        table_names = list(tables.keys())

        # Table Selection
        col1, col2 = st.columns(2)
        with col1:
            table1 = st.selectbox("Left Table", table_names, key="table1")
        with col2:
            table2 = st.selectbox("Right Table", table_names, key="table2",
                                 index=min(1, len(table_names) - 1))

        # Column Selection
        join_options = get_join_options(tables, table1, table2)
        if not join_options:
            st.warning("No valid PK-FK relationship found between selected tables.")
            selected_join = {"left_col": "", "right_col": ""}
        else:
            selected_label = st.selectbox(
                "Join Condition",
                [option["label"] for option in join_options],
                key="join_condition"
            )
            selected_join = next(option for option in join_options if option["label"] == selected_label)

        col1_selected = selected_join["left_col"]
        col2_selected = selected_join["right_col"]

        st.divider()

        use_third = st.checkbox("Add Third Table")
        table3 = ""
        col2_second = ""
        col3_selected = ""
        if use_third:
            third_table_names = [name for name in table_names if name not in {table1, table2}]
            if not third_table_names:
                st.warning("3-table join ke liye third table first two tables se different hona chahiye.")
            table3 = st.selectbox("Third Table", third_table_names or [""], key="table3")
            third_options = get_join_options(tables, table2, table3)
            if not third_options:
                st.warning("No valid PK-FK relationship found between second and third table.")
            else:
                selected_third_label = st.selectbox(
                    "Second Join Condition",
                    [option["label"] for option in third_options],
                    key="second_join_condition"
                )
                selected_third_join = next(
                    option for option in third_options
                    if option["label"] == selected_third_label
                )
                col2_second = selected_third_join["left_col"]
                col3_selected = selected_third_join["right_col"]

        # Join Type Selection
        join_type = st.radio("Join Type", ["INNER", "LEFT"], horizontal=True)

        st.divider()
        
        use_agg = st.checkbox(
            "Add Aggregation",
            disabled=use_third,
            help="Aggregation is supported only for two-table joins."
        )
        agg_table = ""
        agg_col = ""
        agg_func = ""
        if use_third:
            st.info("Aggregation is disabled for 3-table joins.")
        use_agg = use_agg and not use_third
        if use_agg:
            col_a_t, col_a_c, col_a_f = st.columns(3)
            with col_a_t:
                agg_table = st.selectbox("Agg Table", [table1, table2], key="agg_table")
            with col_a_c:
                agg_cols = list(tables.get(agg_table, {}).keys())
                agg_col = st.selectbox("Agg Column", agg_cols, key="agg_col")
            with col_a_f:
                agg_func = st.selectbox("Function", ["SUM", "AVG", "COUNT", "MIN", "MAX"], key="agg_func")

        st.divider()

        # Display Hinglish Command
        hinglish_cmd = build_hinglish_command(
            table1, col1_selected, table2, col2_selected, join_type,
            use_third, table3, col2_second, col3_selected,
            use_agg, agg_table, agg_col, agg_func
        )
        st.code(hinglish_cmd, language="text")

        # Execute Button
        can_execute = bool(col1_selected and col2_selected and (not use_third or (table3 and col2_second and col3_selected)))
        if st.button("Execute Query", use_container_width=True, type="primary", disabled=not can_execute):
            with st.spinner("Executing query..."):
                stdout, stderr = execute_command(hinglish_cmd)

                if stderr and "Error" in stderr:
                    st.error(f"Execution Error:\n{stderr}")
                else:
                    st.session_state.last_output = stdout
                    st.session_state.last_command = hinglish_cmd
                    st.session_state.last_join_type = join_type
                    st.session_state.last_table1 = table1
                    st.session_state.last_table2 = table2
                    st.session_state.last_table3 = table3 if use_third else ""
                    st.success("Query executed successfully!")

# ============================================================================
# Main Area - Results Display
# ============================================================================

if "last_output" not in st.session_state:
    st.info("Use the sidebar to build and execute a query")
else:
    # Tabs for different output views
    tab1, tab2, tab3 = st.tabs(["Results", "Hinglish Command", "Raw Output"])

    with tab1:
        st.subheader("Query Results")

        # Try to load nice DataFrame from CSV first
        df = load_csv_output(
            st.session_state.last_join_type,
            st.session_state.last_table1,
            st.session_state.last_table2,
            st.session_state.get("last_table3", "")
        )

        if df is not None:
            st.dataframe(df, use_container_width=True)
            st.info(f"{len(df)} rows returned")

            # Download CSV
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download as CSV",
                data=csv,
                file_name=f"join_result.csv",
                mime="text/csv"
            )
        else:
            st.warning("Could not parse tabular output")

        agg_output = parse_aggregation_result(st.session_state.last_output)
        if agg_output:
            st.divider()
            st.subheader("Aggregation Result")
            st.code(agg_output, language="text")

    with tab2:
        st.subheader("Executed Hinglish Command")
        st.code(st.session_state.last_command, language="text")
        st.markdown("""
        **Command Syntax:**
        ```
        <table1> aur <table2> ko <table1>.<column1> = <table2>.<column2> par <inner|left> join karke [<table>.<column> ka <func> nikal kar] dikha
        <table1> aur <table2> aur <table3> ko <table1>.<pk> = <table2>.<fk> aur <table2>.<fk> = <table3>.<pk> par <inner|left> join karke dikha
        ```

        - **aur** = and
        - **ko** = with
        - **par** = on
        - **karke** = doing/performing
        - **dikha** = show
        - **ka** = of
        - **nikal kar dikha** = calculate and show
        """)

    with tab3:
        st.subheader("Raw Terminal Output")
        st.text_area("Output:", st.session_state.last_output, height=400, disabled=True)

# ============================================================================
# Information Section
# ============================================================================
with st.expander("About HinglishDB"):
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
        ### Features
        - **Hinglish Support**: Commands in Hindi-English mix
        - **Schema Validation**: Automatic FK/PK checking
        - **Multiple Join Types**: INNER and LEFT joins
        - **File-Based**: Pure C++ with fstream, no external DB
        - **Output Formats**: Terminal, CSV, and formatted TXT
        """)

    with col2:
        st.markdown("""
        ### Available Tables
        """)
        tables = get_available_tables()
        for table_name, columns in tables.items():
            st.write(f"**{table_name}**: {', '.join(columns)}")

with st.expander("Example Commands"):
    st.markdown("""
    **INNER JOIN Example:**
    ```
    students aur marks ko students.id = marks.student_id par inner join karke dikha
    ```
    Shows all students who have marks.

    **LEFT JOIN with Aggregation Example:**
    ```
    students aur marks ko students.id = marks.student_id par left join karke marks.score ka avg nikal kar dikha
    ```
    Shows all students, and calculates the average string score across all results.
    """)
