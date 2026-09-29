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
    page_title="QIR-DB | Multi-Database Translation",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# App title and description
st.title("QIR-DB: Multi-Database Translation Engine")
st.markdown("""
A **Query Intermediate Representation Compiler** that translates **Hinglish** commands
into equivalent SQL for **MySQL**, **PostgreSQL**, **SQLite**, and **MongoDB** — no LLM needed.
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
    project_dir = Path(__file__).resolve().parent.parent
    data_dir = project_dir / "data"
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
    join_keywords = {
        "INNER": "inner join",
        "LEFT": "left join",
        "RIGHT": "right join",
        "FULL OUTER": "full outer join",
        "CROSS": "cross join",
    }
    join_keyword = join_keywords.get(join_type, "inner join")
    if join_type == "CROSS":
        # Cross join has no condition
        command = f"{table1} aur {table2} ko cross join karke"
    elif use_third:
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
    project_dir = Path(__file__).resolve().parent.parent
    exe_path = project_dir / "HinglishJoinEngine.exe"

    if not exe_path.exists():
        return None, f"Error: {exe_path} not found. Please compile the C++ program first."

    try:
        # Pass command as command-line argument to the executable
        process = subprocess.Popen(
            [str(exe_path), command],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(project_dir)  # Run from the project root for data/ paths
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
        if '>> AGGREGATE ON JOIN:' in line or '>> AGGREGATE:' in line:
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


def parse_generated_sql(output):
    """Parse generated SQL blocks from terminal output.
    Returns a dict mapping dialect name to SQL string.
    """
    if not output:
        return {}

    results = {}
    lines = output.split('\n')
    in_sql_block = False
    current_dialect = None
    current_sql_lines = []

    for line in lines:
        stripped = line.strip()

        if '==================== Generated SQL ====================' in stripped:
            in_sql_block = True
            continue

        if '========================================================' in stripped and in_sql_block:
            # Save the last dialect if any
            if current_dialect and current_sql_lines:
                results[current_dialect] = '\n'.join(current_sql_lines).strip()
            in_sql_block = False
            current_dialect = None
            current_sql_lines = []
            continue

        if in_sql_block:
            # Check for dialect header like [MySQL], [PostgreSQL], etc.
            if stripped.startswith('[') and stripped.endswith(']'):
                # Save previous dialect
                if current_dialect and current_sql_lines:
                    results[current_dialect] = '\n'.join(current_sql_lines).strip()
                current_dialect = stripped[1:-1]
                current_sql_lines = []
            elif current_dialect and stripped:
                current_sql_lines.append(stripped)

    # Catch any trailing dialect
    if current_dialect and current_sql_lines:
        results[current_dialect] = '\n'.join(current_sql_lines).strip()

    return results


def parse_qir(output):
    """Extract the parsed Query Intermediate Representation from engine output."""
    if not output:
        return None

    lines = output.splitlines()
    start_marker = "======================== QIR ========================="
    end_marker = "========================================================"
    try:
        start = next(index for index, line in enumerate(lines) if start_marker in line)
        end = next(
            index for index in range(start + 1, len(lines))
            if end_marker in lines[index]
        )
    except StopIteration:
        return None
    return "\n".join(lines[start + 1:end]).strip()

def load_csv_output(join_type, table1, table2, table3=""):
    """Load the CSV output file generated by the join"""
    project_dir = Path(__file__).resolve().parent.parent
    file_join_type = {"FULL OUTER": "full_outer"}.get(join_type, join_type.lower())
    if table3:
        csv_file = project_dir / "data" / f"output_{file_join_type}_join_3_{table1}_{table2}_{table3}.csv"
    else:
        csv_file = project_dir / "data" / f"output_{file_join_type}_join_{table1}_{table2}.csv"
    if csv_file.exists():
        return pd.read_csv(str(csv_file))
    return None


def load_select_output(table_name):
    """Load the CSV output generated by a filtered single-table query."""
    project_dir = Path(__file__).resolve().parent.parent
    csv_file = project_dir / "data" / f"output_select_{table_name}.csv"
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

        st.subheader("Custom Hinglish Query")
        custom_query = st.text_area(
            "Write a command",
            placeholder="students aur marks ko students.id = marks.student_id par inner join karke dikha",
            height=90,
            key="custom_query",
            label_visibility="collapsed",
        )
        st.caption("Use any supported command: joins, aggregation, or where/order/limit.")
        if st.button("Run Custom Query", use_container_width=True, key="run_custom_query"):
            if not custom_query.strip():
                st.warning("Enter a Hinglish command first.")
            else:
                with st.spinner("Executing custom query..."):
                    stdout, stderr = execute_command(custom_query.strip())
                if stdout is None or (stderr and "Error" in stderr):
                    st.error(f"Execution Error:\n{stderr or 'Command execution failed.'}")
                else:
                    st.session_state.last_output = stdout
                    st.session_state.last_command = custom_query.strip()
                    st.session_state.last_query_mode = "CUSTOM"
                    st.success("Custom query executed successfully!")

        st.divider()

        st.subheader("Filter, Sort, and Limit")
        select_table = st.selectbox("Table", table_names, key="select_table")
        select_columns = list(tables.get(select_table, {}).keys())
        select_col, select_op, select_value = st.columns(3)
        with select_col:
            filter_column = st.selectbox("Filter Column", select_columns, key="filter_column")
        with select_op:
            filter_operator = st.selectbox(
                "Operator", ["=", "!=", ">", "<", ">=", "<="], key="filter_operator"
            )
        with select_value:
            filter_value = st.text_input("Value", key="filter_value")
        order_col, order_dir, limit_col = st.columns(3)
        with order_col:
            order_options = ["(none)"] + select_columns
            order_column = st.selectbox("Sort By", order_options, key="order_column")
        with order_dir:
            order_direction = st.selectbox("Direction", ["ASC", "DESC"], key="order_direction")
        with limit_col:
            select_limit = st.number_input("Limit", min_value=0, value=10, step=1, key="select_limit")

        select_order_clause = "" if order_column == "(none)" else f" order by {order_column} {order_direction.lower()}"
        select_command = (
            f"{select_table} ko where {filter_column} {filter_operator} {filter_value}"
            f"{select_order_clause} limit {select_limit} dikha"
        )
        st.code(select_command, language="text")
        if st.button("Execute Table Query", use_container_width=True, key="execute_table_query"):
            with st.spinner("Executing table query..."):
                stdout, stderr = execute_command(select_command)
                if stderr and "Error" in stderr:
                    st.error(f"Execution Error:\n{stderr}")
                else:
                    st.session_state.last_output = stdout
                    st.session_state.last_command = select_command
                    st.session_state.last_query_mode = "SELECT"
                    st.session_state.last_select_table = select_table
                    st.success("Table query executed successfully!")

        st.divider()

        st.subheader("Single Table Aggregation")
        agg_single_cols = st.columns(3)
        with agg_single_cols[0]:
            single_agg_table = st.selectbox("Table", table_names, key="single_agg_table")
        with agg_single_cols[1]:
            single_agg_columns = list(tables.get(single_agg_table, {}).keys())
            single_agg_col = st.selectbox("Column", single_agg_columns, key="single_agg_col")
        with agg_single_cols[2]:
            single_agg_func = st.selectbox("Function", ["SUM", "AVG", "COUNT", "MIN", "MAX"], key="single_agg_func")

        single_agg_command = (
            f"{single_agg_table} me {single_agg_col} ka "
            f"{single_agg_func.lower()} nikal kar dikha"
        )
        st.code(single_agg_command, language="text")

        if st.button("Execute Aggregation", use_container_width=True):
            with st.spinner("Executing aggregation..."):
                stdout, stderr = execute_command(single_agg_command)

                if stderr and "Error" in stderr:
                    st.error(f"Execution Error:\n{stderr}")
                else:
                    st.session_state.last_output = stdout
                    st.session_state.last_command = single_agg_command
                    st.session_state.last_query_mode = "AGGREGATION"
                    st.success("Aggregation executed successfully!")

        st.divider()
        st.subheader("Join Query")

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
        join_type_options = ["INNER", "LEFT"] if use_third else [
            "INNER", "LEFT", "RIGHT", "FULL OUTER", "CROSS"
        ]
        join_type = st.radio("Join Type", join_type_options, horizontal=True)

        st.divider()
        
        use_agg = st.checkbox(
            "Add Aggregation",
            help="Apply an aggregation function on the joined result."
        )
        agg_table = ""
        agg_col = ""
        agg_func = ""
        if use_agg:
            col_a_t, col_a_c, col_a_f = st.columns(3)
            with col_a_t:
                agg_table_options = [table1, table2, table3] if use_third and table3 else [table1, table2]
                agg_table = st.selectbox("Agg Table", agg_table_options, key="agg_table")
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
        if join_type == "CROSS":
            can_execute = True
        else:
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
                    st.session_state.last_query_mode = "JOIN"
                    st.success("Query executed successfully!")

# ============================================================================
# Main Area - Results Display
# ============================================================================

if "last_output" not in st.session_state:
    st.info("Use the sidebar to build and execute a query")
else:
    # Tabs for different output views
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Results", "QIR", "Generated SQL", "Hinglish Command", "Raw Output"])

    with tab1:
        st.subheader("Query Results")

        if st.session_state.get("last_query_mode") == "AGGREGATION":
            agg_output = parse_aggregation_result(st.session_state.last_output)
            if agg_output:
                st.code(agg_output, language="text")
            else:
                st.warning("Could not parse aggregation output")
        elif st.session_state.get("last_query_mode") == "SELECT":
            df = load_select_output(st.session_state.last_select_table)
            if df is not None:
                st.dataframe(df, use_container_width=True)
                st.info(f"{len(df)} rows returned")
                st.download_button(
                    label="Download as CSV",
                    data=df.to_csv(index=False),
                    file_name="select_result.csv",
                    mime="text/csv"
                )
            else:
                st.warning("Could not load table query output")
        elif st.session_state.get("last_query_mode") == "CUSTOM":
            st.info("Custom query output")
            st.code(st.session_state.last_output, language="text")
        else:
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
        st.subheader("Query Intermediate Representation")
        st.markdown("_The structured command produced by the Hinglish parser._")
        qir = parse_qir(st.session_state.last_output)
        if qir:
            st.code(qir, language="yaml")
        else:
            st.info("Execute a query to see its QIR.")

    with tab3:
        st.subheader("Generated SQL — Multi-Database Translation")
        st.markdown("_Equivalent queries generated via deterministic QIR compilation (no LLM)_")

        generated = parse_generated_sql(st.session_state.last_output)

        if generated:
            dialect_languages = {
                "MySQL": "sql",
                "PostgreSQL": "sql",
                "SQLite": "sql",
                "MongoDB": "javascript",
            }
            cols = st.columns(2)
            for idx, (dialect, sql) in enumerate(generated.items()):
                with cols[idx % 2]:
                    lang = dialect_languages.get(dialect, "sql")
                    st.markdown(f"**{dialect}**")
                    st.code(sql, language=lang)
        else:
            st.info("Execute a query to see generated SQL for all target databases.")

    with tab4:
        st.subheader("Executed Hinglish Command")
        st.code(st.session_state.last_command, language="text")
        st.markdown("""
        **Command Syntax:**
        ```
        <table> me <column> ka <sum|avg|count|min|max> nikal kar dikha
        <table1> aur <table2> ko <table1>.<column1> = <table2>.<column2> par <inner|left> join karke [<table>.<column> ka <func> nikal kar] dikha
        <table1> aur <table2> ko cross join karke dikha
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

    with tab5:
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
        - **Multiple Join Types**: INNER, LEFT, and CROSS joins
        - **Aggregation**: SUM, AVG, COUNT, MIN, and MAX
        - **Custom Queries**: Run any supported Hinglish command directly
        - **Filtering**: WHERE/JAHAN, sorting, and LIMIT controls
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

    **Single Table Aggregation Example:**
    ```
    marks me score ka avg nikal kar dikha
    ```
    Calculates an aggregate directly from one table.

    **CROSS JOIN Example:**
    ```
    students aur courses ko cross join karke dikha
    ```
    Shows the Cartesian product — every student paired with every course.

    **LEFT JOIN with Aggregation Example:**
    ```
    students aur marks ko students.id = marks.student_id par left join karke marks.score ka avg nikal kar dikha
    ```
    Shows all students, and calculates the average string score across all results.
    """)
