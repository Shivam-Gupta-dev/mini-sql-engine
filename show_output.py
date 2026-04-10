"""
==========================================================
 Hinglish Schema-Aware Join Engine
 show_output.py - CSV Output Viewer (using pandas)
==========================================================

 This script reads a join output CSV file from the data/
 folder and displays it as a nicely formatted table using
 the pandas library.

 Usage:
   python show_output.py
   python show_output.py <filename.csv>

 If no filename is given, it lists all available CSV files
 in data/ and lets you pick one.

 Requirements:
   pip install pandas tabulate
"""

import os
import sys

try:
    import pandas as pd
except ImportError:
    print("[Error] pandas library not installed!")
    print("Run: pip install pandas tabulate")
    sys.exit(1)

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False


def get_csv_files():
    """Find all CSV output files in the data/ folder."""
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(data_dir):
        print("[Error] 'data/' folder nahi mila!")
        return []
    csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    csv_files.sort()
    return csv_files


def print_table(filepath):
    """Read a CSV file using pandas and display it as a formatted table."""
    if not os.path.exists(filepath):
        print(f"[Error] File '{filepath}' nahi mila!")
        return

    # Read CSV into a pandas DataFrame
    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"[Error] CSV read failed: {e}")
        return

    if df.empty:
        print("[Error] File is empty!")
        return

    filename = os.path.basename(filepath)

    # Display file info
    print()
    print(f"  File : {filename}")
    print(f"  Rows : {len(df)}")
    print(f"  Cols : {len(df.columns)}")
    print()

    # Replace NaN with "NULL" for display
    df = df.fillna("NULL")

    # Display as a formatted table using tabulate (if available) or pandas
    if HAS_TABULATE:
        table_str = tabulate(df, headers="keys", tablefmt="psql",
                             showindex=False, numalign="left")
        print(table_str)
    else:
        # Fallback: use pandas to_string with nice formatting
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", None)
        pd.set_option("display.max_colwidth", None)
        print(df.to_string(index=False))

    print(f"\n  Total rows: {len(df)}")
    print()


def main():
    # If a filename is passed as argument, show that file directly
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
        # If just a filename (no path), look in data/
        if os.path.sep not in filepath and "/" not in filepath:
            filepath = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "data", filepath
            )
        print_table(filepath)
        return

    # Otherwise, list available CSV files and let user pick
    csv_files = get_csv_files()
    if not csv_files:
        print("Koi CSV output file nahi mila! Pehle join command run karo.")
        return

    print("\n  Available join output files:")
    print("  " + "-" * 50)
    for i, f in enumerate(csv_files, 1):
        print(f"  {i}. {f}")
    print("  " + "-" * 50)

    try:
        choice = input("\n  File number select karo (or 'q' to quit): ").strip()
        if choice.lower() == "q":
            return
        idx = int(choice) - 1
        if 0 <= idx < len(csv_files):
            data_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "data"
            )
            filepath = os.path.join(data_dir, csv_files[idx])
            print_table(filepath)
        else:
            print("[Error] Invalid choice!")
    except (ValueError, EOFError):
        print("[Error] Invalid input!")


if __name__ == "__main__":
    main()
