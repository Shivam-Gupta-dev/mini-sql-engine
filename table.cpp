/*
 * ==========================================================
 *  Hinglish Schema-Aware Join Engine
 *  table.cpp - Table class implementation
 * ==========================================================
 *
 *  This file implements all Table operations:
 *    - Loading a table from a .tbl file (with #schema: and #data: sections)
 *    - Parsing column metadata (name, type, PK, FK references)
 *    - Column index lookup and existence checks
 *    - Schema printing
 *
 *  File format expected:
 *    #schema:
 *    column_name TYPE [PRIMARY_KEY] [FOREIGN_KEY REFERENCES table(column)]
 *    ...
 *    #data:
 *    val1,val2,val3
 *    ...
 */

#include "table.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <algorithm>

// ---- Constructor ----
Table::Table() : name("") {}

// ============================================================
//  loadFromFile — Parse a .tbl file into schema + data
// ============================================================
// Algorithm:
//   1. Open data/<tableName>.tbl
//   2. Read line-by-line; detect #schema: and #data: markers
//   3. In schema section, parse each line for column definition:
//      column_name TYPE [PRIMARY_KEY] [FOREIGN_KEY REFERENCES table(column)]
//   4. In data section, split each line by comma into row values
// ============================================================
bool Table::loadFromFile(const string &tableName)
{
    name = tableName;
    schema.clear();
    rows.clear();

    // Build file path: data/<tableName>.tbl
    // If the executable is launched from the parent project folder, fall back
    // to HinglishDB/data/<tableName>.tbl.
    string filePath = "data/" + tableName + ".tbl";
    ifstream file(filePath);
    if (!file.is_open())
    {
        string fallbackPath = "HinglishDB/data/" + tableName + ".tbl";
        file.open(fallbackPath);
        if (file.is_open())
        {
            filePath = fallbackPath;
        }
    }
    if (!file.is_open())
    {
        cerr << "[Error] Table file '" << filePath << "' nahi mila! (File not found)" << endl;
        return false;
    }

    string line;
    bool inSchema = false; // Currently inside #schema: section
    bool inData = false;   // Currently inside #data: section

    while (getline(file, line))
    {
        // Trim trailing whitespace / carriage return
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n' ||
                                 line.back() == ' ' || line.back() == '\t'))
        {
            line.pop_back();
        }
        // Skip empty lines
        if (line.empty())
            continue;

        // ---- Detect section markers ----
        if (line == "#schema:")
        {
            inSchema = true;
            inData = false;
            continue;
        }
        if (line == "#data:")
        {
            inSchema = false;
            inData = true;
            continue;
        }

        // ---- Parse schema line ----
        // Format: column_name TYPE [PRIMARY_KEY] [FOREIGN_KEY REFERENCES table(column)]
        if (inSchema)
        {
            Column col;
            istringstream iss(line);
            vector<string> tokens;
            string token;
            while (iss >> token)
            {
                tokens.push_back(token);
            }

            if (tokens.size() < 2)
            {
                cerr << "[Error] Schema line corrupt: '" << line << "'" << endl;
                file.close();
                return false;
            }

            col.name = tokens[0]; // Column name
            col.type = tokens[1]; // Data type (INT / STRING)

            // Scan remaining tokens for constraints
            for (size_t i = 2; i < tokens.size(); i++)
            {
                if (tokens[i] == "PRIMARY_KEY")
                {
                    col.isPrimaryKey = true;
                }
                else if (tokens[i] == "FOREIGN_KEY")
                {
                    col.isForeignKey = true;
                    // Expect: FOREIGN_KEY REFERENCES table(column)
                    if (i + 2 < tokens.size() && tokens[i + 1] == "REFERENCES")
                    {
                        // Parse "table(column)"
                        string ref = tokens[i + 2];
                        size_t parenOpen = ref.find('(');
                        size_t parenClose = ref.find(')');
                        if (parenOpen != string::npos && parenClose != string::npos &&
                            parenClose > parenOpen)
                        {
                            col.refTable = ref.substr(0, parenOpen);
                            col.refColumn = ref.substr(parenOpen + 1,
                                                       parenClose - parenOpen - 1);
                        }
                        i += 2; // Skip "REFERENCES" and "table(column)"
                    }
                }
            }

            schema.push_back(col);
        }

        // ---- Parse data line ----
        // Comma-separated values, stored as strings
        if (inData)
        {
            vector<string> row;
            stringstream ss(line);
            string value;
            while (getline(ss, value, ','))
            {
                // Trim whitespace from value
                size_t start = value.find_first_not_of(" \t");
                size_t end = value.find_last_not_of(" \t");
                if (start != string::npos)
                {
                    row.push_back(value.substr(start, end - start + 1));
                }
                else
                {
                    row.push_back("");
                }
            }

            // Validate row has correct number of columns
            if (row.size() != schema.size())
            {
                cerr << "[Warning] Data row has " << row.size() << " values but schema has "
                     << schema.size() << " columns. Skipping row." << endl;
                continue;
            }

            rows.push_back(row);
        }
    }

    file.close();

    // Validate that we got at least a schema
    if (schema.empty())
    {
        cerr << "[Error] Table '" << tableName << "' ka schema khaali hai! (Empty schema)" << endl;
        return false;
    }

    return true;
}

// ============================================================
//  getColumnIndex — Return index of column by name (-1 if not found)
// ============================================================
int Table::getColumnIndex(const string &columnName) const
{
    for (size_t i = 0; i < schema.size(); i++)
    {
        if (schema[i].name == columnName)
        {
            return static_cast<int>(i);
        }
    }
    return -1;
}

// ============================================================
//  columnExists — Check if a column exists in the schema
// ============================================================
bool Table::columnExists(const string &columnName) const
{
    return getColumnIndex(columnName) != -1;
}

// ============================================================
//  printSchema — Display the schema metadata for this table
// ============================================================
void Table::printSchema() const
{
    cout << "\n  Schema for table: " << name << endl;
    cout << "  " << string(60, '-') << endl;
    cout << "  " << left << setw(15) << "Column"
         << setw(10) << "Type"
         << setw(15) << "Constraint"
         << "Reference" << endl;
    cout << "  " << string(60, '-') << endl;

    for (const auto &col : schema)
    {
        cout << "  " << left << setw(15) << col.name
            << setw(10) << col.type;

        if (col.isPrimaryKey)
        {
            cout << setw(15) << "PRIMARY_KEY";
        }
        else if (col.isForeignKey)
        {
            cout << setw(15) << "FOREIGN_KEY";
        }
        else
        {
            cout << setw(15) << "-";
        }

        if (col.isForeignKey)
        {
            cout << col.refTable << "(" << col.refColumn << ")";
        }
        else
        {
            cout << "-";
        }

        cout << endl;
    }
    cout << "  " << string(60, '-') << endl;
    cout << "  Total rows: " << rows.size() << endl;
    cout << endl;
}

// ============================================================
//  getRows — Return all data rows
// ============================================================
vector<vector<string>> Table::getRows() const
{
    return rows;
}



// schema = [
//     Column {
//         name: "attendance_id",
//         type: "INT",
//         isPrimaryKey: true,
//         isForeignKey: false,
//         refTable: "",
//         refColumn: ""
//     },
//     Column {
//         name: "student_id",
//         type: "INT",
//         isPrimaryKey: false,
//         isForeignKey: true,
//         refTable: "students",
//         refColumn: "id"
//     },
//     Column {
//         name: "course_id",
//         type: "INT",
//         isPrimaryKey: false,
//         isForeignKey: true,
//         refTable: "courses",
//         refColumn: "course_id"
//     },
//     Column {
//         name: "attendance_percentage",
//         type: "INT",
//         isPrimaryKey: false,
//         isForeignKey: false,
//         refTable: "",
//         refColumn: ""
//     },
//     Column {
//         name: "month",
//         type: "INT",
//         isPrimaryKey: false,
//         isForeignKey: false,
//         refTable: "",
//         refColumn: ""
//     }
// ]



// rows = [
//     ["1", "1", "1", "90", "1"],
//     ["2", "1", "2", "85", "1"],
//     ["3", "2", "3", "92", "2"],
//     ["4", "2", "4", "88", "2"],
//     ["5", "3", "5", "95", "1"],
//     ["6", "1", "6", "78", "2"],
//     ["7", "2", "7", "91", "1"],
//     ["8", "3", "8", "87", "2"],
//     ["9", "1", "9", "80", "1"],
//     ["10", "2", "10", "93", "2"]
// ]