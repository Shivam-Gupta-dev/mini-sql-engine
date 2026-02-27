/*
 * ==========================================================
 *  Hinglish Schema-Aware Join Engine
 *  table.h - Table & Column structure declarations
 * ==========================================================
 *
 *  The Table class represents a single relational table loaded
 *  from a .tbl file in the /data directory.
 *
 *  File format (strict):
 *      #schema:
 *      column_name TYPE [PRIMARY_KEY] [FOREIGN_KEY REFERENCES table(column)]
 *      ...
 *      #data:
 *      val1,val2,val3
 *      ...
 *
 *  Supported types: INT, STRING
 *  Constraints: PRIMARY_KEY, FOREIGN_KEY REFERENCES table(column)
 *
 *  This engine is READ-ONLY — no create/insert/update/delete.
 *  Tables must already exist in the data/ folder.
 */

#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>

using namespace std;

// ---- Column metadata structure ----
// Stores schema information for a single column, including
// name, data type, and constraint flags (PK / FK).
struct Column {
    string name;            // Column name (e.g. "id", "student_id")
    string type;            // Data type: "INT" or "STRING"
    bool isPrimaryKey;      // true if this column is the PRIMARY KEY
    bool isForeignKey;      // true if this column is a FOREIGN KEY
    string refTable;        // Referenced table name (only if FK)
    string refColumn;       // Referenced column name (only if FK)

    Column() : isPrimaryKey(false), isForeignKey(false) {}
};

// ---- Table class ----
// Represents one relational table with full schema metadata
// and row data, loaded from a .tbl file.
class Table {
public:
    string name;                        // Table name (filename without .tbl)
    vector<Column> schema;              // Schema: ordered list of Column structs
    vector<vector<string>> rows;        // Data rows (values stored as strings)

    // ---- Constructor ----
    Table();

    // Load a table from data/<tableName>.tbl
    // Parses both #schema: and #data: sections.
    // Returns true on success, false on file-not-found or corrupt format.
    bool loadFromFile(const string& tableName);

    // Get the index of a column by name (-1 if not found)
    int getColumnIndex(const string& columnName) const;

    // Check whether a column exists in this table
    bool columnExists(const string& columnName) const;

    // Print the full schema metadata to stdout
    void printSchema() const;

    // Return all data rows
    vector<vector<string>> getRows() const;
};

#endif // TABLE_H
