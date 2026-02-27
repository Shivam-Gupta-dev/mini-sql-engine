/*
 * ==========================================================
 *  Hinglish Schema-Aware Join Engine
 *  engine.cpp - Engine class implementation
 * ==========================================================
 *
 *  The Engine executes JOIN operations on pre-existing tables.
 *  It loads tables from data/ .tbl files, validates schema,
 *  and performs nested-loop joins.
 *
 *  Join Algorithms:
 *    INNER JOIN — O(n*m) nested loop:
 *      For each row in A:
 *        For each row in B:
 *          If A[colA] == B[colB] → output merged row
 *
 *    LEFT JOIN — O(n*m) nested loop with NULL padding:
 *      For each row in A:
 *        matched = false
 *        For each row in B:
 *          If A[colA] == B[colB] → output merged row; matched = true
 *        If !matched → output A row + NULL for all B columns
 *
 *  Schema Validation (before any join):
 *    1. Both tables must exist and load successfully
 *    2. Both specified columns must exist in their tables
 *    3. If a column is a foreign key, validate the reference
 *    4. Warn if join is not on a PK-FK relationship
 */

#include "engine.h"
#include <iostream>
#include <iomanip>
#include <algorithm>

// ============================================================
//  loadTable — Load a table from file (with caching)
// ============================================================
// If the table is already in the cache, return it directly.
// Otherwise, load from data/<tableName>.tbl and cache it.
bool Engine::loadTable(const string& tableName, Table& table) {
    // Check cache first
    auto it = tableCache.find(tableName);
    if (it != tableCache.end()) {
        table = it->second;
        return true;
    }

    // Load from file
    if (!table.loadFromFile(tableName)) {
        return false;
    }

    // Cache for future use
    tableCache[tableName] = table;
    return true;
}

// ============================================================
//  validateJoin — Pre-join schema validation
// ============================================================
// Checks:
//   1. Both columns exist in their respective tables
//   2. If a column is a FK, validate the reference matches
//   3. Warn if join is not on a PK-FK pair
bool Engine::validateJoin(const Table& tA, const Table& tB,
                          const string& colA, const string& colB) {
    // Check column existence
    if (!tA.columnExists(colA)) {
        cerr << "[Error] Column '" << colA << "' table '"
             << tA.name << "' mein nahi hai!" << endl;
        return false;
    }
    if (!tB.columnExists(colB)) {
        cerr << "[Error] Column '" << colB << "' table '"
             << tB.name << "' mein nahi hai!" << endl;
        return false;
    }

    int idxA = tA.getColumnIndex(colA);
    int idxB = tB.getColumnIndex(colB);
    const Column& cA = tA.schema[idxA];
    const Column& cB = tB.schema[idxB];

    // FK reference validation: if colB is a foreign key referencing tA,
    // check that the reference table and column match.
    if (cB.isForeignKey) {
        if (cB.refTable != tA.name || cB.refColumn != colA) {
            cerr << "[Warning] Foreign key '" << tB.name << "." << colB
                 << "' references '" << cB.refTable << "(" << cB.refColumn
                 << ")' but join is on '" << tA.name << "." << colA << "'." << endl;
        }
    }
    // Also check the reverse direction
    if (cA.isForeignKey) {
        if (cA.refTable != tB.name || cA.refColumn != colB) {
            cerr << "[Warning] Foreign key '" << tA.name << "." << colA
                 << "' references '" << cA.refTable << "(" << cA.refColumn
                 << ")' but join is on '" << tB.name << "." << colB << "'." << endl;
        }
    }

    // Warn if join is not on a PK-FK relationship
    bool pkfk = false;
    if (cA.isPrimaryKey && cB.isForeignKey) pkfk = true;
    if (cB.isPrimaryKey && cA.isForeignKey) pkfk = true;

    if (!pkfk) {
        cout << "[Info] Yeh join PK-FK relationship par nahi hai. Results unexpected ho sakte hain." << endl;
    }

    return true;
}

// ============================================================
//  printSeparator — Print a horizontal line
// ============================================================
void Engine::printSeparator(int totalWidth) {
    cout << "  " << string(totalWidth, '-') << endl;
}

// ============================================================
//  printJoinHeader — Print the combined column header
// ============================================================
// Format: tableA.col1 | tableA.col2 | ... | tableB.col1 | ...
// Each column cell is 20 chars wide for alignment.
void Engine::printJoinHeader(const Table& tA, const Table& tB) {
    int colWidth = 20;
    int totalCols = (int)(tA.schema.size() + tB.schema.size());
    int totalWidth = totalCols * (colWidth + 3) + 1;

    cout << endl;
    printSeparator(totalWidth);

    cout << "  |";
    // Table A columns
    for (const auto& col : tA.schema) {
        string header = tA.name + "." + col.name;
        cout << " " << left << setw(colWidth) << header << "|";
    }
    // Table B columns
    for (const auto& col : tB.schema) {
        string header = tB.name + "." + col.name;
        cout << " " << left << setw(colWidth) << header << "|";
    }
    cout << endl;

    printSeparator(totalWidth);
}

// ============================================================
//  printJoinRow — Print a merged row (both tables have values)
// ============================================================
void Engine::printJoinRow(const Table& /*tA*/, const vector<string>& rowA,
                          const Table& /*tB*/, const vector<string>& rowB) {
    int colWidth = 20;
    cout << "  |";

    for (const auto& val : rowA) {
        cout << " " << left << setw(colWidth) << val << "|";
    }
    for (const auto& val : rowB) {
        cout << " " << left << setw(colWidth) << val << "|";
    }
    cout << endl;
}

// ============================================================
//  printLeftNullRow — Print a row where B side is all NULLs
// ============================================================
// Used in LEFT JOIN when no match is found for a row in table A.
void Engine::printLeftNullRow(const Table& /*tA*/, const vector<string>& rowA,
                              const Table& tB) {
    int colWidth = 20;
    cout << "  |";

    for (const auto& val : rowA) {
        cout << " " << left << setw(colWidth) << val << "|";
    }
    // Fill B columns with NULL
    for (size_t i = 0; i < tB.schema.size(); i++) {
        cout << " " << left << setw(colWidth) << "NULL" << "|";
    }
    cout << endl;
}

// ============================================================
//  execute — Route a parsed command to the appropriate method
// ============================================================
void Engine::execute(const ParsedCommand& cmd) {
    switch (cmd.type) {
        case CMD_INNER_JOIN:
            innerJoin(cmd.leftTable, cmd.rightTable,
                      cmd.leftColumn, cmd.rightColumn);
            break;

        case CMD_LEFT_JOIN:
            leftJoin(cmd.leftTable, cmd.rightTable,
                     cmd.leftColumn, cmd.rightColumn);
            break;

        case CMD_EXIT:
            // Handled in main.cpp
            break;

        case CMD_UNKNOWN:
            cerr << "[Error] Command samajh nahi aaya! Check your syntax." << endl;
            cout << "\nSupported commands:" << endl;
            cout << "  INNER JOIN: <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par inner join karke dikha" << endl;
            cout << "  LEFT JOIN : <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par left join karke dikha" << endl;
            cout << "  EXIT      : band karo" << endl;
            break;
    }
}

// ============================================================
//  INNER JOIN
// ============================================================
//  Algorithm (Nested Loop Join):
//    For each row rA in tableA:
//      For each row rB in tableB:
//        If rA[colA] == rB[colB]:
//          Output merged row: [all A columns] + [all B columns]
//
//  Time complexity: O(|A| * |B|)
// ============================================================
void Engine::innerJoin(const string& tableA, const string& tableB,
                       const string& colA, const string& colB) {
    // Load both tables
    Table tA, tB;
    if (!loadTable(tableA, tA)) {
        cerr << "[Error] Table '" << tableA << "' load nahi ho saka!" << endl;
        return;
    }
    if (!loadTable(tableB, tB)) {
        cerr << "[Error] Table '" << tableB << "' load nahi ho saka!" << endl;
        return;
    }

    // Print schema info for both tables
    cout << "\n[Info] Loading schemas..." << endl;
    tA.printSchema();
    tB.printSchema();

    // Validate join columns and schema
    if (!validateJoin(tA, tB, colA, colB)) {
        return;
    }

    // Get column indices
    int idxA = tA.getColumnIndex(colA);
    int idxB = tB.getColumnIndex(colB);

    cout << "\n>> INNER JOIN: " << tableA << "." << colA
         << " = " << tableB << "." << colB << endl;

    // Print header
    printJoinHeader(tA, tB);

    int colWidth = 20;
    int totalCols = (int)(tA.schema.size() + tB.schema.size());
    int totalWidth = totalCols * (colWidth + 3) + 1;

    // Nested loop join
    int matchCount = 0;
    for (const auto& rowA : tA.rows) {
        for (const auto& rowB : tB.rows) {
            // Join condition: compare values at specified columns
            if (rowA[idxA] == rowB[idxB]) {
                printJoinRow(tA, rowA, tB, rowB);
                matchCount++;
            }
        }
    }

    printSeparator(totalWidth);
    cout << "\n  Total matched rows: " << matchCount << endl;

    if (matchCount == 0) {
        cout << "  Koi matching row nahi mila! (No matching rows found)" << endl;
    }
    cout << endl;
}

// ============================================================
//  LEFT JOIN
// ============================================================
//  Algorithm (Nested Loop Left Join):
//    For each row rA in tableA:
//      matched = false
//      For each row rB in tableB:
//        If rA[colA] == rB[colB]:
//          Output merged row: [all A columns] + [all B columns]
//          matched = true
//      If !matched:
//        Output: [all A columns] + [NULL for each B column]
//
//  This ensures every row from the left table appears in output.
//  Time complexity: O(|A| * |B|)
// ============================================================
void Engine::leftJoin(const string& tableA, const string& tableB,
                      const string& colA, const string& colB) {
    // Load both tables
    Table tA, tB;
    if (!loadTable(tableA, tA)) {
        cerr << "[Error] Table '" << tableA << "' load nahi ho saka!" << endl;
        return;
    }
    if (!loadTable(tableB, tB)) {
        cerr << "[Error] Table '" << tableB << "' load nahi ho saka!" << endl;
        return;
    }

    // Print schema info for both tables
    cout << "\n[Info] Loading schemas..." << endl;
    tA.printSchema();
    tB.printSchema();

    // Validate join columns and schema
    if (!validateJoin(tA, tB, colA, colB)) {
        return;
    }

    // Get column indices
    int idxA = tA.getColumnIndex(colA);
    int idxB = tB.getColumnIndex(colB);

    cout << "\n>> LEFT JOIN: " << tableA << "." << colA
         << " = " << tableB << "." << colB << endl;

    // Print header
    printJoinHeader(tA, tB);

    int colWidth = 20;
    int totalCols = (int)(tA.schema.size() + tB.schema.size());
    int totalWidth = totalCols * (colWidth + 3) + 1;

    // Nested loop left join
    int totalRows = 0;
    for (const auto& rowA : tA.rows) {
        bool matched = false;

        for (const auto& rowB : tB.rows) {
            if (rowA[idxA] == rowB[idxB]) {
                printJoinRow(tA, rowA, tB, rowB);
                matched = true;
                totalRows++;
            }
        }

        // If no match found for this left row, pad B side with NULLs
        if (!matched) {
            printLeftNullRow(tA, rowA, tB);
            totalRows++;
        }
    }

    printSeparator(totalWidth);
    cout << "\n  Total result rows: " << totalRows << endl;
    cout << endl;
}
