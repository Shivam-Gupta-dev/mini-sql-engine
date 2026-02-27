/*
 * ==========================================================
 *  Hinglish Schema-Aware Join Engine
 *  engine.h - Engine class declaration
 * ==========================================================
 *
 *  The Engine class is the core execution layer.
 *  It receives parsed commands and performs JOIN operations
 *  on pre-existing tables loaded from the data/ folder.
 *
 *  Supported operations:
 *    - INNER JOIN (nested-loop)
 *    - LEFT JOIN  (nested-loop with NULL padding)
 *
 *  Schema validation is performed before every join:
 *    - Both tables must exist
 *    - Both columns must exist
 *    - FK reference consistency is checked
 */

#ifndef ENGINE_H
#define ENGINE_H

#include <string>
#include <vector>
#include <map>
#include "table.h"
#include "parser.h"

using namespace std;

class Engine {
public:
    // Execute a fully parsed command (routes to join or exit)
    void execute(const ParsedCommand& cmd);

    // ---- Join operations ----

    // INNER JOIN: For each row in A, for each row in B,
    // if A[colA] == B[colB], print the merged row.
    void innerJoin(const string& tableA, const string& tableB,
                   const string& colA, const string& colB);

    // LEFT JOIN: For each row in A,
    //   if match found in B → print merged row
    //   else → print A row + NULL for all B columns
    void leftJoin(const string& tableA, const string& tableB,
                  const string& colA, const string& colB);

private:
    // Cache of loaded tables to avoid re-reading files
    map<string, Table> tableCache;

    // Load a table (from cache or from file). Returns true on success.
    bool loadTable(const string& tableName, Table& table);

    // Validate that both tables and columns exist before joining.
    // Also warns if join is not on a PK-FK relationship.
    bool validateJoin(const Table& tA, const Table& tB,
                      const string& colA, const string& colB);

    // Print the combined header row: tableA.col1 | tableA.col2 | ... | tableB.col1 | ...
    void printJoinHeader(const Table& tA, const Table& tB);

    // Print a merged data row from two tables
    void printJoinRow(const Table& tA, const vector<string>& rowA,
                      const Table& tB, const vector<string>& rowB);

    // Print a left-join row where B has no match (NULLs for B columns)
    void printLeftNullRow(const Table& tA, const vector<string>& rowA,
                          const Table& tB);

    // Print a horizontal separator line
    void printSeparator(int totalWidth);
};

#endif // ENGINE_H
