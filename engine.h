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
 *    - AGGREGATION (sum, avg, count, min, max on a single column)
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
#include <fstream>
#include "table.h"
#include "parser.h"

using namespace std;

class Engine
{
public:
    // Execute a fully parsed command (routes to join or exit)
    void execute(const ParsedCommand &cmd);

    // INNER JOIN: For each row in A, for each row in B,
    // if A[colA] == B[colB], print the merged row.
    void innerJoin(const string &tableA, const string &tableB,
                   const string &colA, const string &colB,
                   const string &aggTable, const string &aggCol, AggregationFunction aggFunc);

    // LEFT JOIN: For each row in A,
    //   if match found in B → print merged row
    //   else → print A row + NULL for all B columns
    void leftJoin(const string &tableA, const string &tableB,
                  const string &colA, const string &colB,
                  const string &aggTable, const string &aggCol, AggregationFunction aggFunc);

    // ---- Aggregation operation ----
    void aggregate(const string &tableName, const string &columnName, AggregationFunction func);

private:
    // Cache of loaded tables to avoid re-reading files
    map<string, Table> tableCache;

    // Process aggregation on joined rows
    void processJoinAggregation(const vector<vector<string>> &resultRows,
                                const Table &tA, const Table &tB,
                                const string &aggTable, const string &aggCol,
                                AggregationFunction aggFunc);

    // Load a table (from cache or from file). Returns true on success.
    bool loadTable(const string &tableName, Table &table);

    // Validate that both tables and columns exist before joining.
    // Also warns if join is not on a PK-FK relationship.
    bool validateJoin(const Table &tA, const Table &tB,
                      const string &colA, const string &colB);

    // Save join results as a clean CSV file (header + data rows)
    void saveJoinOutput(const string &fileName, const Table &tA, const Table &tB,
                        const string &colA, const string &colB,
                        const string &joinType,
                        const vector<vector<string>> &resultRows);

    // Save join results as a human-readable TXT file (with metadata header)
    void saveTextOutput(const string &fileName, const Table &tA, const Table &tB,
                        const string &colA, const string &colB,
                        const string &joinType,
                        const vector<vector<string>> &resultRows);

    // Save aggregation results to output files
    void saveAggregateOutput(const string &fileName, const string &tableName,
                             const string &colName, const string &funcStr,
                             double result);

    // Print a complete joined table with widths based on headers and result values.
    int printJoinTable(const Table &tA, const Table &tB,
                       const vector<vector<string>> &resultRows);

    // Print a horizontal separator line
    void printSeparator(int totalWidth);

    // Legacy row printers retained for focused terminal formatting helpers.
    void printJoinRow(const Table &tA, const vector<string> &rowA,
                      const Table &tB, const vector<string> &rowB);
    void printLeftNullRow(const Table &tA, const vector<string> &rowA,
                          const Table &tB);
};

#endif // ENGINE_H
