/*
 * ==========================================================
 *  Hinglish Schema-Aware Join Engine
 *  parser.h - Parser class declaration
 * ==========================================================
 *
 *  The Parser takes a raw Hinglish command string and
 *  identifies the join type plus all relevant parameters
 *  (table names, join condition columns).
 *
 *  Supported command patterns (case-insensitive):
 *
 *    INNER JOIN:
 *      <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par inner join karke dikha
 *
 *    LEFT JOIN:
 *      <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par left join karke dikha
 *
 *    EXIT:
 *      band karo
 *
 *  The parser uses simple string matching — no external NLP.
 */

#ifndef PARSER_H
#define PARSER_H

#include <string>
#include <vector>

using namespace std;

// Enum representing the type of command identified by the parser
enum CommandType {
    CMD_INNER_JOIN,     // INNER JOIN between two tables
    CMD_LEFT_JOIN,      // LEFT JOIN between two tables
    CMD_EXIT,           // Exit the program
    CMD_UNKNOWN         // Unrecognized command
};

enum AggregationFunction {
    AGG_SUM,
    AGG_AVG,
    AGG_COUNT,
    AGG_MIN,
    AGG_MAX,
    AGG_NONE
};

// Struct holding all parsed information from a command
// For joins: both table names + join condition (tableA.colA = tableB.colB)
// For aggregations: table name, column name, and aggregation function
struct ParsedCommand {
    CommandType type;

    string leftTable;       // Left table name (first table in command)
    string rightTable;      // Right table name (second table in command)

    string leftColumn;      // Left join column (from leftTable)
    string rightColumn;     // Right join column (from rightTable)

    string aggTable;        // Table name for aggregation
    string aggColumn;       // Column name for aggregation
    AggregationFunction aggFunc; // Type of aggregation function
};

class Parser {
public:
    // Parse a raw input string and return a ParsedCommand struct.
    // Converts input to lowercase, extracts tables, join type,
    // and join condition. Returns CMD_UNKNOWN on invalid syntax.
    ParsedCommand parse(const string& input);

private:
    // Helper: convert string to lowercase (for case-insensitive matching)
    string toLower(const string& s);

    // Helper: trim whitespace from both ends
    string trim(const string& s);

    // Helper: tokenize a string by whitespace
    vector<string> tokenize(const string& s);
};

#endif // PARSER_H
