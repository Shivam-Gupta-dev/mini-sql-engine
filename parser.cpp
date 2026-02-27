/*
 * ==========================================================
 *  Hinglish Schema-Aware Join Engine
 *  parser.cpp - Parser class implementation
 * ==========================================================
 *
 *  The parser uses simple string matching and tokenization
 *  to identify Hinglish join commands. No external NLP
 *  libraries are used — just standard C++ string operations.
 *
 *  Command patterns are matched by looking for specific
 *  Hindi/Hinglish keywords in the input string.
 *
 *  Expected command format:
 *    <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par <inner|left> join karke dikha
 *
 *  Parsing steps:
 *    1. Convert entire input to lowercase
 *    2. Check for "band karo" (exit)
 *    3. Look for "aur" to identify the two table names
 *    4. Look for "par" to locate the join condition
 *    5. Parse "table.column = table.column" between "ko" and "par"
 *    6. Detect join type: "inner join" or "left join"
 *    7. Validate that "karke dikha" is present
 */

#include "parser.h"
#include <sstream>
#include <algorithm>
#include <iostream>

// ---- Helper: Convert string to lowercase ----
string Parser::toLower(const string& s) {
    string result = s;
    transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
}

// ---- Helper: Trim whitespace from both ends ----
string Parser::trim(const string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    size_t end = s.find_last_not_of(" \t\r\n");
    if (start == string::npos) return "";
    return s.substr(start, end - start + 1);
}

// ---- Helper: Tokenize string by whitespace ----
vector<string> Parser::tokenize(const string& s) {
    vector<string> tokens;
    stringstream ss(s);
    string token;
    while (ss >> token) {
        tokens.push_back(token);
    }
    return tokens;
}

// ============================================================
//  Main parse function
// ============================================================
// Analyzes the input string and returns a ParsedCommand struct.
//
// Supported patterns:
//   1. "band karo" → CMD_EXIT
//   2. "<t1> aur <t2> ko <t1>.<c1> = <t2>.<c2> par inner join karke dikha"
//      → CMD_INNER_JOIN
//   3. "<t1> aur <t2> ko <t1>.<c1> = <t2>.<c2> par left join karke dikha"
//      → CMD_LEFT_JOIN
//
// The parser extracts:
//   - leftTable, rightTable
//   - leftColumn, rightColumn (from the join condition)
//   - Join type (inner or left)
// ============================================================
ParsedCommand Parser::parse(const string& input) {
    ParsedCommand cmd;
    cmd.type = CMD_UNKNOWN;

    string trimmed = trim(input);
    if (trimmed.empty()) {
        return cmd;
    }

    // Convert to lowercase for case-insensitive matching
    string lower = toLower(trimmed);

    // ============================================================
    // COMMAND: EXIT — "band karo"
    // ============================================================
    if (lower.find("band karo") != string::npos) {
        cmd.type = CMD_EXIT;
        return cmd;
    }

    // ============================================================
    // COMMAND: JOIN — "<t1> aur <t2> ko <cond> par <type> join karke dikha"
    // ============================================================
    // Step 1: Must contain "aur", "ko", "par", "join", "karke", "dikha"
    if (lower.find("aur") == string::npos ||
        lower.find("ko") == string::npos ||
        lower.find("par") == string::npos ||
        lower.find("join") == string::npos ||
        lower.find("karke") == string::npos ||
        lower.find("dikha") == string::npos) {
        cerr << "[Error] Invalid syntax! Yeh command samajh nahi aaya." << endl;
        cerr << "Expected format:" << endl;
        cerr << "  <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par inner join karke dikha" << endl;
        cerr << "  <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par left join karke dikha" << endl;
        return cmd;
    }

    // Step 2: Tokenize the lowercase input
    vector<string> tokens = tokenize(lower);

    // Step 3: Find positions of key tokens
    int aurPos = -1, koPos = -1, parPos = -1;
    int joinPos = -1;

    for (int i = 0; i < (int)tokens.size(); i++) {
        if (tokens[i] == "aur" && aurPos == -1) aurPos = i;
        else if (tokens[i] == "ko" && koPos == -1) koPos = i;
        // Find the last "par" before "join" (the one that separates condition from join type)
        else if (tokens[i] == "join") joinPos = i;
    }

    // Find "par" — should be immediately before "inner/left join"
    for (int i = 0; i < (int)tokens.size(); i++) {
        if (tokens[i] == "par") parPos = i;
    }

    // Validate positions
    if (aurPos < 0 || koPos < 0 || parPos < 0 || joinPos < 0) {
        cerr << "[Error] Join command mein keywords missing hain!" << endl;
        return cmd;
    }

    if (aurPos == 0) {
        cerr << "[Error] Left table name missing (before 'aur')!" << endl;
        return cmd;
    }

    if (aurPos + 1 >= koPos) {
        cerr << "[Error] Right table name missing (between 'aur' and 'ko')!" << endl;
        return cmd;
    }

    // Step 4: Extract table names
    // Left table = token before "aur"
    cmd.leftTable = tokens[aurPos - 1];
    // Right table = token after "aur"
    cmd.rightTable = tokens[aurPos + 1];

    // Step 5: Extract join condition between "ko" and "par"
    // Expected format: <table>.<col> = <table>.<col>
    // This should be tokens from koPos+1 to parPos-1
    if (koPos + 3 > parPos) {
        cerr << "[Error] Join condition incomplete! Expected: table.col = table.col" << endl;
        return cmd;
    }

    // The condition tokens are: tokens[koPos+1] "=" tokens[koPos+3]
    // Or: tokens[koPos+1] = "table.col", tokens[koPos+2] = "=", tokens[koPos+3] = "table.col"
    string leftCond = tokens[koPos + 1];
    string rightCond;

    // Find "=" sign in condition
    int eqPos = -1;
    for (int i = koPos + 1; i < parPos; i++) {
        if (tokens[i] == "=") {
            eqPos = i;
            break;
        }
    }

    if (eqPos < 0) {
        cerr << "[Error] '=' sign nahi mila join condition mein!" << endl;
        return cmd;
    }

    leftCond = tokens[eqPos - 1];  // table.col on left side of =
    rightCond = tokens[eqPos + 1]; // table.col on right side of =

    // Parse "table.column" format
    size_t dotLeft = leftCond.find('.');
    size_t dotRight = rightCond.find('.');

    if (dotLeft == string::npos || dotRight == string::npos) {
        cerr << "[Error] Join condition mein dot (.) notation use karo!" << endl;
        cerr << "  Example: students.id = marks.student_id" << endl;
        return cmd;
    }

    // Extract column names (part after the dot)
    cmd.leftColumn = leftCond.substr(dotLeft + 1);
    cmd.rightColumn = rightCond.substr(dotRight + 1);

    // Also validate that the table names in condition match the declared tables
    string condLeftTable = leftCond.substr(0, dotLeft);
    string condRightTable = rightCond.substr(0, dotRight);

    if ((condLeftTable != cmd.leftTable || condRightTable != cmd.rightTable) &&
        (condLeftTable != cmd.rightTable || condRightTable != cmd.leftTable)) {
        cerr << "[Warning] Join condition ke table names (" << condLeftTable << ", "
             << condRightTable << ") declared tables (" << cmd.leftTable << ", "
             << cmd.rightTable << ") se match nahi karte!" << endl;
    }

    // If the condition's table order is swapped relative to the declared order,
    // swap the columns accordingly
    if (condLeftTable == cmd.rightTable && condRightTable == cmd.leftTable) {
        swap(cmd.leftColumn, cmd.rightColumn);
    }

    // Step 6: Determine join type — look at the token before "join"
    if (joinPos > 0) {
        string joinModifier = tokens[joinPos - 1];
        if (joinModifier == "inner") {
            cmd.type = CMD_INNER_JOIN;
        } else if (joinModifier == "left") {
            cmd.type = CMD_LEFT_JOIN;
        } else if (joinModifier == "par") {
            // If "par" is right before "join", default to INNER JOIN
            cmd.type = CMD_INNER_JOIN;
        } else {
            cerr << "[Error] Unknown join type: '" << joinModifier << "'. Use 'inner' or 'left'." << endl;
            return cmd;
        }
    }

    // Step 7: Validate "karke dikha" is present at the end
    if (lower.find("karke dikha") == string::npos) {
        cerr << "[Error] Command ke end mein 'karke dikha' likho!" << endl;
        cmd.type = CMD_UNKNOWN;
        return cmd;
    }

    return cmd;
}
