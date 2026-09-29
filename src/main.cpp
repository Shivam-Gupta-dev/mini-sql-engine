/*
 * ==========================================================
 *  Hinglish Schema-Aware Join Engine
 *  main.cpp - Entry point
 * ==========================================================
 *
 *  This is the main driver program for the join engine.
 *  It runs an infinite REPL loop that:
 *    1. Takes user input in Hinglish
 *    2. Passes it to the Parser for tokenization
 *    3. Sends the parsed command to the Engine for execution
 *    4. Exits when user types "band karo"
 *
 *  Architecture:
 *    User Input → Parser → ParsedCommand → Engine → Table I/O → Output
 *
 *  This engine is READ-ONLY. It performs only INNER JOIN,
 *  LEFT JOIN, CROSS JOIN, and AGGREGATION on pre-existing .tbl files in the data/ folder.
 *  No create/insert/update/delete operations are supported.
 */

#include <iostream>
#include <string>
#ifdef _WIN32
#include <windows.h>
#endif
#include "parser.h"
#include "engine.h"

using namespace std;

#ifdef _WIN32
class ConsoleInputGuard {
private:
    HANDLE inputHandle;
    DWORD originalMode;
    bool active;

public:
    ConsoleInputGuard() : inputHandle(GetStdHandle(STD_INPUT_HANDLE)), originalMode(0), active(false) {
        if (inputHandle != INVALID_HANDLE_VALUE && GetConsoleMode(inputHandle, &originalMode)) {
            DWORD quietMode = originalMode & ~ENABLE_ECHO_INPUT;
            if (SetConsoleMode(inputHandle, quietMode)) {
                active = true;
            }
        }
    }

    void restore() {
        if (active) {
            FlushConsoleInputBuffer(inputHandle);
            SetConsoleMode(inputHandle, originalMode);
            active = false;
        }
    }

    ~ConsoleInputGuard() {
        restore();
    }
};
#endif

// ---- Display the welcome banner ----
void showBanner() {
    cout << endl;
    cout << "================================================================" << endl;
    cout << "    ___  ___ ____        ____  ____                             " << endl;
    cout << "   / _ \\|_ _|  _ \\      |  _ \\| __ )                            " << endl;
    cout << "  | | | || || |_) |_____| | | |  _ \\                           " << endl;
    cout << "  | |_| || ||  _ <______|_| |_| |_) |                          " << endl;
    cout << "   \\__\\_\\___|_| \\_\\       |____/|____/                           " << endl;
    cout << "                                                               " << endl;
    cout << "================================================================" << endl;
    cout << "  QIR-DB: Query Intermediate Representation Compiler            " << endl;
    cout << "  Multi-Database Translation Engine (Hinglish -> SQL)           " << endl;
    cout << "  Targets: MySQL | PostgreSQL | SQLite | MongoDB                " << endl;
    cout << "  Type 'band karo' to exit.                                     " << endl;
    cout << "  Type 'madad' for supported commands.                          " << endl;
    cout << "================================================================" << endl;
    cout << endl;
}

// ---- Display quick help with supported commands ----
void showHelp() {
    cout << "Supported Commands:" << endl;
    cout << "--------------------" << endl;
    cout << "  General JOIN syntax:" << endl;
    cout << "     <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par <join_type> join karke dikha" << endl;
    cout << "     join_type: inner, left, right, full outer" << endl;
    cout << "     Note: join_type optional hai; 'par join' default INNER JOIN chalata hai." << endl;
    cout << endl;
    cout << "  1. INNER JOIN:" << endl;
    cout << "     Syntax : <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par inner join karke dikha" << endl;
    cout << "     Example: students aur marks ko students.id = marks.student_id par inner join karke dikha" << endl;
    cout << endl;
    cout << "  2. LEFT JOIN:" << endl;
    cout << "     Syntax : <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par left join karke dikha" << endl;
    cout << "     Example: students aur marks ko students.id = marks.student_id par left join karke dikha" << endl;
    cout << endl;
    cout << "  3. RIGHT JOIN / FULL OUTER JOIN:" << endl;
    cout << "     Syntax : <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par right join karke dikha" << endl;
    cout << "     Syntax : <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par full outer join karke dikha" << endl;
    cout << endl;
    cout << "  4. DEFAULT INNER JOIN:" << endl;
    cout << "     Syntax : <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par join karke dikha" << endl;
    cout << "     Example: students aur marks ko students.id = marks.student_id par join karke dikha" << endl;
    cout << endl;
    cout << "  5. CROSS JOIN:" << endl;
    cout << "     Syntax : <t1> aur <t2> ko cross join karke dikha" << endl;
    cout << "     Example: students aur courses ko cross join karke dikha" << endl;
    cout << endl;
    cout << "  6. AGGREGATION ON JOIN:" << endl;
    cout << "     Syntax : <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par <inner|left> join karke <table>.<col> ka <func> nikal kar dikha" << endl;
    cout << "     Syntax : <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par <inner|left> join karke <col> ka <func> nikal kar dikha" << endl;
    cout << "     Functions: sum, avg, count, min, max" << endl;
    cout << "     Example : students aur marks ko students.id = marks.student_id par inner join karke marks.score ka sum nikal kar dikha" << endl;
    cout << "     Example : students aur marks ko students.id = marks.student_id par left join karke score ka avg nikal kar dikha" << endl;
    cout << endl;
    cout << "  7. SINGLE TABLE AGGREGATION:" << endl;
    cout << "     Syntax : <table> me <column> ka <sum|avg|count|min|max> nikal kar dikha" << endl;
    cout << "     Example: marks me score ka avg nikal kar dikha" << endl;
    cout << endl;
    cout << "  8. FILTER / SORT / LIMIT:" << endl;
    cout << "     Syntax : <table> ko where <column> <operator> <value> order by <column> <asc|desc> limit <n> dikha" << endl;
    cout << "     Example: marks ko where score >= 90 order by score desc limit 5 dikha" << endl;
    cout << "     Also supported: jahan, sort; operators =, !=, >, <, >=, <=" << endl;
    cout << endl;
    cout << "  9. THREE TABLE JOIN:" << endl;
    cout << "     Syntax : <t1> aur <t2> aur <t3> ko <t1>.<pk> = <t2>.<fk> aur <t2>.<fk> = <t3>.<pk> par <inner|left> join karke dikha" << endl;
    cout << "     Example: students aur enrollments aur courses ko students.id = enrollments.student_id aur enrollments.course_id = courses.course_id par inner join karke dikha" << endl;
    cout << endl;
    cout << "  10. EXIT:" << endl;
    cout << "     band karo" << endl;
    cout << endl;
    cout << "  11. HELP:" << endl;
    cout << "     madad" << endl;
    cout << endl;
    cout << "  [QIR-DB] Every query also generates equivalent SQL for:" << endl;
    cout << "     MySQL | PostgreSQL | SQLite | MongoDB" << endl;
    cout << "     Generated SQL is printed to terminal and saved to data/ folder." << endl;
    cout << endl;
}

string commandTypeName(CommandType type) {
    switch (type) {
        case CMD_INNER_JOIN: return "INNER_JOIN";
        case CMD_LEFT_JOIN: return "LEFT_JOIN";
        case CMD_RIGHT_JOIN: return "RIGHT_JOIN";
        case CMD_FULL_OUTER_JOIN: return "FULL_OUTER_JOIN";
        case CMD_CROSS_JOIN: return "CROSS_JOIN";
        case CMD_INNER_JOIN_3: return "INNER_JOIN_3";
        case CMD_LEFT_JOIN_3: return "LEFT_JOIN_3";
        case CMD_AGGREGATE: return "AGGREGATE";
        case CMD_SELECT: return "SELECT";
        case CMD_GROUP_AGGREGATE: return "GROUP_AGGREGATE";
        case CMD_EXIT: return "EXIT";
        default: return "UNKNOWN";
    }
}

string aggregationFunctionName(AggregationFunction function) {
    switch (function) {
        case AGG_SUM: return "SUM";
        case AGG_AVG: return "AVG";
        case AGG_COUNT: return "COUNT";
        case AGG_MIN: return "MIN";
        case AGG_MAX: return "MAX";
        default: return "NONE";
    }
}

void showQIR(const ParsedCommand& command) {
    cout << endl;
    cout << "  ======================== QIR =========================" << endl;
    cout << "  type: " << commandTypeName(command.type) << endl;
    if (!command.leftTable.empty()) cout << "  left_table: " << command.leftTable << endl;
    if (!command.rightTable.empty()) cout << "  right_table: " << command.rightTable << endl;
    if (!command.thirdTable.empty()) cout << "  third_table: " << command.thirdTable << endl;
    if (!command.leftColumn.empty()) cout << "  left_column: " << command.leftColumn << endl;
    if (!command.rightColumn.empty()) cout << "  right_column: " << command.rightColumn << endl;
    if (!command.secondJoinTable.empty()) cout << "  second_join_table: " << command.secondJoinTable << endl;
    if (!command.secondLeftColumn.empty()) cout << "  second_left_column: " << command.secondLeftColumn << endl;
    if (!command.thirdColumn.empty()) cout << "  third_column: " << command.thirdColumn << endl;
    if (!command.aggTable.empty()) cout << "  aggregation_table: " << command.aggTable << endl;
    if (!command.aggColumn.empty()) cout << "  aggregation_column: " << command.aggColumn << endl;
    if (!command.groupColumn.empty()) cout << "  group_column: " << command.groupColumn << endl;
    if (command.aggFunc != AGG_NONE) {
        cout << "  aggregation_function: " << aggregationFunctionName(command.aggFunc) << endl;
    }
    if (!command.filterColumn.empty()) {
        cout << "  filter: " << command.filterColumn << " "
             << command.filterOperator << " " << command.filterValue << endl;
    }
    if (!command.orderColumn.empty()) {
        cout << "  order_by: " << command.orderColumn
             << (command.orderDescending ? " DESC" : " ASC") << endl;
    }
    if (command.limit >= 0) cout << "  limit: " << command.limit << endl;
    cout << "  ========================================================" << endl;
}

// ============================================================
//  MAIN FUNCTION — REPL Loop
// ============================================================
int main(int argc, char* argv[]) {
    // Create engine and parser instances
    Engine engine;
    Parser parser;

    // Check if a command is provided as an argument (for programmatic use)
    if (argc > 1) {
        // Single command mode (for Streamlit/external calls)
        string input = argv[1];
        ParsedCommand cmd = parser.parse(input);
        showQIR(cmd);
        engine.execute(cmd);
        return 0;
    }

#ifdef _WIN32
    ConsoleInputGuard startupInputGuard;
#endif

    // Show welcome banner and help only in interactive mode
    showBanner();
    showHelp();

#ifdef _WIN32
    startupInputGuard.restore();
#endif

    // ---- REPL Loop (Read-Eval-Print Loop) ----
    // Continuously reads user input, parses it, and executes
    // the corresponding join operation. Exits on "band karo".
    while (true) {
        cout << "\nHinglishDB> " << flush;
        string input;
        getline(cin, input);

        // Skip empty input
        if (input.empty()) continue;

        // Check for help command
        string lowerInput = input;
        for (auto& c : lowerInput) c = tolower(c);
        if (lowerInput.find("madad") != string::npos) {
            showHelp();
            continue;
        }

        // Parse the input command
        ParsedCommand cmd = parser.parse(input);
        showQIR(cmd);

        // Handle exit
        if (cmd.type == CMD_EXIT) {
            cout << endl;
            cout << "============================================" << endl;
            cout << "  Program band ho raha hai... Alvida!       " << endl;
            cout << "  (Shutting down... Goodbye!)               " << endl;
            cout << "============================================" << endl;
            cout << endl;
            break;
        }

        // Execute the command
        engine.execute(cmd);
    }

    return 0;
}
