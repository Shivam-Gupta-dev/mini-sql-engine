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
 *  LEFT JOIN, and AGGREGATION on pre-existing .tbl files in the data/ folder.
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
    cout << "   _   _ _             _ _     _        _       _              " << endl;
    cout << "  | | | (_)_ __   __ _| (_)___| |__    | | ___ (_)_ __        " << endl;
    cout << "  | |_| | | '_ \\ / _` | | / __| '_ \\   | |/ _ \\| | '_ \\      " << endl;
    cout << "  |  _  | | | | | (_| | | \\__ \\ | | |  | | (_) | | | | |     " << endl;
    cout << "  |_| |_|_|_| |_|\\__, |_|_|___/_| |_|  |_|\\___/|_|_| |_|     " << endl;
    cout << "                 |___/                                         " << endl;
    cout << "================================================================" << endl;
    cout << "  Hinglish Schema-Aware Join Engine                            " << endl;
    cout << "  File-Based Mini Relational Processor in C++                  " << endl;
    cout << "  Type 'band karo' to exit.                                    " << endl;
    cout << "  Type 'madad' for supported commands.                         " << endl;
    cout << "================================================================" << endl;
    cout << endl;
}

// ---- Display quick help with supported commands ----
void showHelp() {
    cout << "Supported Commands:" << endl;
    cout << "--------------------" << endl;
    cout << "  General JOIN syntax:" << endl;
    cout << "     <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par <join_type> join karke dikha" << endl;
    cout << "     join_type: inner, left" << endl;
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
    cout << "  3. DEFAULT INNER JOIN:" << endl;
    cout << "     Syntax : <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par join karke dikha" << endl;
    cout << "     Example: students aur marks ko students.id = marks.student_id par join karke dikha" << endl;
    cout << endl;
    cout << "  4. AGGREGATION ON JOIN:" << endl;
    cout << "     Syntax : <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par <inner|left> join karke <table>.<col> ka <func> nikal kar dikha" << endl;
    cout << "     Syntax : <t1> aur <t2> ko <t1>.<col1> = <t2>.<col2> par <inner|left> join karke <col> ka <func> nikal kar dikha" << endl;
    cout << "     Functions: sum, avg, count, min, max" << endl;
    cout << "     Example : students aur marks ko students.id = marks.student_id par inner join karke marks.score ka sum nikal kar dikha" << endl;
    cout << "     Example : students aur marks ko students.id = marks.student_id par left join karke score ka avg nikal kar dikha" << endl;
    cout << endl;
    cout << "  5. THREE TABLE JOIN:" << endl;
    cout << "     Syntax : <t1> aur <t2> aur <t3> ko <t1>.<pk> = <t2>.<fk> aur <t2>.<fk> = <t3>.<pk> par <inner|left> join karke dikha" << endl;
    cout << "     Example: students aur enrollments aur courses ko students.id = enrollments.student_id aur enrollments.course_id = courses.course_id par inner join karke dikha" << endl;
    cout << endl;
    cout << "  6. EXIT:" << endl;
    cout << "     band karo" << endl;
    cout << endl;
    cout << "  7. HELP:" << endl;
    cout << "     madad" << endl;
    cout << endl;
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
