/*
 * ==========================================================
 *  QIR-DB: Query Intermediate Representation Compiler
 *  codegen.cpp - Code Generator implementations
 * ==========================================================
 *
 *  Each code generator translates a ParsedCommand (QIR) into
 *  a target-database query string using deterministic string
 *  templating. No LLM, no NLP — purely mechanical translation.
 *
 *  Dialect differences handled:
 *    MySQL      — backtick quoting, standard SQL joins
 *    PostgreSQL — double-quote quoting, standard SQL joins
 *    SQLite     — double-quote quoting, simpler dialect
 *    MongoDB    — aggregation pipeline ($lookup, $group, $unwind)
 */

#include "codegen.h"
#include <sstream>

// ============================================================
//  CodeGenerator base — shared helper
// ============================================================

string CodeGenerator::aggFuncToString(AggregationFunction func) const {
    switch (func) {
        case AGG_SUM:   return "SUM";
        case AGG_AVG:   return "AVG";
        case AGG_COUNT: return "COUNT";
        case AGG_MIN:   return "MIN";
        case AGG_MAX:   return "MAX";
        default:        return "";
    }
}

// ============================================================
//  MySQL Code Generator
// ============================================================

string MySQLCodeGen::dialectName() const {
    return "MySQL";
}

string MySQLCodeGen::q(const string& identifier) const {
    return "`" + identifier + "`";
}

string MySQLCodeGen::generateTwoTableJoin(const ParsedCommand& cmd, const string& joinType) const {
    string sql = "SELECT *\nFROM " + q(cmd.leftTable);

    if (joinType == "CROSS JOIN") {
        sql += "\nCROSS JOIN " + q(cmd.rightTable);
    } else {
        sql += "\n" + joinType + " " + q(cmd.rightTable)
            + "\n  ON " + q(cmd.leftTable) + "." + q(cmd.leftColumn)
            + " = " + q(cmd.rightTable) + "." + q(cmd.rightColumn);
    }

    // Handle aggregation on join
    if (cmd.aggFunc != AGG_NONE) {
        string funcStr = aggFuncToString(cmd.aggFunc);
        string aggRef;
        if (!cmd.aggTable.empty()) {
            aggRef = q(cmd.aggTable) + "." + q(cmd.aggColumn);
        } else {
            aggRef = q(cmd.aggColumn);
        }

        string joinClause;
        if (joinType == "CROSS JOIN") {
            joinClause = q(cmd.leftTable) + "\nCROSS JOIN " + q(cmd.rightTable);
        } else {
            joinClause = q(cmd.leftTable) + "\n" + joinType + " " + q(cmd.rightTable)
                + "\n  ON " + q(cmd.leftTable) + "." + q(cmd.leftColumn)
                + " = " + q(cmd.rightTable) + "." + q(cmd.rightColumn);
        }

        sql = "SELECT " + funcStr + "(" + aggRef + ") AS " + q(funcStr + "_" + cmd.aggColumn)
            + "\nFROM " + joinClause;
    }

    sql += ";";
    return sql;
}

string MySQLCodeGen::generateThreeTableJoin(const ParsedCommand& cmd, const string& joinType) const {
    string sql;

    if (cmd.aggFunc != AGG_NONE) {
        string funcStr = aggFuncToString(cmd.aggFunc);
        string aggRef;
        if (!cmd.aggTable.empty()) {
            aggRef = q(cmd.aggTable) + "." + q(cmd.aggColumn);
        } else {
            aggRef = q(cmd.aggColumn);
        }
        sql = "SELECT " + funcStr + "(" + aggRef + ") AS " + q(funcStr + "_" + cmd.aggColumn);
    } else {
        sql = "SELECT *";
    }

    sql += "\nFROM " + q(cmd.leftTable)
        + "\n" + joinType + " " + q(cmd.rightTable)
        + "\n  ON " + q(cmd.leftTable) + "." + q(cmd.leftColumn)
        + " = " + q(cmd.rightTable) + "." + q(cmd.rightColumn)
        + "\n" + joinType + " " + q(cmd.thirdTable)
        + "\n  ON " + q(cmd.secondJoinTable) + "." + q(cmd.secondLeftColumn)
        + " = " + q(cmd.thirdTable) + "." + q(cmd.thirdColumn)
        + ";";
    return sql;
}

string MySQLCodeGen::generateAggregate(const ParsedCommand& cmd) const {
    string funcStr = aggFuncToString(cmd.aggFunc);
    string sql = "SELECT " + funcStr + "(" + q(cmd.aggColumn) + ") AS "
        + q(funcStr + "_" + cmd.aggColumn)
        + "\nFROM " + q(cmd.aggTable) + ";";
    return sql;
}

string MySQLCodeGen::generate(const ParsedCommand& cmd) const {
    switch (cmd.type) {
        case CMD_INNER_JOIN:
            return generateTwoTableJoin(cmd, "INNER JOIN");
        case CMD_LEFT_JOIN:
            return generateTwoTableJoin(cmd, "LEFT JOIN");
        case CMD_CROSS_JOIN:
            return generateTwoTableJoin(cmd, "CROSS JOIN");
        case CMD_INNER_JOIN_3:
            return generateThreeTableJoin(cmd, "INNER JOIN");
        case CMD_LEFT_JOIN_3:
            return generateThreeTableJoin(cmd, "LEFT JOIN");
        case CMD_AGGREGATE:
            return generateAggregate(cmd);
        default:
            return "";
    }
}

// ============================================================
//  PostgreSQL Code Generator
// ============================================================

string PostgresCodeGen::dialectName() const {
    return "PostgreSQL";
}

string PostgresCodeGen::q(const string& identifier) const {
    return "\"" + identifier + "\"";
}

string PostgresCodeGen::generateTwoTableJoin(const ParsedCommand& cmd, const string& joinType) const {
    string sql = "SELECT *\nFROM " + q(cmd.leftTable);

    if (joinType == "CROSS JOIN") {
        sql += "\nCROSS JOIN " + q(cmd.rightTable);
    } else {
        sql += "\n" + joinType + " " + q(cmd.rightTable)
            + "\n  ON " + q(cmd.leftTable) + "." + q(cmd.leftColumn)
            + " = " + q(cmd.rightTable) + "." + q(cmd.rightColumn);
    }

    if (cmd.aggFunc != AGG_NONE) {
        string funcStr = aggFuncToString(cmd.aggFunc);
        string aggRef;
        if (!cmd.aggTable.empty()) {
            aggRef = q(cmd.aggTable) + "." + q(cmd.aggColumn);
        } else {
            aggRef = q(cmd.aggColumn);
        }

        string joinClause;
        if (joinType == "CROSS JOIN") {
            joinClause = q(cmd.leftTable) + "\nCROSS JOIN " + q(cmd.rightTable);
        } else {
            joinClause = q(cmd.leftTable) + "\n" + joinType + " " + q(cmd.rightTable)
                + "\n  ON " + q(cmd.leftTable) + "." + q(cmd.leftColumn)
                + " = " + q(cmd.rightTable) + "." + q(cmd.rightColumn);
        }

        sql = "SELECT " + funcStr + "(" + aggRef + ") AS " + q(funcStr + "_" + cmd.aggColumn)
            + "\nFROM " + joinClause;
    }

    sql += ";";
    return sql;
}

string PostgresCodeGen::generateThreeTableJoin(const ParsedCommand& cmd, const string& joinType) const {
    string sql;

    if (cmd.aggFunc != AGG_NONE) {
        string funcStr = aggFuncToString(cmd.aggFunc);
        string aggRef;
        if (!cmd.aggTable.empty()) {
            aggRef = q(cmd.aggTable) + "." + q(cmd.aggColumn);
        } else {
            aggRef = q(cmd.aggColumn);
        }
        sql = "SELECT " + funcStr + "(" + aggRef + ") AS " + q(funcStr + "_" + cmd.aggColumn);
    } else {
        sql = "SELECT *";
    }

    sql += "\nFROM " + q(cmd.leftTable)
        + "\n" + joinType + " " + q(cmd.rightTable)
        + "\n  ON " + q(cmd.leftTable) + "." + q(cmd.leftColumn)
        + " = " + q(cmd.rightTable) + "." + q(cmd.rightColumn)
        + "\n" + joinType + " " + q(cmd.thirdTable)
        + "\n  ON " + q(cmd.secondJoinTable) + "." + q(cmd.secondLeftColumn)
        + " = " + q(cmd.thirdTable) + "." + q(cmd.thirdColumn)
        + ";";
    return sql;
}

string PostgresCodeGen::generateAggregate(const ParsedCommand& cmd) const {
    string funcStr = aggFuncToString(cmd.aggFunc);
    string sql = "SELECT " + funcStr + "(" + q(cmd.aggColumn) + ") AS "
        + q(funcStr + "_" + cmd.aggColumn)
        + "\nFROM " + q(cmd.aggTable) + ";";
    return sql;
}

string PostgresCodeGen::generate(const ParsedCommand& cmd) const {
    switch (cmd.type) {
        case CMD_INNER_JOIN:
            return generateTwoTableJoin(cmd, "INNER JOIN");
        case CMD_LEFT_JOIN:
            return generateTwoTableJoin(cmd, "LEFT JOIN");
        case CMD_CROSS_JOIN:
            return generateTwoTableJoin(cmd, "CROSS JOIN");
        case CMD_INNER_JOIN_3:
            return generateThreeTableJoin(cmd, "INNER JOIN");
        case CMD_LEFT_JOIN_3:
            return generateThreeTableJoin(cmd, "LEFT JOIN");
        case CMD_AGGREGATE:
            return generateAggregate(cmd);
        default:
            return "";
    }
}

// ============================================================
//  SQLite Code Generator
// ============================================================

string SQLiteCodeGen::dialectName() const {
    return "SQLite";
}

string SQLiteCodeGen::q(const string& identifier) const {
    return "\"" + identifier + "\"";
}

string SQLiteCodeGen::generateTwoTableJoin(const ParsedCommand& cmd, const string& joinType) const {
    string sql = "SELECT *\nFROM " + q(cmd.leftTable);

    if (joinType == "CROSS JOIN") {
        sql += "\nCROSS JOIN " + q(cmd.rightTable);
    } else {
        sql += "\n" + joinType + " " + q(cmd.rightTable)
            + "\n  ON " + q(cmd.leftTable) + "." + q(cmd.leftColumn)
            + " = " + q(cmd.rightTable) + "." + q(cmd.rightColumn);
    }

    if (cmd.aggFunc != AGG_NONE) {
        string funcStr = aggFuncToString(cmd.aggFunc);
        string aggRef;
        if (!cmd.aggTable.empty()) {
            aggRef = q(cmd.aggTable) + "." + q(cmd.aggColumn);
        } else {
            aggRef = q(cmd.aggColumn);
        }

        string joinClause;
        if (joinType == "CROSS JOIN") {
            joinClause = q(cmd.leftTable) + "\nCROSS JOIN " + q(cmd.rightTable);
        } else {
            joinClause = q(cmd.leftTable) + "\n" + joinType + " " + q(cmd.rightTable)
                + "\n  ON " + q(cmd.leftTable) + "." + q(cmd.leftColumn)
                + " = " + q(cmd.rightTable) + "." + q(cmd.rightColumn);
        }

        sql = "SELECT " + funcStr + "(" + aggRef + ") AS " + q(funcStr + "_" + cmd.aggColumn)
            + "\nFROM " + joinClause;
    }

    sql += ";";
    return sql;
}

string SQLiteCodeGen::generateThreeTableJoin(const ParsedCommand& cmd, const string& joinType) const {
    string sql;

    if (cmd.aggFunc != AGG_NONE) {
        string funcStr = aggFuncToString(cmd.aggFunc);
        string aggRef;
        if (!cmd.aggTable.empty()) {
            aggRef = q(cmd.aggTable) + "." + q(cmd.aggColumn);
        } else {
            aggRef = q(cmd.aggColumn);
        }
        sql = "SELECT " + funcStr + "(" + aggRef + ") AS " + q(funcStr + "_" + cmd.aggColumn);
    } else {
        sql = "SELECT *";
    }

    sql += "\nFROM " + q(cmd.leftTable)
        + "\n" + joinType + " " + q(cmd.rightTable)
        + "\n  ON " + q(cmd.leftTable) + "." + q(cmd.leftColumn)
        + " = " + q(cmd.rightTable) + "." + q(cmd.rightColumn)
        + "\n" + joinType + " " + q(cmd.thirdTable)
        + "\n  ON " + q(cmd.secondJoinTable) + "." + q(cmd.secondLeftColumn)
        + " = " + q(cmd.thirdTable) + "." + q(cmd.thirdColumn)
        + ";";
    return sql;
}

string SQLiteCodeGen::generateAggregate(const ParsedCommand& cmd) const {
    string funcStr = aggFuncToString(cmd.aggFunc);
    string sql = "SELECT " + funcStr + "(" + q(cmd.aggColumn) + ") AS "
        + q(funcStr + "_" + cmd.aggColumn)
        + "\nFROM " + q(cmd.aggTable) + ";";
    return sql;
}

string SQLiteCodeGen::generate(const ParsedCommand& cmd) const {
    switch (cmd.type) {
        case CMD_INNER_JOIN:
            return generateTwoTableJoin(cmd, "INNER JOIN");
        case CMD_LEFT_JOIN:
            return generateTwoTableJoin(cmd, "LEFT JOIN");
        case CMD_CROSS_JOIN:
            return generateTwoTableJoin(cmd, "CROSS JOIN");
        case CMD_INNER_JOIN_3:
            return generateThreeTableJoin(cmd, "INNER JOIN");
        case CMD_LEFT_JOIN_3:
            return generateThreeTableJoin(cmd, "LEFT JOIN");
        case CMD_AGGREGATE:
            return generateAggregate(cmd);
        default:
            return "";
    }
}

// ============================================================
//  MongoDB Code Generator
// ============================================================

string MongoCodeGen::dialectName() const {
    return "MongoDB";
}

string MongoCodeGen::mongoAggOp(AggregationFunction func) const {
    switch (func) {
        case AGG_SUM:   return "$sum";
        case AGG_AVG:   return "$avg";
        case AGG_COUNT: return "$sum";  // COUNT uses $sum: 1
        case AGG_MIN:   return "$min";
        case AGG_MAX:   return "$max";
        default:        return "";
    }
}

string MongoCodeGen::generateLookup(const ParsedCommand& cmd) const {
    string aggPart = "";

    // If aggregation is requested, add a $group stage
    if (cmd.aggFunc != AGG_NONE) {
        string funcStr = aggFuncToString(cmd.aggFunc);
        string op = mongoAggOp(cmd.aggFunc);
        string fieldRef;

        if (!cmd.aggTable.empty() && cmd.aggTable == cmd.rightTable) {
            fieldRef = "$" + cmd.rightTable + "_joined." + cmd.aggColumn;
        } else if (!cmd.aggTable.empty() && cmd.aggTable == cmd.leftTable) {
            fieldRef = "$" + cmd.aggColumn;
        } else {
            fieldRef = "$" + cmd.aggColumn;
        }

        if (cmd.aggFunc == AGG_COUNT) {
            aggPart = ",\n  { $group: { _id: null, " + funcStr + "_" + cmd.aggColumn + ": { " + op + ": 1 } } }";
        } else {
            aggPart = ",\n  { $group: { _id: null, " + funcStr + "_" + cmd.aggColumn + ": { " + op + ": \"" + fieldRef + "\" } } }";
        }
    }

    string sql = "db." + cmd.leftTable + ".aggregate([\n"
        + "  { $lookup: {\n"
        + "      from: \"" + cmd.rightTable + "\",\n"
        + "      localField: \"" + cmd.leftColumn + "\",\n"
        + "      foreignField: \"" + cmd.rightColumn + "\",\n"
        + "      as: \"" + cmd.rightTable + "_joined\"\n"
        + "  }},\n"
        + "  { $unwind: \"$" + cmd.rightTable + "_joined\" }"
        + aggPart + "\n"
        + "]);";
    return sql;
}

string MongoCodeGen::generateLeftLookup(const ParsedCommand& cmd) const {
    string aggPart = "";

    if (cmd.aggFunc != AGG_NONE) {
        string funcStr = aggFuncToString(cmd.aggFunc);
        string op = mongoAggOp(cmd.aggFunc);
        string fieldRef;

        if (!cmd.aggTable.empty() && cmd.aggTable == cmd.rightTable) {
            fieldRef = "$" + cmd.rightTable + "_joined." + cmd.aggColumn;
        } else {
            fieldRef = "$" + cmd.aggColumn;
        }

        if (cmd.aggFunc == AGG_COUNT) {
            aggPart = ",\n  { $group: { _id: null, " + funcStr + "_" + cmd.aggColumn + ": { " + op + ": 1 } } }";
        } else {
            aggPart = ",\n  { $group: { _id: null, " + funcStr + "_" + cmd.aggColumn + ": { " + op + ": \"" + fieldRef + "\" } } }";
        }
    }

    // LEFT JOIN in MongoDB: $lookup + $unwind with preserveNullAndEmptyArrays: true
    string sql = "db." + cmd.leftTable + ".aggregate([\n"
        + "  { $lookup: {\n"
        + "      from: \"" + cmd.rightTable + "\",\n"
        + "      localField: \"" + cmd.leftColumn + "\",\n"
        + "      foreignField: \"" + cmd.rightColumn + "\",\n"
        + "      as: \"" + cmd.rightTable + "_joined\"\n"
        + "  }},\n"
        + "  { $unwind: {\n"
        + "      path: \"$" + cmd.rightTable + "_joined\",\n"
        + "      preserveNullAndEmptyArrays: true\n"
        + "  }}"
        + aggPart + "\n"
        + "]);";
    return sql;
}

string MongoCodeGen::generateCrossLookup(const ParsedCommand& cmd) const {
    string aggPart = "";

    if (cmd.aggFunc != AGG_NONE) {
        string funcStr = aggFuncToString(cmd.aggFunc);
        string op = mongoAggOp(cmd.aggFunc);
        string fieldRef;

        if (!cmd.aggTable.empty() && cmd.aggTable == cmd.rightTable) {
            fieldRef = "$" + cmd.rightTable + "_joined." + cmd.aggColumn;
        } else {
            fieldRef = "$" + cmd.aggColumn;
        }

        if (cmd.aggFunc == AGG_COUNT) {
            aggPart = ",\n  { $group: { _id: null, " + funcStr + "_" + cmd.aggColumn + ": { " + op + ": 1 } } }";
        } else {
            aggPart = ",\n  { $group: { _id: null, " + funcStr + "_" + cmd.aggColumn + ": { " + op + ": \"" + fieldRef + "\" } } }";
        }
    }

    // CROSS JOIN in MongoDB: $lookup with pipeline (match all), then $unwind
    string sql = "db." + cmd.leftTable + ".aggregate([\n"
        + "  { $lookup: {\n"
        + "      from: \"" + cmd.rightTable + "\",\n"
        + "      pipeline: [],\n"
        + "      as: \"" + cmd.rightTable + "_joined\"\n"
        + "  }},\n"
        + "  { $unwind: \"$" + cmd.rightTable + "_joined\" }"
        + aggPart + "\n"
        + "]);";
    return sql;
}

string MongoCodeGen::generateThreeTableLookup(const ParsedCommand& cmd, bool leftJoin) const {
    string unwindOpts = leftJoin
        ? "{\n      path: \"$" + cmd.rightTable + "_joined\",\n      preserveNullAndEmptyArrays: true\n  }"
        : "\"$" + cmd.rightTable + "_joined\"";

    string unwindOpts2 = leftJoin
        ? "{\n      path: \"$" + cmd.thirdTable + "_joined\",\n      preserveNullAndEmptyArrays: true\n  }"
        : "\"$" + cmd.thirdTable + "_joined\"";

    // Determine which field to use for the second $lookup
    // The secondJoinTable links to the third table
    string secondLocalField;
    if (cmd.secondJoinTable == cmd.leftTable) {
        secondLocalField = cmd.secondLeftColumn;
    } else {
        secondLocalField = cmd.rightTable + "_joined." + cmd.secondLeftColumn;
    }

    string aggPart = "";
    if (cmd.aggFunc != AGG_NONE) {
        string funcStr = aggFuncToString(cmd.aggFunc);
        string op = mongoAggOp(cmd.aggFunc);
        string fieldRef = "$" + cmd.aggColumn;

        if (!cmd.aggTable.empty()) {
            if (cmd.aggTable == cmd.rightTable) {
                fieldRef = "$" + cmd.rightTable + "_joined." + cmd.aggColumn;
            } else if (cmd.aggTable == cmd.thirdTable) {
                fieldRef = "$" + cmd.thirdTable + "_joined." + cmd.aggColumn;
            }
        }

        if (cmd.aggFunc == AGG_COUNT) {
            aggPart = ",\n  { $group: { _id: null, " + funcStr + "_" + cmd.aggColumn + ": { " + op + ": 1 } } }";
        } else {
            aggPart = ",\n  { $group: { _id: null, " + funcStr + "_" + cmd.aggColumn + ": { " + op + ": \"" + fieldRef + "\" } } }";
        }
    }

    string sql = "db." + cmd.leftTable + ".aggregate([\n"
        + "  { $lookup: {\n"
        + "      from: \"" + cmd.rightTable + "\",\n"
        + "      localField: \"" + cmd.leftColumn + "\",\n"
        + "      foreignField: \"" + cmd.rightColumn + "\",\n"
        + "      as: \"" + cmd.rightTable + "_joined\"\n"
        + "  }},\n"
        + "  { $unwind: " + unwindOpts + " },\n"
        + "  { $lookup: {\n"
        + "      from: \"" + cmd.thirdTable + "\",\n"
        + "      localField: \"" + secondLocalField + "\",\n"
        + "      foreignField: \"" + cmd.thirdColumn + "\",\n"
        + "      as: \"" + cmd.thirdTable + "_joined\"\n"
        + "  }},\n"
        + "  { $unwind: " + unwindOpts2 + " }"
        + aggPart + "\n"
        + "]);";
    return sql;
}

string MongoCodeGen::generateAggregate(const ParsedCommand& cmd) const {
    string funcStr = aggFuncToString(cmd.aggFunc);
    string op = mongoAggOp(cmd.aggFunc);

    string valueExpr;
    if (cmd.aggFunc == AGG_COUNT) {
        valueExpr = "{ " + op + ": 1 }";
    } else {
        valueExpr = "{ " + op + ": \"$" + cmd.aggColumn + "\" }";
    }

    string sql = "db." + cmd.aggTable + ".aggregate([\n"
        + "  { $group: {\n"
        + "      _id: null,\n"
        + "      " + funcStr + "_" + cmd.aggColumn + ": " + valueExpr + "\n"
        + "  }}\n"
        + "]);";
    return sql;
}

string MongoCodeGen::generate(const ParsedCommand& cmd) const {
    switch (cmd.type) {
        case CMD_INNER_JOIN:
            return generateLookup(cmd);
        case CMD_LEFT_JOIN:
            return generateLeftLookup(cmd);
        case CMD_CROSS_JOIN:
            return generateCrossLookup(cmd);
        case CMD_INNER_JOIN_3:
            return generateThreeTableLookup(cmd, false);
        case CMD_LEFT_JOIN_3:
            return generateThreeTableLookup(cmd, true);
        case CMD_AGGREGATE:
            return generateAggregate(cmd);
        default:
            return "";
    }
}
