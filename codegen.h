/*
 * ==========================================================
 *  QIR-DB: Query Intermediate Representation Compiler
 *  codegen.h - Code Generator class declarations
 * ==========================================================
 *
 *  The CodeGenerator base class defines the interface for
 *  translating a ParsedCommand (QIR) into a target-database
 *  query string. Each dialect subclass implements generate()
 *  using deterministic string templating — no LLM or NLP.
 *
 *  Supported backends:
 *    - MySQLCodeGen     (backtick quoting)
 *    - PostgresCodeGen  (double-quote quoting)
 *    - SQLiteCodeGen    (double-quote quoting, simpler dialect)
 *    - MongoCodeGen     (aggregation pipeline syntax)
 */

#ifndef CODEGEN_H
#define CODEGEN_H

#include <string>
#include "parser.h"

using namespace std;

// ---- Abstract base class for all code generators ----
// Each subclass translates a ParsedCommand into a dialect-specific
// query string using deterministic string templating.
class CodeGenerator {
public:
    virtual ~CodeGenerator() {}

    // Return the human-readable name of this dialect (e.g. "MySQL")
    virtual string dialectName() const = 0;

    // Translate a ParsedCommand into a query string for this dialect.
    // Returns an empty string for CMD_EXIT or CMD_UNKNOWN.
    virtual string generate(const ParsedCommand& cmd) const = 0;

protected:
    // Helper: convert AggregationFunction enum to SQL function name
    string aggFuncToString(AggregationFunction func) const;
};

// ---- MySQL Code Generator ----
// Uses backtick quoting: `table`.`column`
class MySQLCodeGen : public CodeGenerator {
public:
    string dialectName() const override;
    string generate(const ParsedCommand& cmd) const override;

private:
    // Quote an identifier with backticks
    string q(const string& identifier) const;

    // Build a 2-table JOIN query
    string generateTwoTableJoin(const ParsedCommand& cmd, const string& joinType) const;

    // Build a 3-table JOIN query
    string generateThreeTableJoin(const ParsedCommand& cmd, const string& joinType) const;

    // Build a standalone aggregation query
    string generateAggregate(const ParsedCommand& cmd) const;
};

// ---- PostgreSQL Code Generator ----
// Uses double-quote quoting: "table"."column"
class PostgresCodeGen : public CodeGenerator {
public:
    string dialectName() const override;
    string generate(const ParsedCommand& cmd) const override;

private:
    string q(const string& identifier) const;
    string generateTwoTableJoin(const ParsedCommand& cmd, const string& joinType) const;
    string generateThreeTableJoin(const ParsedCommand& cmd, const string& joinType) const;
    string generateAggregate(const ParsedCommand& cmd) const;
};

// ---- SQLite Code Generator ----
// Uses double-quote quoting (same as PostgreSQL for identifiers)
// but has simpler dialect rules
class SQLiteCodeGen : public CodeGenerator {
public:
    string dialectName() const override;
    string generate(const ParsedCommand& cmd) const override;

private:
    string q(const string& identifier) const;
    string generateTwoTableJoin(const ParsedCommand& cmd, const string& joinType) const;
    string generateThreeTableJoin(const ParsedCommand& cmd, const string& joinType) const;
    string generateAggregate(const ParsedCommand& cmd) const;
};

// ---- MongoDB Code Generator ----
// Generates MongoDB aggregation pipeline syntax:
//   db.collection.aggregate([...])
class MongoCodeGen : public CodeGenerator {
public:
    string dialectName() const override;
    string generate(const ParsedCommand& cmd) const override;

private:
    // Build a $lookup + $unwind pipeline for joins
    string generateLookup(const ParsedCommand& cmd) const;

    // Build a $lookup pipeline for left joins (no $unwind, uses preserveNullAndEmptyArrays)
    string generateLeftLookup(const ParsedCommand& cmd) const;

    // Build a cross join via $lookup with no condition + $unwind
    string generateCrossLookup(const ParsedCommand& cmd) const;

    // Build a 3-table join pipeline
    string generateThreeTableLookup(const ParsedCommand& cmd, bool leftJoin) const;

    // Build a standalone $group aggregation
    string generateAggregate(const ParsedCommand& cmd) const;

    // Map AggregationFunction to MongoDB accumulator operator
    string mongoAggOp(AggregationFunction func) const;
};

#endif // CODEGEN_H
