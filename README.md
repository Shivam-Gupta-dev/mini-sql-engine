# QIR-DB: Query Intermediate Representation Compiler

A **Multi-Database Translation Engine** built in C++ that reads Hinglish (Hindi + English) commands and generates equivalent queries for **MySQL**, **PostgreSQL**, **SQLite**, and **MongoDB** — all via deterministic compiler-style code generation. No LLM needed.

> No external databases. No SQLite runtime. No AI/LLM. Pure C++ with `fstream` + compiler-style string templating.

---

## Architecture

```
                                    ┌──→ MySQLCodeGen     → MySQL SQL string
Hinglish Input → Parser → QIR ─────├──→ PostgresCodeGen  → PostgreSQL SQL string
               (tokenizer)  (ParsedCommand) ├──→ SQLiteCodeGen    → SQLite SQL string
                                    ├──→ MongoCodeGen     → MongoDB aggregation pipeline
                                    └──→ Engine           → Execute + Terminal/File Output
```

| Module | Responsibility |
|--------|---------------|
| `Parser` | Tokenizes Hinglish input, extracts table names, join type, and condition |
| `Engine` | Loads tables, validates schema, executes nested-loop joins, saves output |
| `CodeGenerator` | Abstract base class — translates `ParsedCommand` to target-database query |
| `MySQLCodeGen` | MySQL dialect (backtick quoting) |
| `PostgresCodeGen` | PostgreSQL dialect (double-quote quoting) |
| `SQLiteCodeGen` | SQLite dialect (double-quote quoting) |
| `MongoCodeGen` | MongoDB aggregation pipeline syntax |
| `Table` | Parses `.tbl` files — schema metadata (PK, FK, types) and data rows |
| `show_output.py` | Reads CSV output using pandas, displays as formatted table |
| `streamlit_ui.py` | Web UI with query builder and Generated SQL tab |

---

## Features

- Reads `.tbl` table files with strict `#schema:` / `#data:` format
- Parses schema metadata: column names, data types (`INT`, `STRING`), `PRIMARY_KEY`, `FOREIGN_KEY REFERENCES`
- Supports **INNER JOIN**, **LEFT JOIN**, **CROSS JOIN** with explicit join conditions
- Supports **3-table JOINs** with chained conditions
- Supports single-table and joined-result aggregation: `sum`, `avg`, `count`, `min`, `max`
- **Multi-Database SQL Generation**: Every query automatically generates equivalent SQL for MySQL, PostgreSQL, SQLite, and MongoDB
- Schema validation before every join (column existence, FK reference checks, PK-FK warnings)
- Accepts **Hinglish (Hindi + English)** commands — no external NLP libraries
- Outputs results to terminal + saves as **CSV**, **formatted TXT**, and **Generated SQL** files
- Streamlit Web UI with **Generated SQL** tab showing all dialect translations
- Infinite REPL loop — exits on `band karo`
- Never crashes on bad input — all errors are handled gracefully

---

## Project Structure

```
HinglishDB/
├── main.cpp            # Entry point — REPL loop
├── parser.h / .cpp     # Hinglish command parser
├── engine.h / .cpp     # Join execution engine with schema validation + SQL generation
├── codegen.h / .cpp    # Multi-database code generators (MySQL, PostgreSQL, SQLite, MongoDB)
├── table.h  / .cpp     # Table loader — parses .tbl files (schema + data)
├── show_output.py      # Python CSV viewer using pandas + tabulate
├── streamlit_ui.py     # Streamlit Web UI with Generated SQL tab
├── QIR_DB_PLAN.txt     # Implementation plan document
└── data/
    ├── students.tbl    # Sample table: students (id, name, age, ...)
    ├── marks.tbl       # Sample table: marks (mark_id, student_id FK, subject, score)
    ├── output_*.csv    # Generated CSV join outputs
    ├── output_*.txt    # Generated TXT join outputs (formatted + metadata)
    └── output_generated_sql_*.txt  # Generated SQL for all dialects
```

---

## Table File Format (`.tbl`)

Each table file follows a strict format with `#schema:` and `#data:` sections:

**`data/students.tbl`**
```
#schema:
id INT PRIMARY_KEY
name STRING
age INT
#data:
1,Shivam,21
2,Rahul,22
3,Aman,20
```

**`data/marks.tbl`**
```
#schema:
mark_id INT PRIMARY_KEY
student_id INT FOREIGN_KEY REFERENCES students(id)
subject STRING
score INT
#data:
1,1,Math,90
2,1,Physics,85
3,2,Math,70
```

### Schema Rules

| Token | Meaning |
|-------|---------|
| `column_name` | Name of the column |
| `INT` / `STRING` | Data type |
| `PRIMARY_KEY` | Marks column as primary key |
| `FOREIGN_KEY REFERENCES table(column)` | Marks column as foreign key with reference |

---

## Supported Hinglish Commands

### 1. INNER JOIN

```
students aur marks ko students.id = marks.student_id par inner join karke dikha
```

### 2. LEFT JOIN

```
students aur marks ko students.id = marks.student_id par left join karke dikha
```

### 3. CROSS JOIN

```
students aur courses ko cross join karke dikha
```

### 4. THREE TABLE JOIN

```
students aur enrollments aur courses ko students.id = enrollments.student_id aur enrollments.course_id = courses.course_id par inner join karke dikha
```

### 5. Single Table Aggregation

```
marks me score ka avg nikal kar dikha
```

### 6. Aggregation On Join

```
students aur marks ko students.id = marks.student_id par inner join karke marks.score ka max nikal kar dikha
```

### 7. Help / Exit

```
madad
band karo
```

---

## Multi-Database SQL Generation

Every query automatically generates equivalent SQL for all target databases:

**Input:**
```
students aur marks ko students.id = marks.student_id par inner join karke dikha
```

**Generated Output:**

```
==================== Generated SQL ====================

[MySQL]
SELECT *
FROM `students`
INNER JOIN `marks`
  ON `students`.`id` = `marks`.`student_id`;

[PostgreSQL]
SELECT *
FROM "students"
INNER JOIN "marks"
  ON "students"."id" = "marks"."student_id";

[SQLite]
SELECT *
FROM "students"
INNER JOIN "marks"
  ON "students"."id" = "marks"."student_id";

[MongoDB]
db.students.aggregate([
  { $lookup: {
      from: "marks",
      localField: "id",
      foreignField: "student_id",
      as: "marks_joined"
  }},
  { $unwind: "$marks_joined" }
]);

========================================================
```

### Dialect Differences Handled

| Feature | MySQL | PostgreSQL | SQLite | MongoDB |
|---------|-------|------------|--------|---------|
| Identifier quoting | `` `backticks` `` | `"double quotes"` | `"double quotes"` | N/A |
| JOIN syntax | Standard SQL | Standard SQL | Standard SQL | `$lookup` pipeline |
| CROSS JOIN | `CROSS JOIN` | `CROSS JOIN` | `CROSS JOIN` | `$lookup` with empty pipeline |
| Aggregation | `SELECT SUM(col)` | `SELECT SUM(col)` | `SELECT SUM(col)` | `$group` stage |

---

## How to Build & Run

### Prerequisites

- **C++ compiler** with C++17 support (g++, MSVC, clang++)
- **Python 3** with `pandas` and `tabulate` (for the CSV viewer)

### Compile

```bash
cd HinglishDB
g++ -std=c++17 -o HinglishJoinEngine.exe main.cpp parser.cpp engine.cpp table.cpp codegen.cpp
```

### Run

```bash
./HinglishJoinEngine.exe
```

### Streamlit Web UI

```bash
pip install streamlit pandas
streamlit run streamlit_ui.py
```

### Install Python dependencies (for CSV viewer)

```bash
pip install pandas tabulate
```

### View CSV output

```bash
python show_output.py                                          # interactive mode
python show_output.py output_inner_join_students_marks.csv     # direct file
```

---

## Output Files

Every query generates multiple output files in `data/`:

| File | Format | Content |
|------|--------|---------|
| `output_*_join_*.csv` | CSV | Clean header + data rows (used by `show_output.py`) |
| `output_*_join_*.txt` | TXT | Metadata comments (type, condition, timestamp) + bordered table |
| `output_generated_sql_*.txt` | TXT | Generated SQL for MySQL, PostgreSQL, SQLite, MongoDB |

---

## Why No LLM Is Needed

1. **Parsing is deterministic** — The Hinglish grammar is fixed/finite; keyword-based tokenization handles it perfectly.
2. **SQL dialects differ in syntax, not semantics** — Translating between MySQL/PostgreSQL/SQLite is a formatting problem, not an AI problem.
3. **QIR is already structured** — `ParsedCommand` gives typed, validated fields; translation to SQL is simple string templating.
4. **A compiler is more reliable** — Deterministic code generation ALWAYS produces correct SQL. An LLM might hallucinate incorrect syntax.

---

## Join Algorithms

**INNER JOIN** — Nested Loop `O(n × m)`:
```
For each row in A:
  For each row in B:
    If A[colA] == B[colB] → output merged row
```

**LEFT JOIN** — Nested Loop with NULL padding `O(n × m)`:
```
For each row in A:
  matched = false
  For each row in B:
    If A[colA] == B[colB] → output merged row; matched = true
  If !matched → output A row + NULL for all B columns
```

---

## Error Handling

| Scenario | Response |
|----------|----------|
| Table file not found | Error message with file path |
| Column not found in table | Error message with column and table name |
| Invalid join syntax | Syntax hint with expected format |
| Corrupted schema in `.tbl` | Error message — skips bad lines |
| FK reference mismatch | Warning printed before join |
| Non PK-FK join | Error message - join is blocked until a valid PK-FK pair is used |

---

## Constraints

- Standard C++ only (`fstream`, `sstream`, `vector`, `map`, `string`)
- No external databases or SQL libraries
- No external NLP or LLM — deterministic compiler-style code generation
- Read-only engine — no `CREATE`, `INSERT`, `UPDATE`, `DELETE`
- Tables must pre-exist in `data/` folder
