import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEST_EXE = ROOT / "HinglishJoinEngine_test.exe"


def run(command, check=True):
    result = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"Command failed: {' '.join(map(str, command))}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result


def build_engine():
    run(
        [
            "g++",
            "-std=c++17",
            "-Wall",
            "-Wextra",
            "-pedantic",
            "-o",
            str(TEST_EXE),
            "main.cpp",
            "parser.cpp",
            "engine.cpp",
            "table.cpp",
        ]
    )


def execute_hinglish(command):
    result = run([str(TEST_EXE), command])
    return result.stdout + result.stderr


def assert_contains(text, expected):
    if expected not in text:
        raise AssertionError(f"Expected to find {expected!r} in output:\n{text}")


def test_inner_join():
    output = execute_hinglish(
        "students aur marks ko students.id = marks.student_id par inner join karke dikha"
    )
    assert_contains(output, ">> INNER JOIN: students.id = marks.student_id")
    assert_contains(output, "Total matched rows: 30")


def test_left_join():
    output = execute_hinglish(
        "students aur marks ko students.id = marks.student_id par left join karke dikha"
    )
    assert_contains(output, ">> LEFT JOIN: students.id = marks.student_id")
    assert_contains(output, "Total result rows: 30")


def test_join_aggregation():
    output = execute_hinglish(
        "students aur marks ko students.id = marks.student_id par inner join karke marks.score ka avg nikal kar dikha"
    )
    assert_contains(output, ">> AGGREGATE ON JOIN: AVG of marks.score")
    assert_contains(output, "84.2")


def test_invalid_table_error():
    output = execute_hinglish(
        "missing aur marks ko missing.id = marks.student_id par inner join karke dikha"
    )
    assert_contains(output, "Table file 'data/missing.tbl' nahi mila")


def test_invalid_pk_fk_join_is_blocked():
    output = execute_hinglish(
        "students aur marks ko students.age = marks.score par inner join karke dikha"
    )
    assert_contains(output, "Join blocked: valid PK-FK relationship required")


def test_three_table_join():
    output = execute_hinglish(
        "students aur enrollments aur courses ko students.id = enrollments.student_id aur enrollments.course_id = courses.course_id par inner join karke dikha"
    )
    assert_contains(output, ">> 3-TABLE INNER JOIN: students.id = enrollments.student_id AND enrollments.course_id = courses.course_id")
    assert_contains(output, "Total result rows: 10")


def test_dynamic_width_keeps_long_values_separated():
    output = execute_hinglish(
        "students aur marks ko students.id = marks.student_id par inner join karke dikha"
    )
    assert_contains(output, "Information Technology|")


def main():
    try:
        build_engine()
        tests = [
            test_inner_join,
            test_left_join,
            test_join_aggregation,
            test_invalid_table_error,
            test_invalid_pk_fk_join_is_blocked,
            test_three_table_join,
            test_dynamic_width_keeps_long_values_separated,
        ]
        for test in tests:
            test()
            print(f"PASS {test.__name__}")
        print(f"\n{len(tests)} tests passed.")
    finally:
        if TEST_EXE.exists():
            TEST_EXE.unlink()


if __name__ == "__main__":
    main()
