from src.redmine.snapshots import captureAllIssueSnapshots


def main() -> None:
    result = captureAllIssueSnapshots()
    print(
        "Captured daily snapshots for "
        f"{result['captured_for_date']}: "
        f"{result['created_runs']} project slices, "
        f"{result['captured_issues']} issues, "
        f"{result['already_captured_projects']} projects already had a slice, "
        f"{result['remaining_projects']} projects still remain, "
        f"{len(result['skipped_projects'])} skipped projects."
    )


if __name__ == "__main__":
    main()
