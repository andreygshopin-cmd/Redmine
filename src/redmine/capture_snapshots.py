from src.redmine.snapshots import captureAllIssueSnapshots


def main() -> None:
    result = captureAllIssueSnapshots()
    print(
        "Captured snapshot batch "
        f"{result['snapshot_batch_id']} for {result['captured_for_date']}: "
        f"{result['created_runs']} project slices, "
        f"{result['captured_issues']} issues, "
        f"{len(result['skipped_projects'])} skipped projects."
    )


if __name__ == "__main__":
    main()
