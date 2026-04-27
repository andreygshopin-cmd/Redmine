import argparse

from src.redmine.snapshots import runIssueSnapshotCaptureJob


def _buildArgumentParser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture Redmine issue snapshots")
    parser.add_argument("--mode", choices=("all", "project"), default="all")
    parser.add_argument("--project-redmine-id", type=int, default=None)
    parser.add_argument("--adopt-lock", action="store_true")
    return parser


def main() -> None:
    arguments = _buildArgumentParser().parse_args()
    result = runIssueSnapshotCaptureJob(arguments.mode, arguments.project_redmine_id, arguments.adopt_lock)
    skippedProjects = result.get("skipped_projects") or []
    print(
        "Captured daily snapshots for "
        f"{result['captured_for_date']}: "
        f"{result['created_runs']} project slices, "
        f"{result['captured_issues']} issues, "
        f"{result['already_captured_projects']} projects already had a slice, "
        f"{result['remaining_projects']} projects still remain, "
        f"{len(skippedProjects)} skipped projects."
    )


if __name__ == "__main__":
    main()
