from src.redmine.config import loadConfig


def main() -> None:
    config = loadConfig()

    if config.redmineUrl:
        print(f"Redmine URL configured: {config.redmineUrl}")
    else:
        print("Redmine project is ready.")


if __name__ == "__main__":
    main()
