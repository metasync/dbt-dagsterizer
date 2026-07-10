from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def main() -> None:
    default_env = "{{ cookiecutter.default_env }}".strip()
    schedule_timezone = "{{ cookiecutter.schedule_timezone }}".strip()

    supported = {"development", "sandbox", "production"}

    if not default_env:
        raise ValueError("cookiecutter.default_env must be non-empty")

    if default_env not in supported:
        raise ValueError(
            f"Unsupported cookiecutter.default_env ({default_env}). Supported: {sorted(supported)}"
        )

    if not schedule_timezone:
        raise ValueError("cookiecutter.schedule_timezone must be non-empty")

    try:
        ZoneInfo(schedule_timezone)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(
            f"Invalid cookiecutter.schedule_timezone ({schedule_timezone!r}). "
            "Use an IANA timezone such as 'UTC', 'Asia/Macau', or 'America/New_York'."
        ) from exc


if __name__ == "__main__":
    main()
