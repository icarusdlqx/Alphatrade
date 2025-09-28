import datetime as dt

import pytz

from scheduler import minute_marker


def test_minute_marker_differs_between_days():
    tz = pytz.timezone("America/New_York")
    first_day = tz.localize(dt.datetime(2024, 1, 1, 11, 50))
    next_day = tz.localize(dt.datetime(2024, 1, 2, 11, 50))

    first_marker = minute_marker(first_day)
    second_marker = minute_marker(next_day)

    # Consecutive days at the same minute must produce unique markers so the
    # scheduler runs on both days.
    assert first_marker != second_marker
    assert second_marker[1] == first_marker[1]
