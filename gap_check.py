import datetime
import re
import shutil
import sqlite3
from collections import Counter
from typing import Callable
from zoneinfo import ZoneInfo

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import typedstream
from tabulate import tabulate
from tqdm import tqdm

from config import CHAT_DB_PATH, USER_MAP

plt.style.use("seaborn-v0_8-darkgrid")

TZ = ZoneInfo("America/New_York")
MAX_ME_COUNT = 3
MEABLE_TIMEOUT_HOURS = 24
SELF_ME_DELAY_SECONDS = 30


def from_typedstream(data: bytes) -> str:
    ts = typedstream.stream.TypedStreamReader.from_data(data)
    unarchiver = typedstream.Unarchiver(ts)
    root = unarchiver.decode_single_root()

    strings = [
        obj
        for obj in root.contents
        if isinstance(obj, typedstream.archiving.TypedValue)
        and isinstance(obj.value, typedstream.types.foundation.NSString)
    ]

    if not strings:
        raise ValueError(f"{data=} doesn't have a string in it")

    if len(strings) > 1:
        raise ValueError(strings)

    typed_value: typedstream.types.foundation.NSString = strings[0].value
    return typed_value.value


class Message:
    def __init__(self, row: sqlite3.Row) -> None:
        self.id: str = row["id"]

        # https://www.epochconverter.com/coredata
        self.date = datetime.datetime.fromtimestamp(
            row["date"] / 1e9 + 978_307_200, datetime.UTC
        ).astimezone(TZ)

        self.has_attachment = bool(row["has_attachment"])

        # read from attributedBody first
        # TODO: maybe switch to message_summary_info?
        self.text = (
            from_typedstream(row["attributedBody"])
            if row["attributedBody"] is not None
            else str(row["text"])
        )

        self.spark = (
            self.date.hour == 16
            and self.date.minute == 20
            and self.text.lower().strip() == "spark"
        )

        self.spark_cheat = self.spark and self.date.microsecond == 0

        self.me = False
        self.not_me = False

        # TODO: is the me supposed to be alone?
        if match := re.search(r"^\s*(not)?\s*me\W*$", self.text, re.IGNORECASE):
            if match.group(1) is None:
                self.me = True
            else:
                self.not_me = True

        self.meable = (
            (
                self.has_attachment
                and (
                    row["balloon_bundle_id"] is None
                    or "gamepigeon" not in row["balloon_bundle_id"]
                )
            )
            or re.search(r"https?://", self.text, re.IGNORECASE)
            or re.search(r"^Wordle \d+ \d", self.text)
        )

    def __str__(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        return f"Message(date={repr(self.date.isoformat(timespec='microseconds'))}, id={repr(self.id)}, name={repr(USER_MAP[self.id])} text={repr(self.text)})"


class MeableMessage(Message):
    def __init__(self, row: sqlite3.Row) -> None:
        super().__init__(row)
        self.mes: set[str] = set()


class User:
    def __init__(self, name: str) -> None:
        self.name = name

        self.mes: int = 0
        self.not_mes: int = 0
        self.dates: list[datetime.datetime] = []

        self.sparks: int = 0
        self.spark_dates: list[datetime.datetime] = []

        self.spark_cheats: int = 0

        self.meable_message_count: int = 0
        self.own_mes_count: int = 0

    @property
    def total(self) -> int:
        return self.mes + self.not_mes

    def __repr__(self) -> str:
        return f"User(name={repr(self.name)}, mes={self.mes}, not_mes={self.not_mes}, sparks={self.sparks}, spark_cheats={self.spark_cheats})"

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, User):
            return NotImplemented

        return self.total < other.total


def _get_month_start(date: datetime.datetime) -> datetime.datetime:
    return date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _increment_month(month: datetime.datetime) -> datetime.datetime:
    if month.month == 12:
        return month.replace(year=month.year + 1, month=1)
    return month.replace(month=month.month + 1)


def _generate_monthly_counts(
    dates: list[datetime.datetime],
    start_date: datetime.datetime,
    end_date: datetime.datetime,
) -> tuple[list[datetime.date], list[int]]:
    month_counts = Counter(_get_month_start(date) for date in dates)

    months: list[datetime.date] = []
    counts: list[int] = []
    month = _get_month_start(start_date)
    end_month = _get_month_start(end_date)

    while month <= end_month:
        months.append(month)
        counts.append(month_counts[month])
        month = _increment_month(month)

    return months, counts


def _filter_meable_messages(
    meable_msgs: list[MeableMessage],
    predicate: Callable[[MeableMessage], bool],
) -> tuple[list[MeableMessage], list[MeableMessage]]:
    kept: list[MeableMessage] = []
    removed: list[MeableMessage] = []

    for meable in meable_msgs:
        if predicate(meable):
            kept.append(meable)
        else:
            removed.append(meable)

    return kept, removed


def _format_axes(
    ax: plt.Axes,  # pyright: ignore[reportPrivateImportUsage]
    xlabel: str,
    ylabel: str,
) -> None:
    locator = mdates.MonthLocator(interval=3)
    formatter = mdates.DateFormatter("%Y-%m")

    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend()


users = {name: User(name) for name in USER_MAP.values()}

meable_msgs: list[MeableMessage] = []
old_meable: list[MeableMessage] = []

last_spark = datetime.date(1, 1, 1)

now = datetime.datetime.now(TZ)

with sqlite3.connect(CHAT_DB_PATH) as con:
    con.row_factory = sqlite3.Row

    cur = con.cursor()

    with open("messages_query.sql", "r") as f:
        query = f.read()

    rows = list(cur.execute(query))

    first_row_date = Message(rows[0]).date
    first_message_date = now

    for row in tqdm(rows):
        msg = Message(row)
        user = users[USER_MAP[msg.id]]

        # if msg.date < datetime.datetime(2026, 1, 1, tzinfo=TZ):
        #     continue

        first_message_date = min(first_message_date, msg.date)

        # if "spark" in msg.text.lower():
        #     print(msg)

        if msg.spark:
            # print(msg)
            if msg.spark_cheat:
                user.spark_cheats += 1
                continue

            if msg.date.date() > last_spark:
                user.sparks += 1

                last_spark = msg.date.date()

                user.spark_dates.append(msg.date)

            continue

        if msg.meable:
            user.meable_message_count += 1
            meable_msgs.append(MeableMessage(row))
            continue

        if msg.me or msg.not_me:
            # remove meable that are 24+ hours old
            meable_msgs, old_removed = _filter_meable_messages(
                meable_msgs,
                lambda m: msg.date
                < m.date + datetime.timedelta(hours=MEABLE_TIMEOUT_HOURS),
            )
            old_meable.extend(old_removed)

            for meable in sorted(meable_msgs, key=lambda m: m.date):
                # if already me'd, look at next message
                if msg.id in meable.mes:
                    continue

                # can't be first me to own message unless 30 seconds have passed
                if (
                    msg.id == meable.id
                    and len(meable.mes) == 0
                    and msg.date
                    < meable.date + datetime.timedelta(seconds=SELF_ME_DELAY_SECONDS)
                ):
                    continue

                meable.mes.add(msg.id)

                if msg.id == meable.id:
                    user.own_mes_count += 1

                if msg.me:
                    user.mes += 1
                else:
                    user.not_mes += 1

                user.dates.append(msg.date)

                break

            meable_msgs, full_removed = _filter_meable_messages(
                meable_msgs, lambda m: len(m.mes) < MAX_ME_COUNT
            )
            old_meable.extend(full_removed)

print(f"gap check since: {first_message_date}\n")

output: list[list[str | int | float]] = [
    [
        user.name,
        user.mes,
        user.not_mes,
        user.total,
        user.sparks,
        user.spark_cheats,
        user.meable_message_count,
        user.own_mes_count,
        user.own_mes_count / user.meable_message_count * 100
        if user.meable_message_count > 0
        else "-",
    ]
    for user in sorted(users.values(), reverse=True)
]

print(
    tabulate(
        output,
        headers=[
            "user",
            "mes",
            "not mes",
            "total",
            "sparks",
            "spark cheats",
            "meable messages",
            "own mes",
            "own mes %",
        ],
    )
)

print(f"{sum(user.total for user in users.values())=}")
print(f"{len(old_meable) * 3=}")

fig_totals, axs_totals = plt.subplots(2, 1)
fig_rates, axs_rates = plt.subplots(2, 1)

ax_mes: plt.Axes = axs_totals[0]  # pyright: ignore[reportPrivateImportUsage]
ax_me_rate: plt.Axes = axs_rates[0]  # pyright: ignore[reportPrivateImportUsage]
ax_sparks: plt.Axes = axs_totals[1]  # pyright: ignore[reportPrivateImportUsage]
ax_spark_rate: plt.Axes = axs_rates[1]  # pyright: ignore[reportPrivateImportUsage]

for user in sorted(users.values(), reverse=True):
    dates = user.dates
    counts = [i + 1 for i in range(len(dates))]
    dates.append(now)

    if counts:
        counts.append(counts[-1])
    else:
        counts.append(0)

    ax_mes.plot(
        dates,  # pyright: ignore[reportArgumentType]
        counts,
        label=user.name,
    )

    months, monthly_counts = _generate_monthly_counts(dates, first_message_date, now)
    ax_me_rate.plot(
        months,  # pyright: ignore[reportArgumentType]
        monthly_counts,
        label=user.name,
    )

for user in sorted(users.values(), key=lambda user: user.sparks, reverse=True):
    dates = user.spark_dates
    counts = [i + 1 for i in range(len(dates))]
    dates.append(now)

    if counts:
        counts.append(counts[-1])
    else:
        counts.append(0)

    ax_sparks.plot(
        dates,  # pyright: ignore[reportArgumentType]
        counts,
        label=user.name,
    )

    months, monthly_counts = _generate_monthly_counts(dates, first_message_date, now)
    ax_spark_rate.plot(
        months,  # pyright: ignore[reportArgumentType]
        monthly_counts,
        label=user.name,
    )

_format_axes(ax_mes, "date", "(not) me count")
_format_axes(ax_me_rate, "date", "me rate per month")
_format_axes(ax_sparks, "date", "spark count")
_format_axes(ax_spark_rate, "date", "spark rate per month")

fig_totals.suptitle(f"gap check since {first_message_date}")
fig_rates.suptitle(f"gap check since {first_message_date}")
plt.show()

shutil.copyfile(
    CHAT_DB_PATH, f"chat.db.since{first_row_date.strftime('%Y%m%d%H%M')}.bak"
)
