import datetime
import re
import shutil
import sqlite3
from zoneinfo import ZoneInfo

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import typedstream
from tabulate import tabulate
from tqdm import tqdm

from config import CHAT_DB_PATH, USER_MAP

plt.style.use("seaborn-v0_8-darkgrid")

TZ = ZoneInfo("America/New_York")


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

    if strings:
        # multiple strings in the body, that doesn't make sense
        if len(strings) > 1:
            raise ValueError(strings)

        typed_value: typedstream.types.foundation.NSString = strings[0].value

        return typed_value.value

    raise ValueError(f"{data=} doesn't have a string in it")


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
        if row["attributedBody"] is not None:
            self.text = from_typedstream(row["attributedBody"])
        else:
            self.text = str(row["text"])

        self.spark = (
            self.date.hour == 16
            and self.date.minute == 20
            and self.text.lower().strip() == "spark"
        )

        self.spark_cheat = self.spark and self.date.microsecond == 0

        self.me, self.not_me = False, False

        # TODO: is the me supposed to be alone?
        if match := re.search(r"^\s*(not)?\s*me\W*$", self.text, re.IGNORECASE):
            if match.groups()[0] is None:
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

        #  print(", ".join(map(str, row)))

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

    @property
    def total(self) -> int:
        return self.mes + self.not_mes

    def __repr__(self) -> str:
        return f"User(name={repr(self.name)}, mes={self.mes}, not_mes={self.not_mes}, sparks={self.sparks}, spark_cheats={self.spark_cheats})"

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, User):
            return NotImplemented

        return self.total < other.total


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

        # if msg.date < datetime.datetime(2024, 1, 1, tzinfo=TZ):
        #     continue

        first_message_date = min(first_message_date, msg.date)

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
            meable_msgs.append(MeableMessage(row))
            continue

        if msg.me or msg.not_me:
            # remove meable that are 24+ hours old
            # meable_msgs = [
            #     meable
            #     for meable in meable_msgs
            #     if msg.date < meable.date + timedelta(hours=24) and len(meable.mes) < 3
            # ]

            new_meable: list[MeableMessage] = []
            for meable in meable_msgs:
                if msg.date < meable.date + datetime.timedelta(hours=24):
                    new_meable.append(meable)
                else:
                    old_meable.append(meable)
            meable_msgs = new_meable

            for meable in sorted(meable_msgs, key=lambda m: m.date):
                # if already me'd, look at next message
                if msg.id in meable.mes:
                    continue

                # can't be first me to own message unless 30 seconds have passed
                if (
                    msg.id == meable.id
                    and len(meable.mes) == 0
                    and msg.date < meable.date + datetime.timedelta(seconds=30)
                ):
                    continue

                meable.mes.add(msg.id)

                if msg.me:
                    user.mes += 1
                else:
                    user.not_mes += 1

                user.dates.append(msg.date)

                break

            new_meable = []
            for meable in meable_msgs:
                if len(meable.mes) < 3:
                    new_meable.append(meable)
                else:
                    old_meable.append(meable)
            meable_msgs = new_meable

print(f"gap check since: {first_message_date}\n")

output: list[list[str | int]] = []

for user in sorted(users.values(), reverse=True):
    output.append(
        [
            user.name,
            user.mes,
            user.not_mes,
            user.total,
            user.sparks,
            user.spark_cheats,
        ]
    )

print(
    tabulate(
        output,
        headers=["user", "mes", "not mes", "total", "sparks", "spark cheats"],
    )
)

print(f"{sum(user.total for user in users.values())=}")
print(f"{len(old_meable)*3=}")

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
    counts.append(counts[-1])
    ax_mes.plot(
        dates,  # pyright: ignore[reportArgumentType]
        counts,
        label=user.name,
    )

    month_counts: dict[datetime.datetime, int] = {}
    for date in dates:
        month = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        month_counts[month] = month_counts.get(month, 0) + 1

    months: list[datetime.date] = []
    counts: list[int] = []
    month = first_message_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    while month <= now.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
        months.append(month)
        counts.append(month_counts.get(month, 0))
        if month.month == 12:
            month = month.replace(year=month.year + 1, month=1)
        else:
            month = month.replace(month=month.month + 1)

    ax_me_rate.plot(
        months,  # pyright: ignore[reportArgumentType]
        counts,
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

    month_counts = {}
    for date in dates:
        month = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        month_counts[month] = month_counts.get(month, 0) + 1

    months = []
    counts = []

    month = first_message_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while month <= now.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
        months.append(month)
        counts.append(month_counts.get(month, 0))
        if month.month == 12:
            month = month.replace(year=month.year + 1, month=1)
        else:
            month = month.replace(month=month.month + 1)

    ax_spark_rate.plot(
        months,  # pyright: ignore[reportArgumentType]
        counts,
        label=user.name,
    )

quarter_year_locator = mdates.MonthLocator(interval=3)
date_formatter = mdates.DateFormatter("%Y-%m")

ax_mes.xaxis.set_major_locator(quarter_year_locator)
ax_mes.xaxis.set_major_formatter(date_formatter)
ax_mes.set_xlabel("date")
ax_mes.set_ylabel("(not) me count")
ax_mes.legend()

ax_me_rate.xaxis.set_major_locator(quarter_year_locator)
ax_me_rate.xaxis.set_major_formatter(date_formatter)
ax_me_rate.set_xlabel("date")
ax_me_rate.set_ylabel("me rate per month")
ax_me_rate.legend()

ax_sparks.xaxis.set_major_locator(quarter_year_locator)
ax_sparks.xaxis.set_major_formatter(date_formatter)
ax_sparks.set_xlabel("date")
ax_sparks.set_ylabel("spark count")
ax_sparks.legend()

ax_spark_rate.xaxis.set_major_locator(quarter_year_locator)
ax_spark_rate.xaxis.set_major_formatter(date_formatter)
ax_spark_rate.set_xlabel("date")
ax_spark_rate.set_ylabel("spark rate per month")
ax_spark_rate.legend()

fig_totals.suptitle(f"gap check since {first_message_date}")
fig_rates.suptitle(f"gap check since {first_message_date}")
plt.show()

shutil.copyfile(
    CHAT_DB_PATH, f"chat.db.since{first_row_date.strftime('%Y%m%d%H%M')}.bak"
)
