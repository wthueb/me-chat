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

first_message_date = datetime.datetime.now(datetime.UTC)

with sqlite3.connect(CHAT_DB_PATH) as con:
    con.row_factory = sqlite3.Row

    cur = con.cursor()

    with open("messages_query.sql", "r") as f:
        query = f.read()

    for row in tqdm(list(cur.execute(query))):
        msg = Message(row)
        user = users[USER_MAP[msg.id]]

        first_message_date = min(msg.date, first_message_date)

        if msg.spark:
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
            #     if msg.date < meable.date + timedelta(hours=24) and len(meable.mes) < 3  # noqa
            # ]

            new_meable = []
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

output: list[list] = []

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

fig, axs = plt.subplots(2, 1)

ax_mes: plt.Axes = axs[0]  # pyright: ignore[reportPrivateImportUsage]
ax_sparks: plt.Axes = axs[1]  # pyright: ignore[reportPrivateImportUsage]

for user in sorted(users.values(), reverse=True):
    dates = user.dates
    counts = [i + 1 for i in range(len(dates))]
    dates.append(datetime.datetime.now())
    counts.append(counts[-1])
    ax_mes.plot(
        user.dates,  # pyright: ignore[reportArgumentType]
        counts,
        label=user.name,
    )

for user in sorted(users.values(), key=lambda user: user.sparks, reverse=True):
    dates = user.spark_dates
    counts = [i + 1 for i in range(len(dates))]
    dates.append(datetime.datetime.now())
    if counts:
        counts.append(counts[-1])
    else:
        counts.append(0)
    ax_sparks.plot(
        user.spark_dates,  # pyright: ignore[reportArgumentType]
        counts,
        label=user.name,
    )

quarter_year_locator = mdates.MonthLocator(interval=3)
ax_mes.xaxis.set_major_locator(quarter_year_locator)
ax_mes.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax_mes.set_xlabel("date")
ax_mes.set_ylabel("(not) me count")
ax_mes.legend()

quarter_year_locator = mdates.MonthLocator(interval=3)
ax_sparks.xaxis.set_major_locator(quarter_year_locator)
ax_sparks.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax_sparks.set_xlabel("date")
ax_sparks.set_ylabel("spark count")
ax_sparks.legend()

fig.suptitle(f"gap check since {first_message_date}")
plt.show()

shutil.copyfile(
    CHAT_DB_PATH, f"chat.db.since{first_message_date.strftime('%Y%m%d%H%M')}.bak"
)
