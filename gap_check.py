import re
import shutil
import sqlite3
import datetime

from tqdm import tqdm
import typedstream
from dateutil import tz
from tabulate import tabulate

from config import ADAM_EMAIL, ADAM_NUMBER, CHAT_DB_PATH, ME, USER_MAP


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
        row_dict = dict(row)

        self.id: str = row_dict["id"]

        # TODO: use is_from_me
        if self.id is None:
            self.id = ME
        elif self.id == ADAM_EMAIL:
            self.id = ADAM_NUMBER

        # https://www.epochconverter.com/coredata
        self.date = (
            datetime.datetime.fromtimestamp(
                row_dict["date"] / 1e9 + 978_307_200, datetime.UTC
            )
            .replace(tzinfo=tz.tzutc())
            .astimezone(tz.gettz("America/New_York"))
        )

        self.has_attachment = bool(row_dict["has_attachment"])

        # read from attributedBody first
        # TODO: maybe switch to message_summary_info?
        if row_dict["attributedBody"] is not None:
            self.text = from_typedstream(row_dict["attributedBody"])
        else:
            self.text = str(row_dict["text"])

        self.spark = (
            self.date.hour == 16
            and self.date.minute == 20
            and self.text.lower().strip() == "spark"
        )

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
        return f"Message(date={repr(self.date.isoformat(timespec="microseconds"))}, id={repr(USER_MAP[self.id]) if self.id in USER_MAP else repr(self.id)}, text={repr(self.text)})"


class MeableMessage(Message):
    def __init__(self, row: sqlite3.Row) -> None:
        super().__init__(row)
        self.mes: set[str] = set()


class User:
    def __init__(self, name: str) -> None:
        self.name = name
        self.mes: int = 0
        self.not_mes: int = 0
        self.sparks: int = 0

    def __str__(self) -> str:
        return (
            f"{self.name}: {self.mes} mes & {self.not_mes} not mes"
            f" ({self.mes + self.not_mes} total), {self.sparks} sparks"
        )


counts = {id: User(name) for id, name in USER_MAP.items()}

meable_msgs: list[MeableMessage] = []
old_meable: list[MeableMessage] = []

last_spark = datetime.date(1, 1, 1)

first_message_date = datetime.datetime.now().replace(tzinfo=tz.tzutc())

with sqlite3.connect(CHAT_DB_PATH) as con:
    con.row_factory = sqlite3.Row

    cur = con.cursor()

    with open("messages_query.sql", "r") as f:
        query = f.read()

    for row in tqdm(list(cur.execute(query))):
        msg = Message(row)

        first_message_date = min(msg.date, first_message_date)

        if msg.spark:
            if msg.date.date() > last_spark:
                # print(msg)
                counts[msg.id].sparks += 1

                last_spark = msg.date.date()

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
                    counts[msg.id].mes += 1
                else:
                    counts[msg.id].not_mes += 1

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

for user in sorted(
    counts.values(), key=lambda user: user.mes + user.not_mes, reverse=True
):
    output.append(
        [user.name, user.mes, user.not_mes, user.mes + user.not_mes, user.sparks]
    )

print(tabulate(output, headers=["user", "mes", "not mes", "total", "sparks"]))

print(f"{sum(user.mes + user.not_mes for user in counts.values())=}")
print(f"{len(old_meable)*3=}")

shutil.copyfile(
    CHAT_DB_PATH, f"chat.db.since{first_message_date.strftime('%Y%m%d%H%M')}.bak"
)
