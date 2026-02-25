import fcntl
from pathlib import Path
from datetime import datetime, timedelta

CLIENTS_FILE = Path("clients.txt")
CLIENTS_TTL_DAYS = 180  # считаем "новым", если не писал столько дней

def is_new_client(user_id: int) -> bool:
    CLIENTS_FILE.touch(exist_ok=True)

    now = datetime.now()
    ttl = timedelta(days=CLIENTS_TTL_DAYS)
    uid = str(int(user_id))

    # формат строк: "user_id ISO_TIMESTAMP"
    with open(CLIENTS_FILE, "r+", encoding="utf-8") as f:
        fcntl.flock(f, fcntl.LOCK_EX)

        lines = f.read().splitlines()
        known: dict[str, datetime] = {}

        for line in lines:
            line = line.strip()
            if not line:
                continue
            parts = line.split(maxsplit=1)
            if len(parts) != 2:
                continue
            k, ts = parts
            try:
                known[k] = datetime.fromisoformat(ts)
            except Exception:
                continue

        is_new = True
        last = known.get(uid)
        if last is not None and (now - last) < ttl:
            is_new = False

        # обновляем last_seen для текущего пользователя
        known[uid] = now

        # чистим устаревшие записи и перезаписываем файл
        f.seek(0)
        f.truncate()
        for k, ts in known.items():
            if (now - ts) < ttl:
                f.write(f"{k} {ts.isoformat()}\n")

        fcntl.flock(f, fcntl.LOCK_UN)

    return is_new
