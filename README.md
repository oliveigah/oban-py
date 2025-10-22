
```python
# myapp/hard_worker.py
from oban import job, worker

@job(queue="exports", tags=["func"])
def some_work(foo, bar):
    ...

@worker(priority=2, queue="mailers")
class MailWorker:
    async def process(job):
        ...

# myapp/oban.py
from myapp.db import engine
from oban import Oban

oban = Oban(
    connection=engine,
    queues={"default": 10, "mailers": 10, "exports": 4},
    pruner={"max_age": 10}
)

# web.py (client - enqueue only)
from myapp import some_work, MailWorker

@app.route("/send-email")
def send_email():
    some_work.enqueue("foo", "bar"),
    some_work.enqueue("bim", "bap"),
    MailWorker.enqueue({"email": "foo@example.com")

# worker.py (server - full background processing)
from myapp.oban import oban

if __name__ == "__main__":
    # Oban is a context manager, this will call `start()` and then `stop()`
    with oban:
        signal.pause()
```

```python
# From my application code:
from app import hard_work, HardWorker

async def main():
    await hard_work.enqueue("foo", "bar")
    await HardWorker.enqueue({"foo": "bar"})

# Or enqueue many at once
from app import hard_work, oban

def main():
  oban.enqueue_many(
    hard_work.new("foo", "bar"),
    hard_work.new("bim", "bap"),
  )
```
