from __future__ import annotations

import logging
import subprocess
import time
from typing import Optional, Final

from redis.client import Redis

from base import Worker
from config import config


class MyRedis:
  def __init__(self):
    self.rds: Final = Redis(host='localhost', port=6379, password=None,
                       db=0, decode_responses=False)
    self.rds.flushall()
    self.rds.xgroup_create(config["IN"], Worker.GROUP, id="0", mkstream=True)

  def add_file(self, fname: str):
    self.rds.xadd(config["IN"], {config["FNAME"]: fname})

  def top(self, n: int) -> list[tuple[bytes, float]]:
    return self.rds.zrevrangebyscore(config["COUNT"], '+inf', '-inf', 0, n,
                                     withscores=True)

  def executeWordCountAck(self, group, message_id, wc):
    wordCount = []
    for word, count in wc.items():
        wordCount.append(word)
        wordCount.append(count)
    return self.rds.fcall('atomicIncrementAndAck', 2, config["IN"], config["COUNT"], group, message_id, *wordCount)

  def is_pending(self) -> bool:
    pendingList = self.rds.xpending(config["IN"], Worker.GROUP)
    return pendingList['pending']>0
    # pass

  def restart(self, downtime: int):
    self.rds.shutdown()
    time.sleep(downtime)
    subprocess.run(["docker", "run", "-d", "-p", "6379:6379", "-v", "/home/baadalvm/redis:/data", "--name", "redis", "--rm", "redis:7.4"])
    time.sleep(1)
    # pass