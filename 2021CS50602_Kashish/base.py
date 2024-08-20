from __future__ import annotations

import logging
import os
import signal
import sys
from abc import abstractmethod, ABC
from threading import current_thread
from typing import Any, Final
from multiprocessing import Process


class Worker(ABC):
  GROUP: Final = "worker"
  counter = 0

  def __init__(self, **kwargs: Any):
    Worker.counter += 1
    self.name = f"worker-{Worker.counter}"
    self.pid = -1
    self.crash = kwargs['crash'] if 'crash' in kwargs else False
    self.slow = kwargs['slow'] if 'slow' in kwargs else False

  def create_and_run(self, **kwargs: Any) -> None:
    # Create a process using the multiprocessing library and set self.pid
    def target():
      self.run(**kwargs)
    self.process = Process(target=target, name=self.name)
    self.process.start()
    self.pid = self.process.pid
    logging.info(f"Started {self.name} with PID {self.pid}")
    # raise NotImplementedError

  @abstractmethod
  def run(self, **kwargs: Any) -> None:
    raise NotImplementedError

  def kill(self) -> None:
    logging.info(f"Killing {self.name}")
    os.kill(self.pid, signal.SIGKILL)
