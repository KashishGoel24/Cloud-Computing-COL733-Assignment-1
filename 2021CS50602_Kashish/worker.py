import logging
import sys
import time
import pandas as pd
from typing import Any

from base import Worker
from config import config
from mrds import MyRedis


class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']
    # Write the code for the worker thread here.
    while True:
      # Read
      try:
        result = rds.rds.xreadgroup(groupname=Worker.GROUP, consumername=self.name, streams={config["IN"]: ">"}, count=1)
      except:
        continue

      if self.crash:
        # DO NOT MODIFY THIS!!!
        logging.critical(f"CRASHING!")
        sys.exit()

      if self.slow:
        # DO NOT MODIFY THIS!!!
        logging.critical(f"Sleeping!")
        time.sleep(1)

      if not result:
        try:
          if (rds.is_pending()):
            result = rds.rds.xautoclaim(config["IN"], Worker.GROUP, self.name, 5000, '0-0', 1)      # checl what idle time you want to enter here
            # can try to add an optimisation here where you try to claim those files first for which the old assignee has crashed or slept
            messages = result[1]
            if not result:
              continue  # not breaking here cause have to exit only once is_pending returns false
          else:
            logging.info(f"No more files for {self.name}. Exiting.")
            break 
        except:
          continue
      else:
        stream_name, messages = result[0]

      if not messages:
        continue

      message_id, data = messages[0]
      file_name = data.get(config["FNAME"].encode('utf-8'))
      file_name = file_name.decode('utf-8')

      logging.info(f"{self.name} processing file: {file_name}")

      wc = {}
      df = pd.read_csv(file_name, lineterminator='\n')
      df["text"] = df["text"].astype(str)
        
      for text in df["text"]:
          if text == '\n':
            continue
          for word in text.split(" "):
            # print("i am worker ",self.name," word i have got is:",word)
            if word not in wc:
                wc[word] = 0
            wc[word] += 1

      # for word, count in wc.items():
      #   rds.rds.zincrby(config["COUNT"], count, word)
      # rds.rds.xack(config["IN"], Worker.GROUP, message_id)

      acked = 0
      while (not acked):
        # print("waiting to ack")
        try:
          ack_result = rds.executeWordCountAck(Worker.GROUP, message_id, wc)
          acked = 1
        except:
          continue

      logging.info(f"{self.name} completed processing {file_name}")

      # Process here

    logging.info(f"Exiting for worker {self.name}")