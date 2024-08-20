from mrds import MyRedis
import threading
import time

def create_checkpoints(rds: MyRedis, interval: int):
  def checkpoint_subfunction():
    while True:
      try:
        if (rds.is_pending()):
          try:
            rds.rds.bgsave()
            print("created checkpoint")
            time.sleep(interval)
          except:
            continue
        else:
          print("completed checkpointing")
          break
      except:
        continue

  thread = threading.Thread(target=checkpoint_subfunction, daemon=True)
  thread.start()
  # pass