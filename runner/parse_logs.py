import bisect
import collections
import sys

""" Class that replays the execution of the stage."""
class Simulation:
  def __init__(self, tasks):
    self.SLOTS = 40
    self.runtime = 0
    self.runtime_without_fetch = 0

    if len(tasks) < 40:
      self.runtime = max([t.runtime() for t in tasks])
      self.runtime_without_fetch = max([t.runtime_without_fetch() for t in tasks])
      print "Runtime: %s, without fetch: %s" % (self.runtime, self.runtime_without_fetch)
    else:
      # Make a copy of tasks to pass to simulate, because simulate modifies the list.
      self.runtime = self.simulate(list(tasks), Task.runtime)
      print "Runtime:", self.runtime
      self.runtime_without_fetch = self.simulate(list(tasks), Task.runtime_without_fetch)
      print "Runtime without fetch:", self.runtime_without_fetch

  def simulate(self, tasks, runtime_function):
    print "Simulationg for %s tasks" % len(tasks)
    # Sorted list of task finish times.
    finish_times = []
    # Start 40 tasks.
    while len(finish_times) < 40:
      runtime = runtime_function(tasks.pop(0))
      print "Adding task with runtime %s" % runtime
      bisect.insort_left(finish_times, runtime)

    while len(tasks) > 0:
      start_time = finish_times.pop(0)
      finish_time = start_time + runtime_function(tasks.pop(0))
      print finish_times
      print "Task starting at ", start_time, " finishing at", finish_time
      bisect.insort_left(finish_times, finish_time)

    # Job finishes when the last task is done.
    return finish_times[-1]

class Task:
  def __init__(self, start_time, fetch_wait, finish_time):
    self.start_time = start_time
    self.fetch_wait = fetch_wait
    self.finish_time = finish_time

  def runtime(self):
    return self.finish_time - self.start_time

  def runtime_without_fetch(self):
    return self.finish_time - self.start_time - self.fetch_wait

class Stage:
  def __init__(self):
    # TODO: Add a JobLogger event for when the stage arrives.
    self.start_time = -1
    self.finish_time = -1
    self.finish_time_without_shuffle = -1
    self.tasks = []
    self.has_fetch = False
    self.num_tasks = 0
    self.fetch_wait_fractions = []


  def __str__(self):
    if self.has_fetch:
      self.fetch_wait_fractions.sort()
      return ("%s tasks Start: %s, finish: %s, finish without shuffle: %s" %
        (self.num_tasks, self.start_time, self.finish_time, self.finish_time_without_shuffle))
    else:
      return "%s tasks Start: %s, finish: %s" % (self.num_tasks, self.start_time, self.finish_time)

  def total_runtime(self):
    return sum([t.finish_time - t.start_time for t in self.tasks])

  def approximate_runtime(self):
    if self.num_tasks > 40:
      return self.total_runtime() / 40.
    return self.total_runtime() * 1.0 / self.num_tasks

  def total_runtime_without_fetch(self):
    return sum([t.finish_time - t.fetch_wait - t.start_time for t in self.tasks])

  def approximate_runtime_without_fetch(self):
    if self.num_tasks > 40:
      return self.total_runtime_without_fetch() / 40.
    return self.total_runtime_without_fetch() * 1.0 / self.num_tasks

  def add_event(self, line):
    if line.find("TASK_TYPE") == -1:
      return
    self.num_tasks += 1
    print line
    print self

    items = line.split(" ")

    start_time = -1
    fetch_wait = -1
    finish_time = -1
    for pair in items:
      if pair.find("=") == -1:
        continue
      key, value = pair.split("=")
      if key == "START_TIME":
        start_time = int(value)
      elif key == "FINISH_TIME":
        finish_time = int(value)
      elif key == "REMOTE_FETCH_WAIT_TIME":
        fetch_wait = int(value)

    print start_time, finish_time, fetch_wait

    if (start_time == -1 or finish_time == -1 or
        (self.has_fetch and fetch_wait == -1)):
      print ("Missing time on line %s! Start %s, fetch wait %s, finish %s" %
        (line, start_time, fetch_wait, finish_time))

    if self.start_time == -1:
      self.start_time = start_time
    else:
      self.start_time = min(self.start_time, start_time)

    if self.finish_time == -1:
      self.finish_time = finish_time
    else:
      self.finish_time = max(self.finish_time, finish_time)

    if fetch_wait != -1:
      self.tasks.append(Task(start_time, fetch_wait, finish_time))
      self.has_fetch = True
      finish_time_without_shuffle = finish_time - fetch_wait
      if self.finish_time_without_shuffle == -1:
        self.finish_time_without_shuffle = finish_time_without_shuffle
      else:
        self.finish_time_without_shuffle = max(
          self.finish_time_without_shuffle, finish_time_without_shuffle)
      self.fetch_wait_fractions.append(fetch_wait * 1.0 / (finish_time - start_time))
    else:
      self.tasks.append(Task(start_time, 0, finish_time))

    print self

def main(argv):
  filename = argv[0]
  f = open(filename, "r")
  # Map of stage IDs to Stages.
  stages = collections.defaultdict(Stage)

  for line in f:
    STAGE_ID_MARKER = "STAGE_ID="
    stage_id_loc = line.find(STAGE_ID_MARKER)
    if stage_id_loc != -1:
      stage_id_and_suffix = line[stage_id_loc + len(STAGE_ID_MARKER):]
      stage_id = stage_id_and_suffix[:stage_id_and_suffix.find(" ")]
      stages[stage_id].add_event(line)

  total_time = 0
  total_time_without_shuffle = 0
  approx_total_time = 0
  approx_total_time_without_shuffle = 0

  simulated_total_time = 0
  simulated_total_time_without_shuffle = 0
  for id, stage in stages.iteritems():
    print "***********", id, stage
    stage_run_time = stage.finish_time - stage.start_time
    total_time += stage_run_time
    print "Total time: ", total_time
    print "Total runtime: ", stage.total_runtime(), "total w/o fetch:", stage.total_runtime_without_fetch()
    print "New speedup: ", stage.total_runtime_without_fetch() * 1.0 / stage.total_runtime()
    if stage.has_fetch:
      time_without_shuffle = stage.finish_time_without_shuffle - stage.start_time
      print ("Run time: %s, without shuffle: %s, Speedup: %s" %
        (stage_run_time, time_without_shuffle, time_without_shuffle * 1.0 / stage_run_time))
      total_time_without_shuffle += time_without_shuffle
    else:
      total_time_without_shuffle += stage.finish_time - stage.start_time

    approx_total_time += stage.approximate_runtime()
    approx_total_time_without_shuffle += stage.approximate_runtime_without_fetch()

    s = Simulation(stage.tasks)
    simulated_total_time += s.runtime
    simulated_total_time_without_shuffle += s.runtime_without_fetch

  speedup = total_time_without_shuffle * 1.0 / total_time
  # TODO: this overestimates the speedup, because stages can be overlapping!
  print ("Total time: %s, without shuffle: %s, speedup: %s" %
    (total_time, total_time_without_shuffle, speedup))

  print ("Approx total: %s, without shuffle: %s, speedup %s" %
    (approx_total_time, approx_total_time_without_shuffle, approx_total_time_without_shuffle * 1.0 / approx_total_time))

  print "Sim %s without %s speedup %s" % (simulated_total_time, simulated_total_time_without_shuffle, simulated_total_time_without_shuffle * 1.0 / simulated_total_time)
    
if __name__ == "__main__":
  main(sys.argv[1:])
