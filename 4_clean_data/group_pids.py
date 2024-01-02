"""
  Group matched pids pairs as cluster of matched pids using BFS
"""
from collections import deque
import pandas as pd
import gc

def table_to_dict(table):
  # Convert table to dictionary/hash map to perform bfs
  pid_map = dict()
  
  for index, row in table.iterrows():
    if row["pid_l"] not in pid_map:
      # create empty set if id is not in map before 
      pid_map[row["pid_l"]] = set()
      
    if row["pid_r"] not in pid_map:
      pid_map[row["pid_r"]] = set()

    pid_map[row["pid_l"]].add(row["pid_r"])
    pid_map[row["pid_r"]].add(row["pid_l"])
    
  return pid_map


def bfs(pid_map, curr_pid, visited):
  """
    Use bfs to get all pids matched to the current pid 
    pid_map: dictionary created by table_to_dict() function
    curr_pid: current pid to check
    visited: keep track of global visited nodes (i.e. pids that have been added to a cluster)
  """
  curr_cluster = list()
  frontier = deque()
  frontier.append(curr_pid)
  
  
  while len(frontier) > 0:
    curr_node = frontier.popleft()
    visited.add(curr_node)
    curr_cluster.append(curr_node)

    for neighbor in pid_map[curr_node]:
      if neighbor not in visited and neighbor not in frontier:
        frontier.append(neighbor)
        
  return curr_cluster
        
  
def matched_pids(pid_map):
  visited = set()
  clusters = list()
  
  for pid in pid_map:
    if pid not in visited:
      curr_cluster = bfs(pid_map, curr_pid=pid, visited=visited)
      clusters.append(curr_cluster)
  return clusters

def main():
  # --- Create clusters of matched pids
  # Read tables 
  deterministic_table = pd.read_parquet("./linked_data/deterministic_linking_pairs.parquet", engine = "pyarrow").astype("string")
  probabilistic_table = pd.read_parquet("./linked_data/post_inspection_linked_pairs.parquet", columns=["pid_l", "pid_r"], engine = "pyarrow").astype("string")

  # Concatenate pairs from deterministic linking and probabilistic linking (after manual inspection)
  concat_table = pd.concat([deterministic_table, probabilistic_table])

  # Convert table to dicitonary
  pid_map = table_to_dict(concat_table)
  print(len(pid_map))
  
  # Compute clusters of matched pids
  clusters = matched_pids(pid_map)
  print(len(clusters))

  # Overview of cluster
  # max_cluster = 0
  # min_cluster = 20
  # largest_cluster = None
  # for cluster in clusters:
  #   max_cluster = max(max_cluster, len(cluster))
  #   if max_cluster == len(cluster): largest_cluster = cluster 
  #   min_cluster = min(min_cluster, len(cluster))

  # print(f"Largest number of duplicated pid for a child is {max_cluster}")
  # print(f"Smallest number of duplicated pid for a child is {min_cluster}")
  # print(largest_cluster)

  
  # --- Select representitive pid of each cluster based on vacdate
  # read personal info table as dictionary
  personal_info = pd.read_parquet("/cluster_data/vrdata/standardized/personal_info.parquet", columns=["pid", "vacdate"], engine = "pyarrow")
  personal_info["pid"] = personal_info["pid"].astype("string")
  # convert to dictionary for faster vacdate retrievel
  pid_vacdate = dict()
  for index, row in personal_info.iterrows():
    pid_vacdate[row["pid"]] = row["vacdate"]
  print(len(pid_vacdate))
  del personal_info
  gc.collect()

  # choose pid with the latest vacdate as the representitive pid for each matched cluster
  pid_cluster = dict() # dictionary of rep pid point to list of pids in the cluster
  for cluster in clusters:
    lastest_vacdate = pid_vacdate[cluster[0]]
    rep_pid = cluster[0]
    for pid in cluster:
      if pid_vacdate[pid]>lastest_vacdate:
        lastest_vacdate = pid_vacdate[pid]
        rep_pid = pid
    
    pid_cluster[rep_pid] = cluster
  print(len(pid_cluster))

  # --- Convert dictionary to pandas dataframe 
  # create table with old_pid and new_pid columns
  # old_pid - non-unique pid in the original dataset
  # new_pid - updated pid (i.e. representative pid of each cluster)
  pid_mapping = pd.DataFrame(columns = ["old_pid", "unified_pid"])
  for rep_pid, old_pids in pid_cluster.items():
    for pid in old_pids:
      pid_mapping = pd.concat([pid_mapping, pd.DataFrame({"old_pid":pid, "unified_pid":rep_pid}, index=[0])])
    
  print(pid_mapping.shape)
  # --- Save dataframe as a parquet
  pid_mapping.to_parquet("./unified_pid.parquet")


if __name__ == "__main__":
  main()
  
