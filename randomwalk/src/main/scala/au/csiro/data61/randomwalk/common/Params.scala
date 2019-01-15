package au.csiro.data61.randomwalk.common

import au.csiro.data61.randomwalk.common.CommandParser.TaskName
import au.csiro.data61.randomwalk.common.CommandParser.TaskName.TaskName


case class Params(w2vIter: Int = 10,
                  w2vLr: Double = 0.025,
                  w2vPartitions: Int = 1,
                  w2vDim: Int = 128,
                  w2vWindow: Int = 10,
                  walkLength: Int = 80,
                  numWalks: Int = 10,
                  p: Double = 1.0,
                  q: Double = 1.0,
                  weighted: Boolean = false,
                  directed: Boolean = false,
                  output: String = null,
                  rddPartitions: Int = 200,
                  singleOutput: Boolean = true,
                  partitioned: Boolean = false,
                  cmd: TaskName = TaskName.node2vec,
                  contact_table_start_date: String = "",
                  contact_table_end_date: String = "",
                  user_table_date: String = "",
                  region_ids: Array[String] = null,
                  min_outdegree: Int = 2,
                  min_indegree: Int = 2,
                  max_outdegree: Int = 1000,
                  max_indegree: Int = 1000
                 ) extends AbstractParams[Params]