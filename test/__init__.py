
import pytest
from generate_metrics import get_job_ids

def test_valid_job_id():
    bjobs_output = """
    JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
    28250   user_example   RUN   q_residual mn325       g1_r        /bin/bash  Sep  3 22:17
    17381   user_example   EXIT  q_residual mn325          -        /bin/bash  Aug 28 13:36
    17382   user_example   DONE  q_htc      mn325       16*g1       /bin/bash  Aug 28 13:37
    17392   user_example   EXIT  q_htc      mn325          -        *em=65536] Aug 28 14:15
    17395   user_example   EXIT  q_htc      mn325       16*g1       *em=60000] Aug 28 14:24
    17445   user_example   DONE  q_htc      mn325       2*g1        *ifgram_89 Aug 28 16:51
    21182   user_example   DONE  q_residual mn325       16*g1       /bin/bash  Sep  1 12:32
    21573   user_example   EXIT  q_residual mn325          -        /bin/bash  Sep  1 13:40
    21574   user_example   EXIT  q_residual mn325       16*g1       /bin/bash  Sep  1 13:41
    21618   user_example   EXIT  q_residual mn325          -        /bin/bash  Sep  1 13:57
    21619   user_example   EXIT  q_residual mn325       16*g1       /bin/bash  Sep  1 13:57
    21630   user_example   DONE  q_residual mn325       16*g1       /bin/bash  Sep  1 14:15
    21633   user_example   DONE  q_residual mn325       16*g1       /bin/bash  Sep  1 14:18
    22394   user_example   DONE  q_residual mn325       16*g1_r     /bin/bash  Sep  1 20:55
    22397   user_example   DONE  q_residual mn325       16*g1_r     /bin/bash  Sep  1 20:59
    22399   user_example   EXIT  q_residual mn325       16*g1_r     /bin/bash  Sep  1 21:04
    22402   user_example   EXIT  q_residual mn325       16*g1_r     /bin/bash  Sep  1 21:23
    22404   user_example   EXIT  q_residual mn325       12*g1_r     /bin/bash  Sep  1 21:40
                                                        4*g1
    28240   user_example   EXIT  q_residual mn325       g1_r        */jobs.csv Sep  3 21:47
    28241   user_example   EXIT  q_residual mn325       g1_r        /bin/bash  Sep  3 21:48
    """
    excepted = [28250, 17381, 17382, 17392, 17395, 17445, 21182, 21573, 21574, 21618, 21619, 21630, 21633, 22394, 22397, 22399, 22402, 22404, 28240, 28241]
    assert get_job_ids(bjobs_output) == excepted