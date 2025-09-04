from generate_metrics import get_job_ids, parse_bjobs_details
import pytest

def test_get_job_ids():
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
    assert get_job_ids(bjobs_output) == [28250, 17381, 17382, 17392, 17395, 17445, 21182, 21573, 21574, 21618, 21619, 21630, 21633, 22394, 22397, 22399, 22402, 22404, 28240, 28241]


def test_get_used_resources():
    bjobs = """
    Job <28320>, User <user_example>, Project <default>, Status <RUN>, Queue <q_residual>,
                     Interactive pseudo-terminal shell mode, Command </bin/bash
                     >, Share group charged </user_example>
Wed Sep  3 22:58:20: Submitted from host <mn325>, CWD </tmpu/desr_g/user_example>, Req
                     uested Resources <rusage[mem=5120]>;

 RUNLIMIT                
 240.0 min of mn61
Wed Sep  3 22:58:23: Started 1 Task(s) on Host(s) <mn61>, Allocated 1 Slot(s) o
                     n Host(s) <mn61>;
Wed Sep  3 23:22:29: Resource usage collected.
                     The CPU time used is 36 seconds.
                     IDLE_FACTOR(cputime/runtime):   0.02
                     MEM: 6 Mbytes;  SWAP: 243 Mbytes;  NTHREAD: 4
                     PGID: 51348;  PIDs: 51348 
                     PGID: 51361;  PIDs: 51361 
                     PGID: 51363;  PIDs: 51363 


 MEMORY USAGE:
 MAX MEM: 16 Mbytes;  AVG MEM: 7 Mbytes

 SCHEDULING PARAMETERS:
           r15s   r1m  r15m   ut      pg    io   ls    it    tmp    swp    mem
 loadSched   -     -     -     -       -     -    -     -     -      -      -  
 loadStop    -     -     -     -       -     -    -     -     -      -      -  

            ngpus ngpus_shared ngpus_excl_t ngpus_excl_p ngpus_prohibited 
 loadSched     -            -            -            -                -  
 loadStop      -            -            -            -                -  

          gpu_mode0 gpu_temp0 gpu_ecc0 gpu_temp1 gpu_ecc1 
 loadSched       -         -        -         -        -  
 loadStop        -         -        -         -        -  

 RESOURCE REQUIREMENT DETAILS:
 Combined: select[type == any ] order[r15s:pg] rusage[mem=5120.00] same[type:mo
                     del]
 Effective: select[(type == any )] order[r15s:pg] rusage[mem=5120.00] same[type
                     :model] 
"""

    expected = ['28320', 'user_example', 'RUN', 'q_residual', 5120.0, 36.0, 6, 243, 4, 'Wed Sep  3 22:58:23', 'Wed Sep  3 23:22:29', 'mn61']

    assert parse_bjobs_details(bjobs) == expected