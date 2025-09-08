from lsf2csv import get_job_ids, parse_bjobs_details
import pytest

def test_get_job_ids():
    bjobs_output = """
    JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
    28250   user_example   RUN   queue mn325       g1_r        /bin/bash  Sep  3 22:17
    17381   user_example   EXIT  queue mn325          -        /bin/bash  Aug 28 13:36
    17382   user_example   DONE  q_htc      mn325       16*g1       /bin/bash  Aug 28 13:37
    17392   user_example   EXIT  q_htc      mn325          -        *em=65536] Aug 28 14:15
    17395   user_example   EXIT  q_htc      mn325       16*g1       *em=60000] Aug 28 14:24
    17445   user_example   DONE  q_htc      mn325       2*g1        *ifgram_89 Aug 28 16:51
    21182   user_example   DONE  queue mn325       16*g1       /bin/bash  Sep  1 12:32
    21573   user_example   EXIT  queue mn325          -        /bin/bash  Sep  1 13:40
    21574   user_example   EXIT  queue mn325       16*g1       /bin/bash  Sep  1 13:41
    21618   user_example   EXIT  queue mn325          -        /bin/bash  Sep  1 13:57
    21619   user_example   EXIT  queue mn325       16*g1       /bin/bash  Sep  1 13:57
    21630   user_example   DONE  queue mn325       16*g1       /bin/bash  Sep  1 14:15
    21633   user_example   DONE  queue mn325       16*g1       /bin/bash  Sep  1 14:18
    22394   user_example   DONE  queue mn325       16*g1_r     /bin/bash  Sep  1 20:55
    22397   user_example   DONE  queue mn325       16*g1_r     /bin/bash  Sep  1 20:59
    22399   user_example   EXIT  queue mn325       16*g1_r     /bin/bash  Sep  1 21:04
    22402   user_example   EXIT  queue mn325       16*g1_r     /bin/bash  Sep  1 21:23
    22404   user_example   EXIT  queue mn325       12*g1_r     /bin/bash  Sep  1 21:40
                                                 4*g1
    28240   user_example   EXIT  queue mn325       g1_r        */jobs.csv Sep  3 21:47
    28241   user_example   EXIT  queue mn325       g1_r        /bin/bash  Sep  3 21:48
    """
    assert get_job_ids(bjobs_output) == ['28250', '17381', '17382', '17392', '17395', '17445', '21182', '21573', '21574', '21618', '21619', '21630', '21633', '22394', '22397', '22399', '22402', '22404', '28240', '28241']


def test_get_used_resources():
    bjobs = """
    Job <35179>, Job Name <mixcoacmiapplpydelaunay_SenDT41_202301_202506_01>, User 
                     <user_example>, Project <default>, Status <DONE>, Queue <q_htc>, 
                     Command <#!/bin/bash; #BSUB -J mixcoacmiapplpydelaunay_Sen
                     DT41_202301_202506_01;#BSUB -q q_htc;#BSUB -n 1;#BSUB -o /
                     tmpu/desr_g/user_example/insarlab/user_example/scratch/logs/mixcoacmiapp
                     lpydelaunay_SenDT41_202301_202506_01/20250825-213325_%J.o;
                     #BSUB -e /tmpu/desr_g/user_example/insarlab/user_example/scratch/logs/mi
                     xcoacmiapplpydelaunay_SenDT41_202301_202506_01/20250825-21
                     3325_%J.e; export OMP_NUM_THREADS=1; minsarApp.bash /tmpu/
                     desr_g/user_example/code/miztli-insar/samples/finsar/mixcoac/mixc
                     oacmiapplpydelaunay_SenDT41_202301_202506_01.template --no
                     -tmp --start download --stop ifgram --burst-download --mia
                     plpy --no-mintpy>
Sun Sep  7 15:52:03: Submitted from host <mn325>, CWD </tmpu/desr_g/user_example/insar
                     lab/user_example/scratch>, Output File </tmpu/desr_g/user_example/insarl
                     ab/user_example/scratch/logs/mixcoacmiapplpydelaunay_SenDT41_2023
                     01_202506_01/20250825-213325_35179.o>, Error File </tmpu/d
                     esr_g/user_example/insarlab/user_example/scratch/logs/mixcoacmiapplpydel
                     aunay_SenDT41_202301_202506_01/20250825-213325_35179.e>;
Sun Sep  7 15:52:08: Started 1 Task(s) on Host(s) <mn391>, Allocated 1 Slot(s) 
                     on Host(s) <mn391>, Execution Home </home/desr_g/user_example>, E
                     xecution CWD </tmpu/desr_g/user_example/insarlab/user_example/scratch>;
Sun Sep  7 18:14:12: Done successfully. The CPU time used is 580.2 seconds.

 MEMORY USAGE:
 MAX MEM: 2323 Mbytes;  AVG MEM: 82 Mbytes

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
 Combined: select[type == any ] order[r15s:pg] same[type:model]
 Effective: select[type == any ] order[r15s:pg] same[type:model] 
"""

    expected = ['35179', 'user_example', 'DONE', 'q_htc', '580.2', '2323', '82', 'mn391', 'Sun Sep 7 15:52:08', 'Sun Sep 7 18:14:12']
    result = parse_bjobs_details(bjobs) 
    assert result == expected