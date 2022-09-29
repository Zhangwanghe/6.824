# 6.824

## 1. Lab1
Code tagged Lab1 has passed test script src/main/test-mr-many.sh.

## 2. Lab2
Code tagged Lab2D has passed test script src/raft/test-many-verbose.sh.

Here are some tips for This lab:

- Always follow figure2 and when you find some bugs, you should go back to it and check whether all you implementation conforms to it.
- Test as many times as possible in earlier steps which will expose bugs earlier and thus they can be easily traced and fixed.
- Put Log as a separate structure and this is really useful when we turn to Lab2D.

## 3. Lab3
Code tagged Lab3D has passed test script src/kvraft/test-many-verbose.sh.

Here are some tips for This lab:

- Follow instructions from chapter 8 client interaction.
- Since kvserver and raft use different locks, there may be some actions require both locks and require atomicity. 

## 4. Lab4
Code tagged Lab4A has passed test script src/shardctrler/test-many.sh.
Code tagged Lab4B has passed test script src/shardkv/test-many-verbose.sh.

Here are some tips for This lab:

- If a state should be stored in all servers (some states may be stored only in leaders), always apply changes to it after commiting them. This means we should always call kv.rf.Start and wait for the given index to be commited.
- When moving shards, I chose to let servers request from for a given shard the original owner instead of sending to the new owner. This is much easier to determine whether the shard has been successfully moved. We can simply check the owner's config number for this shard and this request can be finished even by a follower and with partition.
- Do not check any reconfiguration before receiving all commiting logs after restarting, or you may end up with repeated actions every time a server starts up.