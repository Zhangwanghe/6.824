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
