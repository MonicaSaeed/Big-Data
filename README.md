# Big-Data

This README file provides instructions on running MapReduce jobs using Hadoop.

## Prerequisites

- Hadoop installed and configured on your system.

## Usage Instructions

### Question 1.3

```
 hadoop fs -copyFromLocal Desktop/Q1.3_dataset.txt
```

```
 hadoop jar Desktop/q1.3.jar question_1.DifferenceDriver Q1.3_dataset.txt q1out
```

```
 hadoop fs -copyToLocal q1out ~/Desktop/out
```

### Question 2

```
 hadoop fs -copyFromLocal Desktop/Q2_dataset.txt
```

```
 hadoop jar Desktop/q2.jar question_2.FriendsDriver Q2_dataset.txt q2out
```

```
 hadoop fs -copyToLocal q2out ~/Desktop/out
```

### Question 3

```
 hadoop fs -copyFromLocal Desktop/Q3_dataset.txt
```

```
hadoop jar Desktop/q3.jar question_3.NameAgeDriver Q3_dataset.txt out
```

```
hadoop fs -copyToLocal out ~/Desktop/out
```
