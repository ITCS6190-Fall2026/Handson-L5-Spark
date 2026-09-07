# Hands-on L5: Word Count with Spark

**ITCS 6190/8190 — Cloud Computing for Data Analysis — Fall 2026**

In Hands-on L4 you ran a word count as a Hadoop MapReduce job: three Java classes, a Maven
build, a JAR copied into the cluster, the input loaded into HDFS, `hadoop jar`, and the output
pulled back out. In this hands-on you run the **same word count on Apache Spark**, twice: first
interactively in the PySpark shell, then as an application submitted with `spark-submit`. Along
the way you will watch the job in the Spark UI.

Everything you need is in this repository. **You are not writing any code.** The point is to
go through the same cycle as in L4 and notice which steps are still there and which ones are
gone.

**Worth 1 point.** Submit the repository link on Canvas by 11:59 pm on the day of the class.
Work individually.

---

## The job you are running

The program counts how many times each word appears in a text file, ignoring words shorter
than three characters, and prints the results from most frequent to least. These are the
same rules as in L4, so if you reuse your L4 input you should get the same counts.

### Example input

```
Hello world
Hello Hadoop
Hadoop is powerful
Hadoop is used for big data
```

### Expected output

```
Hadoop 3
Hello 2
big 1
data 1
for 1
powerful 1
used 1
world 1
```

`is` is missing because it is only two characters. Counting is case-sensitive. Words with the
same count are listed alphabetically.

---

## What is in this repository

| Path | What it is |
| ---- | ---------- |
| `docker-compose.yml` | the cluster: one Spark master and two workers, on the official `apache/spark:4.2.0` image |
| `wordcount.py` | the word count as a PySpark application (about 20 lines; read it) |
| `shared-folder/input/data/input.txt` | **placeholder: you replace this with your own text** |
| `shared-folder/output/` | where the result lands in step 6 |

`shared-folder/` is mounted into every container at `/opt/spark/work-dir/shared`, so a file
you put there on your machine is visible to the master and to both workers. There is no HDFS
in this cluster; the shared folder plays its role.

---

## Prerequisites

- **Docker Desktop**, running. See <https://docs.docker.com/get-started/get-docker/>.

That is all. Spark, Java and Python are inside the image. You do not need Java or Maven on
your machine for this hands-on.

```bash
docker --version
```

---

## Steps

### 1. Put your own text in the input file

Open `shared-folder/input/data/input.txt` and replace the placeholder line with text of your
own. Reusing your L4 input is a good idea: you can compare the two outputs directly.

### 2. Start the Spark cluster

```bash
docker compose up -d
```

The first time, Docker downloads the image (about 1 GB). Give the cluster a few seconds, then
open <http://localhost:8080>. You should see the Spark master with **two workers** in state
ALIVE, each with 2 cores and 2 GB of memory. Compare this page with the NameNode and
ResourceManager pages from L4: one page, one cluster manager.

### 3. Open the PySpark shell against the cluster

```bash
docker exec -it spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077
```

Wait for the banner with `version 4.2.0` and the `>>>` prompt. The shell created a
`SparkSession` for you, available as `spark`. Refresh <http://localhost:8080>: your shell now
appears as a running application, and it has been given executors on both workers.

### 4. Run the word count interactively

Type these lines at the prompt (or paste them one at a time):

```python
from pyspark.sql.functions import explode, split, length, col
lines = spark.read.text("/opt/spark/work-dir/shared/input/data/input.txt")
words = lines.select(explode(split(col("value"), r"\s+")).alias("word"))
counts = words.filter(length("word") >= 3).groupBy("word").count()
counts.orderBy(col("count").desc(), col("word")).show()
```

Nothing happens until the last line: `read`, `select`, `filter` and `groupBy` only build the
plan, `show` runs it. That is the transformations-and-actions model from the slides.

While the shell is open, look at the application UI at <http://localhost:4040>. The **Jobs**
tab shows the job `show` triggered; open it and look at the stages. The **Executors** tab
shows the two workers that ran the tasks. The **SQL / DataFrame** tab shows the query and its
plan.

Try one change before you leave the shell, for example `counts.count()` or
`counts.orderBy("word").show()`. Notice that you did not rebuild or resubmit anything.

Leave the shell with `exit()` or Ctrl-D.

### 5. Look at the application

Open `wordcount.py`. It is the same four lines you just typed, plus the imports, a
`SparkSession` built explicitly (there is no shell to create it), and a few lines at the end
that write the result to a directory. Compare it with the three Java classes from L4.

### 6. Submit the application

`wordcount.py` lives in the root of the repository, so the container cannot see it. Copy it
in, exactly as you copied the JAR in L4, then submit:

```bash
docker cp wordcount.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/wordcount.py \
  /opt/spark/work-dir/shared/input/data/input.txt \
  /opt/spark/work-dir/shared/output/wordcount
```

This is the Spark equivalent of `hadoop jar`. The counts are printed in your terminal, and
the result is written to the output directory.

The output directory must **not** already exist. To rerun, delete
`shared-folder/output/wordcount` on your machine first, or write to a new name.

### 7. Look at the result

On your machine, `shared-folder/output/wordcount/` now contains a `part-00000-...txt` file
with the counts, and an empty `_SUCCESS` marker. Same shape as the `part-r-00000` you copied
out of HDFS in L4, but it is already on your disk: the workers wrote it straight into the
shared folder.

### 8. Stop the cluster

```bash
docker compose down
```

---

## What to commit

- Your **input dataset** at `shared-folder/input/data/input.txt`
- The **output** under `shared-folder/output/wordcount/` (the `part-...txt` file; `_SUCCESS`
  is ignored by `.gitignore`)
- Your **report** in `REPORT.md`

Leave this README, `docker-compose.yml` and `wordcount.py` as they are.

---

## Report

Fill in **`REPORT.md`** in the root of this repository. Keep it short.

### What I ran
The commands you used, in the order you used them. If you deviated from the steps above,
say where and why.

### Input and output
Your input dataset, and the output the application produced. Paste the output rather than
linking to the file.

### What I observed
A few sentences on what you actually noticed: what the master page showed when the shell
connected, how many tasks and executors the Spark UI listed for `show`, how long the job
took, whether the output matched your L4 result.

### L4 versus L5
Write the L4 steps and the L5 steps side by side (a short table is fine) and answer: which
steps disappeared, and what in Spark's design made them unnecessary? Which parts of the work
are still the same, even if you did not see them?

### Problems and fixes
Anything that went wrong and what resolved it. The actual error message is worth more than
"it did not work". If nothing went wrong, say so.

---

## Submission

### 1. Make your own copy of this repository

On the repository page, click the green **Use this template** button, then
**Create a new repository**. Name it `ITCS6190-H5-<your-name>` and set the visibility to
**Public**.

Do not fork, and do not clone this repository directly. A fork or a clone still points at
the course repository, so your work would not end up anywhere we can grade it.

Then clone *your* new repository to your machine and work there.

### 2. Commit your work

Your input dataset, your output, and `REPORT.md`.

### 3. Submit the link

Post the URL of **your** repository on Canvas. Keep it public until grades are posted.
There is no need to add the instructor or the TAs as collaborators.

---

## Optional: change the analysis

If you finish early, edit a copy of `wordcount.py` and resubmit it. Ideas: show only the ten
most frequent words (`.limit(10)`), make the count case-insensitive (`lower(col("value"))`),
or count words per line length. Notice what changing the analysis costs you here compared
with L4.
