# Hands-on L5: Word Count with Spark

**ITCS 6190/8190 — Cloud Computing for Data Analysis — Fall 2026**

In Hands-on L4 you ran a word count as a Hadoop MapReduce job: three Java classes, a Maven
build, a JAR copied into the cluster, the input loaded into HDFS, `hadoop jar`, and the output
pulled back out. In this hands-on you run the **same word count on Apache Spark**, twice: first
interactively in the PySpark shell, then as an application submitted with `spark-submit`. Along
the way you will watch the job in the Spark UI.

The hands-on has two parts. In **part 1** you run the program exactly as it is given, to go
through the whole cycle and see it in the Spark UI. In **part 2** you change the program: you
make the counting case-insensitive, turn the minimum word length into a parameter, and add
three numbers to the output. Then you run it again, twice, without rebuilding anything.

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
| `docker-compose.codespaces.yml`, `conf/spark-defaults.conf` | the same cluster, for GitHub Codespaces (see step 2) |
| `wordcount.py` | the word count as a PySpark application (about 20 lines; read it, then change it in part 2) |
| `shared-folder/input/data/input.txt` | **placeholder: you replace this with your own text** |
| `shared-folder/output/` | where the results land (three runs, three folders) |

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

## Part 1: run the word count as it is given

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

**In a GitHub Codespace**, start the cluster with the other compose file instead:

```bash
docker compose -f docker-compose.codespaces.yml up -d
```

Inside a Codespace, Docker itself runs in a container and traffic between containers on a
Compose network can be dropped. The workers then never register with the master and every job
waits forever with `Initial job has not accepted any resources`. That file puts the whole
cluster on one network to avoid it. Everything after this step is the same, except that the
`docker compose down` in step 11 also needs the `-f docker-compose.codespaces.yml` flag.

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

---

## Part 2: change the code

The program you just ran is case-sensitive and has the number 3 written into it. Now you make
it yours.

### 8. Make three changes to `wordcount.py`

Edit `wordcount.py` on your machine. All three changes are small; the point is that you have
to read the code before you can change it.

**Change 1: count words case-insensitively.** `Hadoop`, `hadoop` and `HADOOP` must count as
one word, reported in lowercase. One function from `pyspark.sql.functions` does this; where
you apply it is up to you.

**Change 2: make the minimum word length a parameter.** Instead of the 3 written into the
filter, read it from an optional third command line argument:

```
spark-submit wordcount.py <input file> [<output directory>] [<min word length>]
```

When the argument is not given, the minimum stays 3, so the command from step 6 keeps working.

**Change 3: print three numbers before the table.**

```
<n> words scanned
<n> words of at least <min> characters
<n> distinct words
```

The first is how many words the split produced, the second how many survived the length
filter, the third how many rows are in the result. A blank line in your input produces one
empty word, which the filter then drops, so the first two numbers can differ by more than the
short words you removed.

Keep everything else as it is: the same sort order (count descending, then the word), the same
one-file output, the same `spark-submit` interface.

### 9. Run your version twice

Copy the file in again after **every** edit, or the cluster keeps running the old one:

```bash
docker cp wordcount.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/wordcount.py \
  /opt/spark/work-dir/shared/input/data/input.txt \
  /opt/spark/work-dir/shared/output/wordcount-v2
```

Then run it once more with a minimum word length of **5 or more**, into a third folder:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/wordcount.py \
  /opt/spark/work-dir/shared/input/data/input.txt \
  /opt/spark/work-dir/shared/output/wordcount-long 5
```

Nothing was rebuilt and nothing was redeployed between those two runs: the second one is the
same application with a different argument.

### 10. Compare the three runs

You now have three results from the same input: `wordcount/` from part 1, `wordcount-v2/` with
the same minimum but case-insensitive, and `wordcount-long/` with a longer minimum. How many
distinct words did folding the case remove? How many did the longer minimum remove?

Watch the **Jobs** tab of the application UI at <http://localhost:4040> while one of these
runs is in progress: it is only there while the application runs, so open it right after you
press Enter. If you miss it, paste your new lines into the PySpark shell from step 3 instead,
where the UI stays up as long as the shell is open (close the shell before submitting again,
or it keeps the cores for itself).

Your version launches more jobs than the original did: two of the three numbers you print are
new actions, and Spark reads and splits the file again for each of them. A DataFrame is a
plan, not a stored result, and nothing is kept unless you ask for it. You will also see more
jobs than you have actions, because Spark runs each shuffle stage of a query as a job of its
own.

### 11. Stop the cluster

```bash
docker compose down
```

---

## What to commit

- Your **input dataset** at `shared-folder/input/data/input.txt`
- Your **modified `wordcount.py`**
- The **three outputs**, under `shared-folder/output/wordcount/`, `wordcount-v2/` and
  `wordcount-long/` (the `part-...txt` files; `_SUCCESS` and the `.crc` files are ignored by
  `.gitignore`)
- Your **report** in `REPORT.md`

Leave this README and `docker-compose.yml` as they are.

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
took.

### What I changed
The three changes you made to `wordcount.py`, with the lines you added or rewrote. `git diff`
before you commit gives you exactly this.

### What the changes did
The three numbers printed by each of your two runs, and the answers to step 10: how many
distinct words folding the case removed, and how many the longer minimum removed. Then: how
many jobs did each run launch, and why does the same file get read more than once?

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

## Optional: stop reading the file three times

If you finish early, fix the waste you saw in step 10. Two ways, both worth trying:

- Cache the filtered words (`kept.cache()` or `.persist()`) before the first count. Run again
  and look at the **Storage** tab of the Spark UI and at the input size of the later jobs.
- Or compute the numbers in one pass with a single aggregation, for example
  `kept.agg(count("*"), countDistinct("word"))`, and see how many jobs are left.

Say in the report which one you tried and what changed in the UI.
