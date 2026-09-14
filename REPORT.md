# Hands-on L5: Report

**Name:**
**Student ID:**
**Email:**

---

## What I ran

The commands you used, in the order you used them. If you deviated from the steps in the
README, say where and why.

```bash

```

---

## Input and output

### My input dataset

```

```

### The output of part 1

Paste the contents of the `part-...txt` file from `shared-folder/output/wordcount/`.

```

```

---

## What I observed

A few sentences on what you actually noticed. Some things worth looking at:

- What the master page at <http://localhost:8080> showed when the shell connected
- How many tasks and executors the Spark UI at <http://localhost:4040> listed for `show`
- How long the job took, in the shell and with `spark-submit`



---

## What I changed

The three changes you made to `wordcount.py`. Paste the lines you added or rewrote
(`git diff` gives you exactly this).

```python

```

---

## What the changes did

### The three numbers

| Run | Min length | Words scanned | Words kept | Distinct words |
| --- | ---------- | ------------- | ---------- | -------------- |
| `wordcount-v2` | 3 | | | |
| `wordcount-long` | | | | |

### The three outputs compared

How many distinct words did folding the case remove (compare `wordcount/` with
`wordcount-v2/`)? How many did the longer minimum remove? Name one word from your own text
whose count changed when the counting became case-insensitive.



### Jobs

How many jobs did your run launch, according to the **Jobs** tab, and how does that compare
with the original program? Why does Spark read the same file more than once in a single run?



---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.


