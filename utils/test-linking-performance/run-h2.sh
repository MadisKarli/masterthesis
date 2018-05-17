# 32 and 16 cores as they need the most ram
spark-submit --master local[32] --num-executors 32 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples100g thesis/tmp/triples100g-32-h2-run2
sleep 1m
spark-submit --master local[16] --num-executors 16 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples100g thesis/tmp/triples100g-16-h2-run2
sleep 1m

spark-submit --master local[32] --num-executors 32 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples50g thesis/tmp/triples50g-32-h2-run2
sleep 1m
spark-submit --master local[16] --num-executors 16 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples50g thesis/tmp/triples50g-16-h2-run2
sleep 1m

spark-submit --master local[32] --num-executors 32 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples25g thesis/tmp/triples25g-32-h2-run2
sleep 1m
spark-submit --master local[16] --num-executors 16 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples25g thesis/tmp/triples25g-16-h2-run2
sleep 1m


# 1 and 2 cores as they are the slowest
spark-submit --master local[1] --num-executors 2 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples100g thesis/tmp/triples100g-2-h2-run2
sleep 1m
spark-submit --master local[2] --num-executors 1 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples100g thesis/tmp/triples100g-1-h2-run2
sleep 1m

spark-submit --master local[2] --num-executors 2 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples50g thesis/tmp/triples50g-2-h2-run2
sleep 1m
spark-submit --master local[1] --num-executors 1 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples50g thesis/tmp/triples50g-1-h2-run2
sleep 1m

spark-submit --master local[2] --num-executors 2 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples25g thesis/tmp/triples25g-2-h2-run2
sleep 1m
spark-submit --master local[1] --num-executors 1 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples25g thesis/tmp/triples25g-1-h2-run2
sleep 1m

# 4 and 8 are left
spark-submit --master local[4] --num-executors 4 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples100g thesis/tmp/triples100g-4-h2-run2
sleep 1m
spark-submit --master local[8] --num-executors 8 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples100g thesis/tmp/triples100g-8-h2-run2
sleep 1m

spark-submit --master local[4] --num-executors 4 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples50g thesis/tmp/triples50g-4-h2-run2
sleep 1m
spark-submit --master local[8] --num-executors 8 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples50g thesis/tmp/triples50g-8-h2-run2
sleep 1m

spark-submit --master local[4] --num-executors 4 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples25g thesis/tmp/triples25g-4-h2-run2
sleep 1m
spark-submit --master local[8] --num-executors 8 link-triples-to-orgs.py company-urls.csv thesis/testdata/triples25g thesis/tmp/triples25g-8-h2-run2
sleep 1m