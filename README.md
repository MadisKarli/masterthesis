# Repository containing master thesis.

## link-triples-to-orgs.py

Connects a triples to companies, companies are read from a csv file. Output is a spark text file. Run the program with no arguments to see how it works. Parameters in cluster company-urls.csv hdfs://ir-hadoop1/user/madis/microdata/2017/triples-microdata thesis/output/company-triples


## bipartite-graphs.py

Creates bipartite graphs from output of link-triples-to-orgs.py. Outputs two dataframes. First one contains company-product pairs where product is schema.org/Product/sameAs value or SKU value. Other dataframe is the same as above but filtered such that only those that have sameAS or SKU value are kept. Parameters in cluster: hdfs://ir-hadoop1/user/madis/microdata/2017/triples-microdata hdfs://ir-hadoop1/user/madis/thesis/outputs/company-triples thesis/output/bipartite-all thesis/output/bipartite-all-sku-only


## calculate-network-metrics.py

Creates company-product graphs, the graphs are bidirectional (edge from product to company and edge from company to product). At the moment calculates only degree and pagerank. Outputs a parquet dataframe file. Parameters in cluster: spark-submit --master local[10] --packages graphframes:graphframes:0.5.0-spark1.6-s_2.10 calculate-network-metrics.py thesis/outputs/bipartite-all-sku-only thesis/ouputs/network-metrics


## network-metrics-to-derivative-input.py

Reads in network metrics parquet file, filters it to only contain infromation about comapnies. Network metrics for products is dropped. Outputs to local files system. 
Parameters in cluster: spark-submit --master local[10]  network-metrics-to-derivate-input.py thesis/ouputs/network-metrics metrics


## collect-derivative-inputs.py

Works only on local system, not in HDFS. Can be run on hadoop machines. Takes derivatives from multiple runs and combines them into one Pandas dataframe. Each time series (degree, pagerank) is outputted to its own file. 
Parameters in cluster: python collect-derivative-inputs.py


Final step is running the derivative algorithm on each output of the last step.
