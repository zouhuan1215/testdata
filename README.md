## Performance of index advisor

We ran a benchmark on *tpch* and *tpcds* testset. Let index advisor recommend composite indexes composed of no more than 3 columns. Then test two cases where the allowed maximum number of recommended indexes are about 10% and 20% of the number of all columns in a database. And the result shows that in 10% and 20% cases, *index advisor* can save about 10% and 30% execution time respectively both on *tpch* and *tpcds* testset. One interesting thing is that instead of improving query efficiency of a large number of queries equally, *index advisor* greatly improves the execution efficiency of specific queries, which have a dominant contribution to the final result. That is, there maybe a type of queries on which indexes make a great difference. From our results, we observe that the output of these queries is all of small scale.

<div align="center">
<img src="docs/src/imgs/idxadv1.png"  height="300" width="600">
</div>


<div align="center">
<img src="docs/src/imgs/idxadv2.png"  height="300" width="600">
</div>