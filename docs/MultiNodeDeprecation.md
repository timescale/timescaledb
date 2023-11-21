## Multi-node Deprecation

Multi-node support has been deprecated.
TimescaleDB 2.13 is the last version that will include multi-node support. Multi-node support in 2.13 is available for PostgreSQL 13, 14 and 15.
This decision was not made lightly, and we want to provide a clear understanding of the reasoning behind this change and the path forward.

### Why we are ending multi-node support

We began to work on multi-node support in 2018 and released the first version in 2020 to address the growing
demand for higher scalability in TimescaleDB deployments.
The distributed architecture of multi-node allowed for horizontal scalability of writes and reads to go beyond what a single node could provide. 

While we added many improvements since the initial launch, the architecture of multi-node came with a number of
inherent limitations and challenges that have limited its adoption.
Regrettably, only about 1% of current TimescaleDB deployments utilize multi-node, and the complexity involved in
maintaining this feature has become a significant obstacle.
It’s not an isolated feature that can be kept in the product with very little effort since adding new features required
extra development and testing to ensure they also worked for multi-node.

As we've evolved our single-node product and expanded our cloud offering to serve thousands of customers,
we've identified more efficient solutions to meet the scalability needs of our users.

First, we’ve been able and will continue to make big improvements in the write and read performance of single-node. 
We’ve scaled a single-node deployment to process 2 million inserts per second and have seen performance improvements of 10x for common queries.
You can read a summary of the latest query performance improvements [here](https://www.timescale.com/blog/8-performance-improvements-in-recent-timescaledb-releases-for-faster-query-analytics/).

And second, we are leveraging cloud technologies that have become very mature to provide higher scalability in a more accessible way.
For example, our cloud offering uses object storage to deliver virtually [infinite storage capacity at a very low cost](https://www.timescale.com/blog/scaling-postgresql-for-cheap-introducing-tiered-storage-in-timescale/).

For those reasons, we’ve decided to focus our efforts on improving single-node and leveraging cloud technologies to solve for high scalability and as a result we’ve ended support for multi-node.

### What this means for you

We understand that this change may raise questions, and we are committed to supporting you through the transition.

For current TimescaleDB multi-node users, please refer to our [migration documentation](https://docs.timescale.com/migrate/latest/multi-node-to-timescale-service/)
for a step-by-step guide to transition to a single-node configuration.

Alternatively, you can continue to use multi-node up to version 2.13. However, please be aware that future versions will no longer include this functionality.

If you have any questions or feedback, you can share them in the #multi-node channel in our [community Slack](https://slack.timescale.com/).

