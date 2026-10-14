# Documentation

Suggested reading order:

| # | Document | What you'll find |
|---|---|---|
| 01 | [Requirements & capacity planning](01-requirements-and-capacity.md) | Business scope, NFRs, the 1 M-user traffic model, sizing math for every tier |
| 02 | [Architecture](02-architecture.md) | Component diagram, sequence diagrams for queue / reserve / pay / events, design principles, code map |
| 03 | [Data model & database scaling](03-data-model-and-database-scaling.md) | Schema, DB-enforced invariants, inventory sharding, pooling, read scaling, partitioning/sharding roadmap, backups |
| 04 | [Race conditions](04-race-conditions.md) | 19 concrete races, how each is prevented, and the test that proves it |
| 05 | [Resilience & failover](05-resilience-and-failover.md) | Load-shedding funnel, failure-mode matrix, breakers/timeouts/retries/bulkheads, chaos drills, multi-region |
| 06 | [Operations runbook](06-operations-runbook.md) | Sale-day timeline, golden signals & alerts, load testing, SQL invariant checks |

Cloud reference deployments (diagram + service mapping + sizing + failover per provider):

* [AWS](cloud/aws.md): CloudFront/WAF · ALB · EKS · Aurora PostgreSQL + RDS Proxy · ElastiCache · MSK
* [Google Cloud](cloud/gcp.md): Cloud CDN/Armor · Global LB · GKE · Cloud SQL · Memorystore Cluster · Managed Kafka
* [Azure](cloud/azure.md): Front Door · AKS · PostgreSQL Flexible Server · Azure Managed Redis · Event Hubs (+ cross-cloud equivalence table)
