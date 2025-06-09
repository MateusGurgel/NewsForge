# NewsForge

Minio setup:

Prerequisites:
- Mc client

Minio:

# Create a bucket: "news"
mc mb local/news

### define the server
mc alias set local http://localhost:9000 admin adminadmin

## define politics
mc admin policy create local batch_member ./compose/minio/batch-policy.json
mc admin policy create local speed_member ./compose/minio/speed-policy.json
mc admin policy create local ingestion_member ./compose/minio/ingestion-policy.json


### add batch user
mc admin user add local batch batchbatch
mc admin policy attach local batch_member --user batch


## add ingestion user
mc admin user add local ingestion ingestioningestion
mc admin policy attach local ingestion_member --user ingestion

## add speed user
mc admin user add local speed speedspeed
mc admin policy attach local speed_member --user speed

---

## Coisas que eu faria diferente:

- Kubernetes ao invés de docker compose
- Processamento via speed, não batch
