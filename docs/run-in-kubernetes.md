# Run in Kubernetes

This document describes how to run the Kafka Schema Registry Migrator in a Kubernetes cluster as a one-time job.

## Prerequisites

- Kubernetes cluster with kubectl configured
- Access to the Docker image: `aywengo/kafka-schema-reg-migrator:latest`

## Create Secrets

First, create Kubernetes secrets for the authentication credentials:

```yaml
# secrets.yaml
apiVersion: v1
kind: Secret
metadata:
  name: schema-registry-auth
type: Opaque
stringData:
  source-username: "source_user"
  source-password: "source_pass"
  dest-username: "dest_user"
  dest-password: "dest_pass"
```

Apply the secrets:
```bash
kubectl apply -f secrets.yaml
```

## Create Job

Create a Kubernetes job to run the migrator:

```yaml
# migration-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: schema-registry-migration
spec:
  template:
    spec:
      containers:
      - name: migrator
        image: aywengo/kafka-schema-reg-migrator:latest
        env:
        - name: SOURCE_SCHEMA_REGISTRY_URL
          value: "http://source-schema-registry:8081"
        - name: DEST_SCHEMA_REGISTRY_URL
          value: "http://dest-schema-registry:8081"
        - name: ENABLE_MIGRATION
          value: "true"
        - name: DRY_RUN
          value: "true"
        - name: SOURCE_USERNAME
          valueFrom:
            secretKeyRef:
              name: schema-registry-auth
              key: source-username
        - name: SOURCE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: schema-registry-auth
              key: source-password
        - name: DEST_USERNAME
          valueFrom:
            secretKeyRef:
              name: schema-registry-auth
              key: dest-username
        - name: DEST_PASSWORD
          valueFrom:
            secretKeyRef:
              name: schema-registry-auth
              key: dest-password
        - name: LOG_LEVEL
          value: "INFO"
        - name: PRESERVE_IDS
          value: "false"
        - name: RETRY_FAILED
          value: "true"
      restartPolicy: Never
  backoffLimit: 0
```

Apply the job:
```bash
kubectl apply -f migration-job.yaml
```

## Monitor Job Status

Check the job status:
```bash
kubectl get jobs schema-registry-migration
```

View the job logs:
```bash
kubectl logs job/schema-registry-migration
```

## Cleanup

After the migration is complete, you can delete the job and secrets:

```bash
kubectl delete job schema-registry-migration
kubectl delete secret schema-registry-auth
```

## Troubleshooting

1. Check job status:
```bash
kubectl describe job schema-registry-migration
```

2. View pod logs:
```bash
kubectl logs -l job-name=schema-registry-migration
```

3. Check pod events:
```bash
kubectl describe pod -l job-name=schema-registry-migration
```

4. Verify secret access:
```bash
kubectl get secret schema-registry-auth -o yaml
```

## Example with Context Support

If you need to use contexts, add the context environment variables:

```yaml
# migration-job-with-context.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: schema-registry-migration
spec:
  template:
    spec:
      containers:
      - name: migrator
        image: aywengo/kafka-schema-reg-migrator:latest
        env:
        # ... existing environment variables ...
        - name: SOURCE_CONTEXT
          value: "source-context"
        - name: DEST_CONTEXT
          value: "dest-context"
        - name: PRESERVE_IDS
          value: "false"
        - name: RETRY_FAILED
          value: "true"
      restartPolicy: Never
  backoffLimit: 0
``` 