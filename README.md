# GDX-Analytics-Snowplow-Gateway-Service
A gateway service to the Snowplow analytics service for BCGov OpenShift projects


## Setup
```

oc -n 8gsiqa-tools policy add-role-to-group system:image-puller 'system:serviceaccounts:8gsiqa-dev' --rolebinding-name=cross-project-pull

```
## Build
```

cd .pipeline
# Building
npm run build -- --pr=0 --dev-mode=true

```
## Deploy
```

# Deploy to DEV
cd .pipeline
npm run deploy -- --pr=0 --env=dev

```
## Clean

```
cd .pipeline

# Build environment
npm run clean -- --pr=0 --env=build

# DEV environment
npm run clean -- --pr=0 --env=dev

# TEST
npm run clean -- --pr=0 --env=test

# PROD
npm run clean -- --pr=0 --env=prod

```