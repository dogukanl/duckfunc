# Duckfunc
duckfunc let's you run one-of duckdb queries using serverless functions.

# Basic
```bash
pip install -r requirements.txt
functions-framework \
    --host localhost \
    --port 8080 \
    --target quack

PYTHONPATH=. py examples/basic.py

# ┌────────────┬───────────┬───────────────────────┬──────────────┐
# │ LocationID │  Borough  │         Zone          │ service_zone │
# │   int64    │  varchar  │        varchar        │   varchar    │
# ├────────────┼───────────┼───────────────────────┼──────────────┤
# │        238 │ Manhattan │ Upper West Side North │ Yellow Zone  │
# │        239 │ Manhattan │ Upper West Side South │ Yellow Zone  │
# └────────────┴───────────┴───────────────────────┴──────────────┘
```

# GCP

## Setup

```bash
export PROJECT="duckfunc-project"
export FUNCTION_NAME="duckfunc-function"
export SERVICE_ACCOUNT="duckfunc-sa"
export SERVICE_ACCOUNT_EMAIL="$SERVICE_ACCOUNT@$PROJECT.iam.gserviceaccount.com"
export BILLING_ACCOUNT_ID="duckfunc-billing-account-id"
export REGION="${REGION:-europe-west1}"


gcloud projects create $PROJECT
gcloud config set project $PROJECT

gcloud billing projects link $PROJECT --billing-account $BILLING_ACCOUNT_ID

gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable cloudfunctions.googleapis.com

gcloud iam service-accounts create $SERVICE_ACCOUNT

roles=(
    "roles/artifactregistry.createOnPushWriter"
    "roles/cloudbuild.builds.builder"
    "roles/logging.logWriter"
    "roles/storage.objectAdmin"
    "roles/run.invoker"
)

for role in ${roles[@]}
do
    gcloud projects add-iam-policy-binding $PROJECT  \
        --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
        --role="$role"
done

gcloud functions deploy $FUNCTION_NAME \
    --trigger-http \
    --no-allow-unauthenticated \
    --entry-point="quack" \
    --runtime="python312" \
    --timeout="3600s" \
    --gen2 \
    --region="$REGION" \
    --service-account="$SERVICE_ACCOUNT_EMAIL"

gcloud iam service-accounts keys create "/tmp/$SERVICE_ACCOUNT.json" \
    --iam-account "$SERVICE_ACCOUNT_EMAIL"

export GOOGLE_APPLICATION_CREDENTIALS="/tmp/$SERVICE_ACCOUNT.json"
py examples/gcp.py

# ┌────────────┬───────────┬───────────────────────┬──────────────┐
# │ LocationID │  Borough  │         Zone          │ service_zone │
# │   int64    │  varchar  │        varchar        │   varchar    │
# ├────────────┼───────────┼───────────────────────┼──────────────┤
# │        238 │ Manhattan │ Upper West Side North │ Yellow Zone  │
# │        239 │ Manhattan │ Upper West Side South │ Yellow Zone  │
# └────────────┴───────────┴───────────────────────┴──────────────┘
```

## Teardown

```bash
rm $GOOGLE_APPLICATION_CREDENTIALS

gcloud functions delete $FUNCTION_NAME

gcloud iam service-accounts delete $SERVICE_ACCOUNT_EMAIL

gcloud projects delete $PROJECT
```