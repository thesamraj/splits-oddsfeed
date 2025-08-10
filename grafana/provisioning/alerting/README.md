# Grafana Alerting Setup

To enable real alert notifications:

## Step 1: Create Local Contact Points File

Copy the example file to create your local configuration:

```bash
cp grafana/provisioning/alerting/contact-points.example.yml grafana/provisioning/alerting/contact-points.local.yml
```

## Step 2: Configure Slack Webhook

1. Get your Slack webhook URL from your Slack app/bot configuration
2. Edit `contact-points.local.yml` and replace `__SLACK_WEBHOOK_URL__` with your real webhook URL

## Step 3: Restart Grafana

Restart Grafana to load the new contact points:

```bash
docker compose restart grafana
```

**Note**: The `contact-points.local.yml` file is gitignored and will never be committed to protect your secrets.