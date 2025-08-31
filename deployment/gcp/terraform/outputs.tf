# Terraform outputs

output "feature_store_url" {
  description = "Feature Store API URL"
  value       = google_cloud_run_service.feature_store.status[0].url
}

output "feature_store_grpc_endpoint" {
  description = "Feature Store gRPC endpoint"
  value       = "${replace(google_cloud_run_service.feature_store.status[0].url, "https://", "")}:443"
}

output "database_info" {
  description = "Database connection information"
  value = {
    instance_name     = google_sql_database_instance.postgresql.name
    connection_name   = google_sql_database_instance.postgresql.connection_name
    private_ip        = google_sql_database_instance.postgresql.private_ip_address
    database_name     = google_sql_database.feature_store_db.name
  }
}

output "redis_info" {
  description = "Redis instance information"
  value = {
    host           = google_redis_instance.feature_store_redis.host
    port           = google_redis_instance.feature_store_redis.port
    memory_size_gb = google_redis_instance.feature_store_redis.memory_size_gb
    redis_version  = google_redis_instance.feature_store_redis.redis_version
  }
}

output "bigquery_info" {
  description = "BigQuery dataset information"
  value = {
    project_id = var.gcp_project_id
    dataset_id = google_bigquery_dataset.feature_store.dataset_id
    location   = google_bigquery_dataset.feature_store.location
  }
}

output "pubsub_info" {
  description = "Pub/Sub topic information"
  value = {
    topic_name        = google_pubsub_topic.feature_updates.name
    subscription_name = google_pubsub_subscription.feature_updates_sub.name
  }
}

output "vpc_info" {
  description = "VPC network information"
  value = {
    network_name    = google_compute_network.feature_store_vpc.name
    subnet_name     = google_compute_subnetwork.feature_store_subnet.name
    connector_name  = google_vpc_access_connector.feature_store_connector.name
  }
}

output "service_account_email" {
  description = "Service account email"
  value       = google_service_account.feature_store_sa.email
}

output "monitoring_info" {
  description = "Monitoring and alerting information"
  value = {
    alert_policies = [
      google_monitoring_alert_policy.high_latency.name,
      google_monitoring_alert_policy.high_error_rate.name
    ]
    notification_channel = google_monitoring_notification_channel.email.name
  }
}