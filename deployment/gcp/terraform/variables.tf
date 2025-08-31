# Terraform variables

variable "gcp_project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "gcp_region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment name (staging, production)"
  type        = string
  default     = "production"
}

variable "database_password" {
  description = "Database password"
  type        = string
  sensitive   = true
}

variable "alert_email" {
  description = "Email for monitoring alerts"
  type        = string
}

variable "enable_deletion_protection" {
  description = "Enable deletion protection for critical resources"
  type        = bool
  default     = true
}

variable "min_instances" {
  description = "Minimum number of Cloud Run instances"
  type        = number
  default     = 2
}

variable "max_instances" {
  description = "Maximum number of Cloud Run instances"
  type        = number
  default     = 50
}

variable "cpu_limit" {
  description = "CPU limit for Cloud Run instances"
  type        = string
  default     = "2"
}

variable "memory_limit" {
  description = "Memory limit for Cloud Run instances"
  type        = string
  default     = "2Gi"
}

variable "redis_memory_gb" {
  description = "Redis memory size in GB"
  type        = number
  default     = 4
}

variable "database_tier" {
  description = "Cloud SQL instance tier"
  type        = string
  default     = "db-custom-2-4096"
}

variable "backup_retention_days" {
  description = "Database backup retention in days"
  type        = number
  default     = 7
}