"""
CI/CD Pipeline Integration Tests
Tests the deployment pipeline and environment configurations
"""
import os
import pytest
import requests
from unittest.mock import patch, MagicMock

class TestCICDPipeline:
    """Test CI/CD pipeline functionality"""
    
    def test_environment_variables_loaded(self):
        """Test that required environment variables are available"""
        required_vars = [
            'DATABASE_URL',
            'REDIS_URL', 
            'ENV'
        ]
        
        for var in required_vars:
            assert os.getenv(var) is not None, f"Missing required env var: {var}"
    
    def test_database_connection_in_ci(self):
        """Test database connectivity in CI environment"""
        from src.feature_store.database import get_database_connection
        
        conn = get_database_connection()
        assert conn is not None
        
        # Test basic query
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        assert result[0] == 1
        
        cursor.close()
        conn.close()
    
    def test_redis_connection_in_ci(self):
        """Test Redis connectivity in CI environment"""
        from src.feature_store.cache import get_redis_client
        
        redis_client = get_redis_client()
        assert redis_client.ping() is True
        
        # Test basic operations
        redis_client.set("test_key", "test_value")
        assert redis_client.get("test_key").decode() == "test_value"
        redis_client.delete("test_key")
    
    def test_api_health_check(self):
        """Test API health endpoint for deployment validation"""
        from src.main import app
        from fastapi.testclient import TestClient
        
        client = TestClient(app)
        response = client.get("/health")
        
        assert response.status_code == 200
        assert "status" in response.json()
        assert response.json()["status"] == "healthy"
    
    def test_grpc_server_startup(self):
        """Test gRPC server can start without errors"""
        from src.grpc_server import create_grpc_server
        
        server = create_grpc_server()
        assert server is not None
        
        # Test server can bind to port
        server.add_insecure_port('[::]:50051')
        server.start()
        server.stop(grace=1)
    
    def test_configuration_switching(self):
        """Test environment configuration switching"""
        from src.config.settings import get_settings
        
        # Test local config
        with patch.dict(os.environ, {'ENV': 'local'}):
            settings = get_settings()
            assert 'localhost' in settings.redis_url
        
        # Test GCP config  
        with patch.dict(os.environ, {'ENV': 'gcp'}):
            settings = get_settings()
            assert 'localhost' not in settings.redis_url


class TestDeploymentValidation:
    """Test deployment validation and rollback capabilities"""
    
    @pytest.mark.skipif(os.getenv('ENV') != 'staging', reason="Staging tests only")
    def test_staging_deployment_health(self):
        """Validate staging deployment is healthy"""
        staging_url = os.getenv('STAGING_URL')
        if not staging_url:
            pytest.skip("No staging URL configured")
        
        response = requests.get(f"{staging_url}/health", timeout=10)
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    @pytest.mark.skipif(os.getenv('ENV') != 'production', reason="Production tests only")
    def test_production_deployment_health(self):
        """Validate production deployment is healthy"""
        prod_url = os.getenv('PRODUCTION_URL')
        if not prod_url:
            pytest.skip("No production URL configured")
        
        response = requests.get(f"{prod_url}/health", timeout=10)
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    def test_performance_sla_validation(self):
        """Test that performance SLAs are met in CI"""
        from src.main import app
        from fastapi.testclient import TestClient
        import time
        
        client = TestClient(app)
        
        # Warm up
        client.get("/health")
        
        # Test latency SLA (should be <40ms for REST API)
        start_time = time.time()
        response = client.get("/api/v1/features/user/test_user_001")
        end_time = time.time()
        
        latency_ms = (end_time - start_time) * 1000
        
        # Allow higher latency in CI environment (no Redis cluster)
        assert latency_ms < 100, f"API latency {latency_ms}ms exceeds CI threshold"
        assert response.status_code in [200, 404]  # 404 is OK for test user
    
    def test_docker_image_security(self):
        """Basic security validation for Docker image"""
        # Test that we're not running as root
        import pwd
        import os
        
        # In production, we should not be root
        if os.getenv('ENV') == 'production':
            current_user = pwd.getpwuid(os.getuid()).pw_name
            assert current_user != 'root', "Should not run as root in production"


class TestGitLabCIPipeline:
    """Test GitLab CI-specific functionality"""
    
    def test_gitlab_ci_variables(self):
        """Test GitLab CI variables are available when needed"""
        ci_vars = [
            'CI_COMMIT_SHA',
            'CI_PIPELINE_ID', 
            'CI_JOB_ID'
        ]
        
        for var in ci_vars:
            # Only required when running in GitLab CI
            if os.getenv('GITLAB_CI'):
                assert os.getenv(var) is not None, f"Missing GitLab CI var: {var}"
    
    def test_coverage_report_generation(self):
        """Test that coverage reports can be generated"""
        import subprocess
        import tempfile
        
        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run([
                'python', '-m', 'pytest', 
                'tests/unit/', 
                '--cov=src', 
                f'--cov-report=xml:{tmpdir}/coverage.xml',
                '--tb=short'
            ], capture_output=True, text=True)
            
            # Coverage command should complete successfully
            assert result.returncode == 0 or result.returncode == 5  # 5 = no tests collected
            
            # Coverage file should be created
            coverage_file = f'{tmpdir}/coverage.xml'
            assert os.path.exists(coverage_file)
    
    @patch('subprocess.run')
    def test_docker_build_simulation(self, mock_subprocess):
        """Test Docker build process simulation"""
        mock_subprocess.return_value = MagicMock(returncode=0)
        
        import subprocess
        
        # Simulate docker build command
        result = subprocess.run([
            'docker', 'build', 
            '-t', 'feature-store:test',
            '.'
        ])
        
        assert result.returncode == 0
        mock_subprocess.assert_called_once()


class TestRollbackCapability:
    """Test deployment rollback functionality"""
    
    def test_health_check_for_rollback_decision(self):
        """Test health checks that would trigger rollback"""
        from src.main import app
        from fastapi.testclient import TestClient
        
        client = TestClient(app)
        
        # Test critical endpoints are working
        critical_endpoints = [
            "/health",
            "/api/v1/features/user/test_user"
        ]
        
        for endpoint in critical_endpoints:
            response = client.get(endpoint)
            # Should not return 5xx errors (would trigger rollback)
            assert response.status_code < 500, f"Endpoint {endpoint} returning 5xx"
    
    def test_configuration_rollback_safety(self):
        """Test that configuration changes are rollback-safe"""
        from src.config.settings import get_settings
        
        # Test with various environment configurations
        test_envs = ['local', 'staging', 'production']
        
        for env in test_envs:
            with patch.dict(os.environ, {'ENV': env}):
                try:
                    settings = get_settings()
                    assert settings is not None
                    assert hasattr(settings, 'redis_url')
                    assert hasattr(settings, 'database_url')
                except Exception as e:
                    pytest.fail(f"Configuration failed for env {env}: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])