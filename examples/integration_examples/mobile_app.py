"""
Mobile App Integration Example for BNPL Feature Store
Demonstrates REST API integration for mobile applications
"""
import requests
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum


class CheckoutDecision(Enum):
    APPROVED = "approved"
    DECLINED = "declined" 
    PENDING = "pending"


@dataclass
class CheckoutRequest:
    user_id: str
    amount: float
    merchant: str
    currency: str = "USD"
    installments: int = 4


class BNPLMobileClient:
    """BNPL Mobile App Client - REST API Integration"""
    
    def __init__(self, api_base_url: str = "http://localhost:8000"):
        self.api_base_url = api_base_url
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "BNPL-Mobile-App/1.0"
        })
    
    def health_check(self) -> bool:
        """Check if feature store API is healthy"""
        try:
            response = self.session.get(f"{self.api_base_url}/health", timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def get_user_features(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user features for credit decision (mobile-optimized)"""
        try:
            start_time = time.time()
            
            response = self.session.get(
                f"{self.api_base_url}/api/v1/features/user/{user_id}",
                timeout=10  # Mobile apps need reasonable timeout
            )
            
            latency_ms = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                features = response.json()
                features['_meta'] = {
                    'latency_ms': round(latency_ms, 2),
                    'api_version': 'v1',
                    'cache_hit': features.get('cache_hit', False)
                }
                return features
            elif response.status_code == 404:
                print(f"⚠️ User {user_id} not found")
                return None
            else:
                print(f"❌ API Error: {response.status_code}")
                return None
                
        except requests.exceptions.Timeout:
            print("⏱️ Request timeout - check network connection")
            return None
        except requests.exceptions.RequestException as e:
            print(f"🌐 Network error: {e}")
            return None
    
    def make_credit_decision(self, checkout_request: CheckoutRequest) -> Dict[str, Any]:
        """Make real-time credit decision for BNPL checkout"""
        print(f"💳 Processing checkout: {checkout_request.user_id} - ${checkout_request.amount}")
        
        # Get user features
        features = self.get_user_features(checkout_request.user_id)
        
        if not features:
            return {
                "decision": CheckoutDecision.DECLINED.value,
                "reason": "Unable to assess user profile", 
                "recommendation": "Please try again later"
            }
        
        # Extract key risk indicators
        features.get('user_features', {})
        transaction_features = features.get('transaction_features', {})
        risk_features = features.get('risk_features', {})
        
        # Credit decision logic (simplified for demo)
        risk_score = risk_features.get('risk_score', 0.5)
        payment_delays = risk_features.get('payment_delays', 0)
        avg_order_value = transaction_features.get('avg_order_value', 0)
        total_orders = transaction_features.get('total_orders', 0)
        
        # Decision criteria
        decision_factors = {
            "risk_score": risk_score,
            "payment_history": payment_delays,
            "purchase_history": total_orders,
            "order_value_ratio": checkout_request.amount / max(avg_order_value, 50)
        }
        
        # Approval logic
        if risk_score < 0.2 and payment_delays == 0:
            decision = CheckoutDecision.APPROVED
            credit_limit = min(checkout_request.amount * 1.5, 1000)
        elif risk_score < 0.4 and payment_delays <= 1 and checkout_request.amount < 300:
            decision = CheckoutDecision.APPROVED  
            credit_limit = checkout_request.amount
        elif risk_score < 0.6 and total_orders > 5 and checkout_request.amount < 200:
            decision = CheckoutDecision.APPROVED
            credit_limit = checkout_request.amount
        else:
            decision = CheckoutDecision.DECLINED
            credit_limit = 0
        
        return {
            "decision": decision.value,
            "credit_limit": credit_limit,
            "installment_plan": self._generate_installment_plan(checkout_request, decision),
            "decision_factors": decision_factors,
            "processing_time_ms": features['_meta']['latency_ms'],
            "recommendation": self._get_recommendation(decision, risk_score)
        }
    
    def _generate_installment_plan(self, request: CheckoutRequest, decision: CheckoutDecision) -> Optional[Dict]:
        """Generate installment plan if approved"""
        if decision != CheckoutDecision.APPROVED:
            return None
        
        installment_amount = request.amount / request.installments
        
        return {
            "total_amount": request.amount,
            "installments": request.installments,
            "installment_amount": round(installment_amount, 2),
            "currency": request.currency,
            "payment_schedule": "bi-weekly",  # Every 2 weeks
            "first_payment_due": "immediate",
            "final_payment_due": f"{request.installments * 2} weeks"
        }
    
    def _get_recommendation(self, decision: CheckoutDecision, risk_score: float) -> str:
        """Get user-friendly recommendation"""
        if decision == CheckoutDecision.APPROVED:
            return "Enjoy your purchase! Pay in easy installments."
        elif risk_score > 0.8:
            return "Complete your profile to improve approval chances."
        else:
            return "Try a smaller amount or build purchase history with us."


def simulate_mobile_app_flow():
    """Simulate typical mobile app user flows"""
    print("📱 Mobile App Flow Simulation")
    print("=" * 30)
    
    client = BNPLMobileClient()
    
    # Check API health
    if not client.health_check():
        print("❌ Feature Store API not available")
        return
    
    print("✅ Connected to Feature Store API")
    
    # Simulate different user scenarios
    checkout_scenarios = [
        CheckoutRequest(
            user_id="user_000001",
            amount=299.99,
            merchant="SHEIN",
            installments=4
        ),
        CheckoutRequest(
            user_id="user_000002", 
            amount=89.99,
            merchant="IKEA",
            installments=3
        ),
        CheckoutRequest(
            user_id="user_000003",
            amount=799.99,  # High amount
            merchant="Apple Store", 
            installments=4
        ),
        CheckoutRequest(
            user_id="new_user_0001",  # New user
            amount=199.99,
            merchant="Amazon",
            installments=4
        )
    ]
    
    for i, scenario in enumerate(checkout_scenarios, 1):
        print(f"\n🛒 Checkout Scenario {i}:")
        print(f"   User: {scenario.user_id}")
        print(f"   Amount: ${scenario.amount}")
        print(f"   Merchant: {scenario.merchant}")
        
        decision_result = client.make_credit_decision(scenario)
        
        # Display results
        decision = decision_result['decision']
        processing_time = decision_result['processing_time_ms']
        
        if decision == CheckoutDecision.APPROVED.value:
            print(f"   ✅ APPROVED in {processing_time:.1f}ms")
            
            installment_plan = decision_result['installment_plan']
            if installment_plan:
                print(f"   💰 Pay ${installment_plan['installment_amount']} every 2 weeks")
                print(f"   📅 {installment_plan['installments']} payments total")
        else:
            print(f"   ❌ DECLINED in {processing_time:.1f}ms")
            print(f"   💡 {decision_result['recommendation']}")
        
        time.sleep(0.5)  # Simulate user interaction delay


def mobile_performance_test():
    """Test mobile app performance requirements"""
    print("\n📊 Mobile Performance Test")
    print("=" * 25)
    
    client = BNPLMobileClient()
    
    # Test performance with multiple rapid requests (like user tapping checkout repeatedly)
    test_user_ids = ["user_000001", "user_000002", "user_000003", "user_000004", "user_000005"]
    response_times = []
    
    print("Testing rapid checkout requests (mobile user behavior)...")
    
    for user_id in test_user_ids:
        checkout = CheckoutRequest(
            user_id=user_id,
            amount=199.99,
            merchant="Test Store"
        )
        
        start_time = time.time()
        result = client.make_credit_decision(checkout)
        end_time = time.time()
        
        total_time = (end_time - start_time) * 1000
        response_times.append(total_time)
        
        print(f"   {user_id}: {total_time:.1f}ms - {result['decision'].upper()}")
    
    # Performance summary
    avg_time = sum(response_times) / len(response_times)
    max_time = max(response_times)
    
    print("\n📈 Performance Results:")
    print(f"   Average response time: {avg_time:.1f}ms")
    print(f"   Maximum response time: {max_time:.1f}ms")
    
    # Mobile performance thresholds
    if avg_time < 100:
        print("   ✅ Excellent mobile performance (<100ms)")
    elif avg_time < 200:
        print("   ✅ Good mobile performance (<200ms)")
    elif avg_time < 500:
        print("   ⚠️ Acceptable mobile performance (<500ms)")
    else:
        print("   ❌ Poor mobile performance (>500ms)")


def main():
    """Main mobile app integration demo"""
    print("📱 BNPL Feature Store - Mobile App Integration")
    print("=" * 50)
    
    try:
        simulate_mobile_app_flow()
        mobile_performance_test()
        
        print("\n🎯 Mobile Integration Best Practices:")
        print("   • Keep API timeouts reasonable (5-10 seconds)")
        print("   • Handle network errors gracefully")  
        print("   • Cache user features when possible")
        print("   • Provide clear user feedback on decisions")
        print("   • Optimize for <200ms response times")
        
    except KeyboardInterrupt:
        print("\n⏹️ Demo interrupted by user")


if __name__ == "__main__":
    main()