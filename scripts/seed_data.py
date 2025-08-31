#!/usr/bin/env python3
"""
Feature Store Data Seeder
Generates realistic BNPL transaction and user data for development/testing
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import argparse
import logging

import psycopg2  # type: ignore  # Install stubs: python3 -m pip install types-psycopg2
import redis
from faker import Faker
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BNPLDataGenerator:
    """Generate realistic BNPL data for feature store testing"""

    def __init__(self, locale: str = "en_US"):
        self.fake = Faker(locale)
        self.merchants = [
            "Amazon", "Noon", "IKEA", "SHEIN", "Zara", "H&M",
            "Nike", "Adidas", "Carrefour", "Lulu", "Virgin Megastore"
        ]
        self.categories = [
            "Electronics", "Fashion", "Home", "Beauty",
            "Sports", "Books", "Grocery", "Travel"
        ]
        self.locations = ["AE", "SA", "EG", "KW", "BH", "OM", "QA"]

    def generate_user(self, user_id: Optional[str] = None) -> Dict:
        if not user_id:
            user_id = f"user_{uuid.uuid4().hex[:8]}"
        age = random.choices(
            list(range(18, 65)),
            weights=[3 if 22 <= i <= 35 else 2 if 18 <= i <= 45 else 1 for i in range(18, 65)]
        )[0]
        base_income = random.uniform(30000, 150000)
        if age < 25:
            base_income *= 0.7
        elif age > 45:
            base_income *= 1.3
        location = random.choice(self.locations)
        if location in ["AE", "QA", "KW"]:
            base_income *= 1.4
        days_since_registration = random.randint(1, 1095)
        return {
            "user_id": user_id,
            "age": age,
            "location": location,
            "income_est": round(base_income, 2),
            "segment": self._determine_segment(age, base_income),
            "days_since_reg": days_since_registration,
            "registration_date": datetime.now() - timedelta(days=days_since_registration),
            "email": self.fake.email(),
            "phone": self.fake.phone_number()
        }

    def generate_user_behavior(self, user: Dict) -> Dict:
        if user["segment"] == "premium":
            total_orders = random.randint(8, 50)
            avg_order_value = random.uniform(300, 2000)
        elif user["segment"] == "regular":
            total_orders = random.randint(3, 25)
            avg_order_value = random.uniform(100, 800)
        else:
            total_orders = random.randint(1, 8)
            avg_order_value = random.uniform(50, 400)
        fav_category = random.choices(
            self.categories,
            weights=[2, 3, 1, 2, 1, 1, 1, 1] if user["age"] < 30 else [1, 2, 3, 2, 2, 2, 2, 1]
        )[0]
        return {
            "user_id": user["user_id"],
            "total_orders": total_orders,
            "avg_order_value": round(avg_order_value, 2),
            "fav_category": fav_category,
            "session_duration": round(random.uniform(2.5, 45.0), 1),
            "last_order_days": random.randint(0, 90)
        }

    def generate_risk_features(self, user: Dict, behavior: Dict) -> Dict:
        if behavior["total_orders"] < 3:
            payment_delays = random.choices([0, 1, 2], weights=[0.7, 0.2, 0.1])[0]
            default_prob = random.uniform(0.02, 0.12)
        elif user["segment"] == "premium":
            payment_delays = random.choices([0, 1, 2], weights=[0.9, 0.08, 0.02])[0]
            default_prob = random.uniform(0.005, 0.03)
        else:
            payment_delays = random.choices([0, 1, 2, 3], weights=[0.8, 0.12, 0.05, 0.03])[0]
            default_prob = random.uniform(0.01, 0.08)
        utilization_rate = random.uniform(0.1, 0.85)
        credit_age_months = min(user["days_since_reg"] // 30, 36)
        return {
            "user_id": user["user_id"],
            "payment_delays": payment_delays,
            "utilization_rate": round(utilization_rate, 3),
            "credit_age": credit_age_months,
            "default_probability": round(default_prob, 4)
        }

    def generate_transactions(
        self,
        user: Dict,
        behavior: Dict,
        num_transactions: Optional[int] = None
    ) -> List[Dict]:
        if num_transactions is None:
            num_transactions = min(behavior["total_orders"], 20)
        transactions: List[Dict] = []
        start_date = user["registration_date"]
        for _ in range(num_transactions):
            days_offset = random.randint(0, max(0, user["days_since_reg"]))
            transaction_date = start_date + timedelta(days=days_offset)
            if user["segment"] == "premium":
                amount = random.uniform(200, 2500)
            elif user["segment"] == "regular":
                amount = random.uniform(80, 1200)
            else:
                amount = random.uniform(30, 500)
            status = random.choices(
                ["approved", "declined", "failed", "pending"],
                weights=[0.92, 0.05, 0.02, 0.01]
            )[0]
            transactions.append({
                "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
                "user_id": user["user_id"],
                "amount": round(amount, 2),
                "merchant": random.choice(self.merchants),
                "category": behavior["fav_category"] if random.random() < 0.6 else random.choice(self.categories),
                "status": status,
                "timestamp": transaction_date,
                "installments": random.choices([1, 3, 4, 6], weights=[0.3, 0.4, 0.2, 0.1])[0]
            })
        return sorted(transactions, key=lambda x: x["timestamp"])

    def _determine_segment(self, age: int, income: float) -> str:
        if income > 100000 and age >= 28:
            return "premium"
        elif income > 50000 or age >= 25:
            return "regular"
        return "new"


class DataSeeder:
    """Seed data into PostgreSQL, Redis, and BigQuery"""

    def __init__(self, config: Dict):
        self.config = config or {}
        self.generator = BNPLDataGenerator()
        self.pg_conn: Optional[psycopg2.extensions.connection] = None
        self.redis_client: Optional[redis.Redis] = None
        self.bq_client: Optional[bigquery.Client] = None

    def setup_connections(self):
        try:
            # PostgreSQL
            pg_cfg = self.config.get("postgresql")
            if pg_cfg:
                if "port" in pg_cfg and isinstance(pg_cfg["port"], str):
                    try:
                        pg_cfg["port"] = int(pg_cfg["port"])
                    except ValueError:
                        pass
                self.pg_conn = psycopg2.connect(**pg_cfg)
                logger.info("PostgreSQL connection established")

            # Redis
            redis_cfg = self.config.get("redis")
            if redis_cfg:
                if "port" in redis_cfg and isinstance(redis_cfg["port"], str):
                    try:
                        redis_cfg["port"] = int(redis_cfg["port"])
                    except ValueError:
                        pass
                self.redis_client = redis.Redis(**redis_cfg)
                try:
                    self.redis_client.ping()
                    logger.info("Redis connection established")
                except Exception:
                    logger.warning("Redis ping failed; continuing without Redis")

            # BigQuery
            bq_cfg = self.config.get("bigquery")
            if bq_cfg:
                project = bq_cfg.get("project")
                self.bq_client = bigquery.Client(project=project) if project else bigquery.Client()
                logger.info("BigQuery client initialized")

        except Exception as e:
            logger.error(f"Connection setup failed: {e}")
            raise

        def seed_users(self, num_users: int = 10000):
            """Generate and insert user data"""
            logger.info(f"Generating {num_users} users...")
    
            users: List[Dict] = []
            behaviors: List[Dict] = []
            risks: List[Dict] = []
    
            for i in range(num_users):
                if i % 1000 == 0 and i > 0:
                    logger.info(f"Generated {i} users...")
    
                user = self.generator.generate_user()
                behavior = self.generator.generate_user_behavior(user)
                risk = self.generator.generate_risk_features(user, behavior)
    
                users.append(user)
                behaviors.append(behavior)
                risks.append(risk)
    
            if self.pg_conn:
                self._insert_postgres_users(users, behaviors, risks)
    
            if self.redis_client:
                self._cache_sample_users(users[:100], behaviors[:100], risks[:100])
    
            if self.bq_client:
                self._insert_bigquery_users(users, behaviors, risks)
    
            logger.info(f"Successfully seeded {num_users} users")
            return users, behaviors, risks

    def seed_transactions(self, users: List[Dict], num_per_user: int = 5):
        """Generate and insert transaction data"""
        logger.info(f"Generating transactions for {len(users)} users...")
        all_transactions: List[Dict] = []

        for i, user in enumerate(users):
            if i % 500 == 0 and i > 0:
                logger.info(f"Generated transactions for {i} users...")

            behavior = self.generator.generate_user_behavior(user)
            transactions = self.generator.generate_transactions(user, behavior, num_per_user)
            all_transactions.extend(transactions)

        if self.pg_conn:
            self._insert_postgres_transactions(all_transactions)

        if self.bq_client:
            self._insert_bigquery_transactions(all_transactions)

        logger.info(f"Successfully seeded {len(all_transactions)} transactions")
        return all_transactions

    def _insert_postgres_users(self, users: List[Dict], behaviors: List[Dict], risks: List[Dict]):
        try:
            if not self.pg_conn:
                logger.warning("PostgreSQL connection not available")
                return

            with self.pg_conn.cursor() as cur:
                user_values = [(u["user_id"], u["age"], u["location"], u["income_est"],
                                u["segment"], u["days_since_reg"], u["registration_date"])
                               for u in users]
                cur.executemany("""
                    INSERT INTO users (user_id, age, location, income_est, segment,
                                     days_since_reg, registration_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO NOTHING
                """, user_values)

                behavior_values = [(b["user_id"], b["total_orders"], b["avg_order_value"],
                                    b["fav_category"], b["session_duration"])
                                   for b in behaviors]
                cur.executemany("""
                    INSERT INTO user_behavior (user_id, total_orders, avg_order_value,
                                             fav_category, session_duration)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET
                        total_orders = EXCLUDED.total_orders,
                        avg_order_value = EXCLUDED.avg_order_value,
                        fav_category = EXCLUDED.fav_category,
                        session_duration = EXCLUDED.session_duration
                """, behavior_values)

                risk_values = [(r["user_id"], r["payment_delays"], r["utilization_rate"],
                                r["credit_age"], r["default_probability"])
                               for r in risks]
                cur.executemany("""
                    INSERT INTO user_risk (user_id, payment_delays, utilization_rate,
                                         credit_age, default_probability)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET
                        payment_delays = EXCLUDED.payment_delays,
                        utilization_rate = EXCLUDED.utilization_rate,
                        credit_age = EXCLUDED.credit_age,
                        default_probability = EXCLUDED.default_probability
                """, risk_values)

            self.pg_conn.commit()
            logger.info("PostgreSQL user data inserted successfully")

        except Exception as e:
            if self.pg_conn:
                try:
                    self.pg_conn.rollback()
                except Exception:
                    logger.warning("Rollback failed or connection not available")
            logger.error(f"PostgreSQL insertion failed: {e}")
            raise

    def _insert_postgres_transactions(self, transactions: List[Dict]):
        try:
            if not self.pg_conn:
                logger.warning("PostgreSQL connection not available")
                return

            with self.pg_conn.cursor() as cur:
                values = [(t["transaction_id"], t["user_id"], t["amount"], t["merchant"],
                           t["category"], t["status"], t["timestamp"], t["installments"])
                          for t in transactions]
                cur.executemany("""
                    INSERT INTO transactions (transaction_id, user_id, amount, merchant,
                                            category, status, timestamp, installments)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                """, values)

            self.pg_conn.commit()
            logger.info("PostgreSQL transaction data inserted successfully")

        except Exception as e:
            if self.pg_conn:
                try:
                    self.pg_conn.rollback()
                except Exception:
                    logger.warning("Rollback failed or connection not available")
            logger.error(f"PostgreSQL transaction insertion failed: {e}")

    def _cache_sample_users(self, users: List[Dict], behaviors: List[Dict], risks: List[Dict]):
        if not self.redis_client:
            logger.warning("Redis client not available; skipping caching")
            return

        try:
            pipeline = self.redis_client.pipeline()
            for user, behavior, risk in zip(users, behaviors, risks):
                cache_data = {
                    "demographics": {
                        "age": user["age"],
                        "location": user["location"],
                        "income_est": user["income_est"],
                        "segment": user["segment"]
                    },
                    "behavior": {
                        "total_orders": behavior["total_orders"],
                        "avg_order_value": behavior["avg_order_value"],
                        "fav_category": behavior["fav_category"],
                        "session_duration": behavior["session_duration"]
                    },
                    "risk": {
                        "payment_delays": risk["payment_delays"],
                        "utilization_rate": risk["utilization_rate"],
                        "credit_age": risk["credit_age"],
                        "default_probability": risk["default_probability"]
                    },
                    "cache_hit": True,
                    "timestamp": datetime.now().timestamp()
                }
                pipeline.setex(f"features:user:{user['user_id']}", 3600, json.dumps(cache_data, default=str))
            pipeline.execute()
            logger.info("Redis sample data cached successfully")
        except Exception as e:
            logger.error(f"Redis caching failed: {e}")

    def _insert_bigquery_users(self, users: List[Dict], behaviors: List[Dict], risks: List[Dict]):
        if not self.bq_client:
            logger.warning("BigQuery client not available; skipping user insert")
            return

        bq_data = []
        for user, behavior, risk in zip(users, behaviors, risks):
            bq_data.append({
                "user_id": user["user_id"],
                "age": user["age"],
                "location": user["location"],
                "income_est": user["income_est"],
                "segment": user["segment"],
                "days_since_reg": user["days_since_reg"],
                "total_orders": behavior["total_orders"],
                "avg_order_value": behavior["avg_order_value"],
                "fav_category": behavior["fav_category"],
                "payment_delays": risk["payment_delays"],
                "utilization_rate": risk["utilization_rate"],
                "default_probability": risk["default_probability"],
                "created_at": datetime.utcnow().isoformat()
            })

        dataset = self.config.get('bigquery', {}).get('dataset', 'features')
        table_id = f"{dataset}.user_features"
        try:
            table = self.bq_client.get_table(table_id)
            errors = self.bq_client.insert_rows_json(table, bq_data)
            if errors:
                logger.error(f"BigQuery insertion errors: {errors}")
            else:
                logger.info("BigQuery user data inserted successfully")
        except Exception as e:
            logger.error(f"BigQuery table insert failed: {e}")

    def _insert_bigquery_transactions(self, transactions: List[Dict]):
        if not self.bq_client:
            logger.warning("BigQuery client not available; skipping transaction insert")
            return

        bq_data = []
        for txn in transactions:
            bq_data.append({
                "transaction_id": txn["transaction_id"],
                "user_id": txn["user_id"],
                "amount": txn["amount"],
                "merchant": txn["merchant"],
                "category": txn["category"],
                "status": txn["status"],
                "installments": txn["installments"],
                "timestamp": txn["timestamp"].isoformat() if isinstance(txn["timestamp"], datetime) else str(txn["timestamp"]),
                "created_at": datetime.utcnow().isoformat()
            })

        dataset = self.config.get('bigquery', {}).get('dataset', 'features')
        table_id = f"{dataset}.transactions"
        try:
            table = self.bq_client.get_table(table_id)
            batch_size = 1000
            for i in range(0, len(bq_data), batch_size):
                batch = bq_data[i:i + batch_size]
                errors = self.bq_client.insert_rows_json(table, batch)
                if errors:
                    logger.error(f"BigQuery batch {i//batch_size} insertion errors: {errors}")
            logger.info("BigQuery transaction data inserted successfully")
        except Exception as e:
            logger.error(f"BigQuery table insert failed: {e}")


    def cleanup(self):
        try:
            if self.pg_conn:
                try:
                    self.pg_conn.close()
                except Exception:
                    logger.warning("Failed to close PostgreSQL connection")
            if self.redis_client:
                try:
                    close_fn = getattr(self.redis_client, "close", None)
                    if callable(close_fn):
                        self.redis_client.close()
                    else:
                        try:
                            self.redis_client.connection_pool.disconnect()
                        except Exception:
                            pass
                except Exception:
                    logger.warning("Failed to close Redis client")
        except Exception as e:
            logger.debug(f"Cleanup encountered issues: {e}")

    def seed_users(self, users):
        raise NotImplementedError


def load_config(config_file: str) -> Dict:
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"Config file {config_file} not found, using defaults")
        return {
            "postgresql": {
                "host": "localhost",
                "port": 5432,
                "database": "feature_store",
                "user": "postgres",
                "password": "postgres"
            },
            "redis": {
                "host": "localhost",
                "port": 6379,
                "db": 0
            },
            "bigquery": {
                "project": "feature-store-dev",
                "dataset": "features"
            }
        }


def main():
    parser = argparse.ArgumentParser(description='Seed BNPL feature store with realistic data')
    parser.add_argument('--users', type=int, default=10000)
    parser.add_argument('--transactions-per-user', type=int, default=5)
    parser.add_argument('--config', default='config/seed_config.json')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    config = load_config(args.config)
    seeder = DataSeeder(config)

    try:
        if not args.dry_run:
            seeder.setup_connections()

        start_time = datetime.now()
        users, behaviors, risks = seeder.seed_users(args.users)

        if not args.dry_run:
            seeder.seed_transactions(users, args.transactions_per_user)
        else:
            logger.info("Dry run - data generated but not inserted")

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Seeding completed in {duration:.2f} seconds")

    finally:
        seeder.cleanup()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
