# app.py
import logging
import json
import random
import time
from datetime import datetime
from typing import Dict, List
import uuid

class EcommerceApp:
    def __init__(self):
        self.setup_logging()
        self.users = self.generate_users(100)
        self.products = self.generate_products(50)
        # Define traffic patterns for each hour (0-23)
        # Values are multipliers for base number of user journeys
        self.traffic_patterns = self.define_traffic_patterns()
        self.error_types = self.define_error_types()

    def setup_logging(self):
        """Configuration du logging structuré"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s',
            handlers=[
                logging.FileHandler('app.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def log_event(self, event_type: str, data: Dict):
        """Génère un log structuré"""
        try:
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'event_type': event_type,
                'session_id': str(uuid.uuid4()),
                'user_id': data.get('user_id'),
                'ip_address': self.generate_ip(), # IP address is still random per event
                'user_agent': self.generate_user_agent(),
                'location': data.get('location'), # Add user's location to the log
                'data': data
            }
            self.logger.info(json.dumps(log_entry))
        except TypeError as e:
            self.logger.error(f"Error serializing log entry for event_type '{event_type}': {e}. Data: {data}")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in log_event for event_type '{event_type}': {e}. Data: {data}")

    def define_traffic_patterns(self) -> List[float]:
        """Définit les patterns de trafic (multiplicateurs) pour chaque heure de la journée."""
        # Exemple de pattern de trafic : plus d'activité en journée et en soirée
        # Heures creuses (nuit) : 0-5
        # Matin : 6-9
        # Journée/Après-midi : 10-16 (pic léger)
        # Soirée (pic) : 17-21
        # Fin de soirée : 22-23
        patterns = [
            0.2, 0.1, 0.1, 0.1, 0.2, 0.3,  # 0-5 AM
            0.5, 0.7, 0.9, 1.0,            # 6-9 AM
            1.2, 1.3, 1.1, 1.0, 1.0, 1.1, 1.2, # 10 AM - 4 PM
            1.5, 1.8, 2.0, 1.7, 1.4, 1.0, 0.7 # 5 PM - 11 PM
        ]
        return patterns

    def define_error_types(self) -> List[Dict]:
        """Définit différents types d'erreurs et leurs détails."""
        return [
            {'code': 'LOGIN_FAILED', 'message': 'Invalid credentials or server error during login.'},
            {'code': 'PAYMENT_FAILED', 'message': 'Payment gateway declined transaction or insufficient funds.'},
            {'code': 'PRODUCT_NOT_FOUND', 'message': 'Requested product does not exist.'},
            {'code': 'OUT_OF_STOCK', 'message': 'Product is out of stock and cannot be added to cart/purchased.'},
            {'code': 'CHECKOUT_ERROR', 'message': 'General error during checkout processing.'},
            {'code': 'SERVER_TIMEOUT', 'message': 'API request timed out.'},
            {'code': 'DATABASE_ERROR', 'message': 'Database connection failed or query error.'},
            {'code': 'INVALID_INPUT', 'message': 'User provided invalid data in a form.'}
        ]

    def get_random_error_details(self) -> Dict:
        """Retourne des détails aléatoires sur une erreur."""
        return random.choice(self.error_types)

    def generate_users(self, count: int) -> List[Dict]:
        """Génère une liste d'utilisateurs factices avec géolocalisation."""
        users = []
        countries = ['USA', 'Canada', 'France', 'Germany', 'UK', 'Australia', 'Japan', 'Brazil', 'India']
        for i in range(count):
            users.append({
                'user_id': str(uuid.uuid4()),
                'username': f'user_{i}',
                'email': f'user_{i}@example.com',
                'location': random.choice(countries) # Assign a random country
            })
        return users

    def generate_products(self, count: int) -> List[Dict]:
        """Génère une liste de produits factices."""
        products = []
        categories = ['Electronics', 'Books', 'Clothing', 'Home & Kitchen', 'Sports']
        for i in range(count):
            products.append({
                'product_id': str(uuid.uuid4()),
                'name': f'Product {i}',
                'category': random.choice(categories),
                'price': round(random.uniform(10.0, 1000.0), 2),
                'stock': random.randint(0, 200)
            })
        return products

    def generate_ip(self) -> str:
        """Génère une adresse IP aléatoire."""
        return f"{random.randint(1,254)}.{random.randint(0,254)}.{random.randint(0,254)}.{random.randint(1,254)}"

    def generate_user_agent(self) -> str:
        """Génère un user agent aléatoire."""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36", # Desktop Chrome
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15", # Desktop Safari
            "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Mobile Safari/537.36", # Android Chrome
            "Mozilla/5.0 (iPhone; CPU iPhone OS 15_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/100.0.4896.75 Mobile/15E148 Safari/604.1", # iOS Chrome
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0", # Desktop Firefox
            "Mozilla/5.0 (iPad; CPU OS 13_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/83.0.4103.88 Mobile/15E148 Safari/604.1" # iPad Chrome
        ]
        return random.choice(user_agents)

    def simulate_user_journey(self, current_hour: int):
        """Simule un parcours utilisateur complet avec différents événements, influencé par l'heure."""
        if not self.users:
            self.logger.error("No users available for simulation. Please check generate_users.")
            return

        user = random.choice(self.users)
        user_id = user['user_id']
        user_location = user['location']

        # 0. Page View (initial landing)
        self.log_event('page_view', {'user_id': user_id, 'page_url': '/', 'location': user_location})
        time.sleep(random.uniform(0.1, 0.5))

        # 1. User Login/Registration
        event_type = random.choice(['login', 'user_registration'])
        self.log_event(event_type, {'user_id': user_id, 'location': user_location})
        time.sleep(random.uniform(0.1, 0.5))

        # Simulate a potential error after login (higher chance during peak hours)
        if random.random() < (0.05 + self.traffic_patterns[current_hour] * 0.02): # Base 5% + up to 4% more during peak
            error_details = self.get_random_error_details()
            self.log_event('error', {'user_id': user_id, 'error_code': error_details['code'], 'message': error_details['message'], 'location': user_location})
            time.sleep(random.uniform(0.1, 0.3))

        # 2. Product Browsing (multiple times)
        num_browsed_products = random.randint(1, 5)
        if not self.products:
            self.log_event('error', {'user_id': user_id, 'error_code': 'NO_PRODUCTS_AVAILABLE', 'message': 'Cannot browse, no products in catalog', 'location': user_location})
            return
        browsed_products = random.sample(self.products, min(len(self.products), num_browsed_products))

        for prod in browsed_products:
            self.log_event('product_view', {
                'user_id': user_id,
                'product_id': prod['product_id'],
                'product_name': prod['name'],
                'price': prod['price'],
                'location': user_location
            })
            time.sleep(random.uniform(0.1, 0.3))
            self.log_event('page_view', {'user_id': user_id, 'page_url': f'/products/{prod["product_id"]}', 'location': user_location})

            # Simulate product not found error
            if random.random() < 0.01: # 1% chance of product not found error
                error_details = next((e for e in self.error_types if e['code'] == 'PRODUCT_NOT_FOUND'), {'code': 'PRODUCT_NOT_FOUND', 'message': 'Product not found'})
                self.log_event('error', {'user_id': user_id, 'error_code': error_details['code'], 'message': error_details['message'], 'product_id': 'non-existent-id', 'location': user_location})
                time.sleep(random.uniform(0.1, 0.3))


        # 3. Search (optional, more likely during peak hours)
        if random.random() < (0.5 + self.traffic_patterns[current_hour] * 0.1): # Base 50% + up to 20% more during peak
            search_term = random.choice(['laptop', 'book', 'shirt', 'kitchen', 'ball', 'smartwatch', 'headphones'])
            self.log_event('search', {
                'user_id': user_id,
                'search_term': search_term,
                'results_count': random.randint(0, 20),
                'location': user_location
            })
            time.sleep(random.uniform(0.1, 0.5))
            self.log_event('page_view', {'user_id': user_id, 'page_url': f'/search?q={search_term}', 'location': user_location})

        # 4. Add to Cart (1 to 3 products, more likely during peak hours)
        cart_products = []
        if browsed_products:
            cart_products = random.sample(browsed_products, min(len(browsed_products), random.randint(1, 3)))

        cart_items_data = []
        for prod in cart_products:
            quantity = random.randint(1, 2)
            # Simulate out of stock error
            if random.random() < 0.03 and prod['stock'] < quantity: # 3% chance if stock is low
                error_details = next((e for e in self.error_types if e['code'] == 'OUT_OF_STOCK'), {'code': 'OUT_OF_STOCK', 'message': 'Product is out of stock'})
                self.log_event('error', {'user_id': user_id, 'error_code': error_details['code'], 'message': error_details['message'], 'product_id': prod['product_id'], 'location': user_location})
                time.sleep(random.uniform(0.1, 0.3))
                continue # Skip adding this product to cart

            self.log_event('add_to_cart', {
                'user_id': user_id,
                'product_id': prod['product_id'],
                'product_name': prod['name'],
                'quantity': quantity,
                'price': prod['price'],
                'location': user_location
            })
            cart_items_data.append({
                'product_id': prod['product_id'],
                'quantity': quantity,
                'price': prod['price']
            })
            time.sleep(random.uniform(0.1, 0.3))
            self.log_event('page_view', {'user_id': user_id, 'page_url': '/cart', 'location': user_location})

        # 5. Remove from Cart (optional, if cart has items)
        if cart_items_data and random.random() < 0.3:
            item_to_remove = random.choice(cart_items_data)
            self.log_event('remove_from_cart', {
                'user_id': user_id,
                'product_id': item_to_remove['product_id'],
                'location': user_location
            })
            cart_items_data = [item for item in cart_items_data if item['product_id'] != item_to_remove['product_id']]
            time.sleep(random.uniform(0.1, 0.3))
            self.log_event('page_view', {'user_id': user_id, 'page_url': '/cart', 'location': user_location})

        # 6. Checkout (only if cart has items)
        if cart_items_data:
            total_amount = sum(item['quantity'] * item['price'] for item in cart_items_data)
            order_id = str(uuid.uuid4())
            self.log_event('checkout_initiated', {
                'user_id': user_id,
                'order_id': order_id,
                'total_amount': round(total_amount, 2),
                'number_of_items': len(cart_items_data),
                'location': user_location
            })
            time.sleep(random.uniform(0.5, 1.5))
            self.log_event('page_view', {'user_id': user_id, 'page_url': '/checkout', 'location': user_location})

            # Simulate a potential error during checkout (higher chance during peak hours)
            if random.random() < (0.05 + self.traffic_patterns[current_hour] * 0.03): # Base 5% + up to 6% more during peak
                error_details = random.choice([
                    next((e for e in self.error_types if e['code'] == 'PAYMENT_FAILED')),
                    next((e for e in self.error_types if e['code'] == 'CHECKOUT_ERROR')),
                    next((e for e in self.error_types if e['code'] == 'SERVER_TIMEOUT'))
                ])
                self.log_event('error', {'user_id': user_id, 'error_code': error_details['code'], 'message': error_details['message'], 'order_id': order_id, 'location': user_location})
                time.sleep(random.uniform(0.1, 0.3))
                # If checkout fails, user might abandon or retry (for simplicity, we abandon)
                self.log_event('cart_abandoned', {'user_id': user_id, 'reason': 'checkout_error', 'order_id': order_id, 'location': user_location})
                time.sleep(random.uniform(0.1, 0.5))
                return # End journey if checkout failed

            # 7. Purchase (Order Confirmation)
            self.log_event('purchase', {
                'user_id': user_id,
                'order_id': order_id,
                'total_amount': round(total_amount, 2),
                'items': cart_items_data,
                'payment_method': random.choice(['credit_card', 'paypal', 'bank_transfer']),
                'location': user_location
            })
            time.sleep(random.uniform(0.1, 0.5))
            self.log_event('page_view', {'user_id': user_id, 'page_url': f'/order-confirmation/{order_id}', 'location': user_location})

        else:
            self.log_event('cart_abandoned', {'user_id': user_id, 'reason': 'no_items_in_cart', 'location': user_location})
            time.sleep(random.uniform(0.1, 0.5))

        # 8. Add to Wishlist (optional)
        if random.random() < 0.2:
            if not self.products:
                self.log_event('error', {'user_id': user_id, 'error_code': 'NO_PRODUCTS_FOR_WISHLIST', 'message': 'Cannot add to wishlist, no products in catalog', 'location': user_location})
            else:
                wishlist_product = random.choice(self.products)
                self.log_event('add_to_wishlist', {
                    'user_id': user_id,
                    'product_id': wishlist_product['product_id'],
                    'product_name': wishlist_product['name'],
                    'location': user_location
                })
                time.sleep(random.uniform(0.1, 0.3))
                self.log_event('page_view', {'user_id': user_id, 'page_url': '/wishlist', 'location': user_location})

        # 9. Submit Review (optional, after purchase)
        if cart_items_data and random.random() < 0.1:
            if not cart_products:
                self.logger.warning(f"Attempted to submit review for user {user_id} but cart_products was empty.")
            else:
                reviewed_product = random.choice(cart_products)
                self.log_event('submit_review', {
                    'user_id': user_id,
                    'product_id': reviewed_product['product_id'],
                    'rating': random.randint(1, 5),
                    'review_text': f"Great product! Very satisfied with {reviewed_product['name']}.",
                    'location': user_location
                })
                time.sleep(random.uniform(0.1, 0.5))
                self.log_event('page_view', {'user_id': user_id, 'page_url': f'/products/{reviewed_product["product_id"]}/review', 'location': user_location})

        # 10. Logout (optional, more likely after peak hours)
        if random.random() < (0.8 - self.traffic_patterns[current_hour] * 0.1): # Less likely during peak, more likely off-peak
            self.log_event('logout', {'user_id': user_id, 'location': user_location})
            time.sleep(random.uniform(0.1, 0.5))
            self.log_event('page_view', {'user_id': user_id, 'page_url': '/logout', 'location': user_location})

        self.log_event('user_session_end', {'user_id': user_id, 'duration_seconds': random.randint(30, 300), 'location': user_location})

    def run_simulation(self, num_days: int = 1):
        """Exécute la simulation pour un nombre donné de jours, en tenant compte des patterns de trafic."""
        self.logger.info(f"Démarrage de la simulation pour {num_days} jours...")
        for day in range(1, num_days + 1):
            for hour in range(24):
                # Calculate number of journeys for this hour based on traffic pattern
                base_journeys_per_hour = 5 # Adjust this base number as needed
                journeys_this_hour = int(base_journeys_per_hour * self.traffic_patterns[hour])
                journeys_this_hour = max(1, journeys_this_hour) # Ensure at least 1 journey per hour

                self.logger.info(f"Simulating Day {day}, Hour {hour}:00 - {journeys_this_hour} user journeys expected.")
                for i in range(journeys_this_hour):
                    self.logger.info(f"  Simulating user journey {i+1}/{journeys_this_hour} for Day {day}, Hour {hour}:00")
                    try:
                        self.simulate_user_journey(current_hour=hour)
                    except Exception as e:
                        self.logger.error(f"An error occurred during user journey {i+1} at Day {day}, Hour {hour}: {e}", exc_info=True)
                    time.sleep(random.uniform(0.1, 0.5)) # Shorter pause between journeys within an hour
                time.sleep(random.uniform(0.5, 2.0)) # Longer pause between hours

        self.logger.info("Simulation terminée.")

if __name__ == "__main__":
    app = EcommerceApp()
    # Exécuter la simulation pour 2 jours
    app.run_simulation(num_days=2)
