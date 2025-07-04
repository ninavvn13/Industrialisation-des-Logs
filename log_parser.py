# log_parser.py
import json
from typing import Dict, List, Any
import logging
from datetime import datetime

class LogParser:
    def __init__(self):
        # Liste pour stocker les lignes de log qui n'ont pas pu être parsées ou validées
        self.failed_logs: List[Dict[str, Any]] = []
        # Statistiques sur le parsing
        self.stats = {'parsed': 0, 'failed': 0}
        # Configuration du logger pour le parser
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('parser.log'), # Logs spécifiques au parser
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def parse_log_line(self, line: str) -> Dict | None:
        """
        Parse une ligne de log JSON.
        Gère les logs malformés et tente une re-tentative simple.
        Retourne le dictionnaire parsé si succès, sinon None.
        """
        parsed_entry = None
        try:
            parsed_entry = json.loads(line)
            # Si le parsing réussit, valider l'entrée
            if self.validate_log_entry(parsed_entry):
                self.stats['parsed'] += 1
                return parsed_entry
            else:
                # Log non valide après parsing
                error_message = f"Validation failed for log entry: {line}"
                self.logger.warning(error_message)
                self._handle_failed_log(line, "validation_failed", error_message)
                return None
        except json.JSONDecodeError as e:
            # Gérer les logs malformés (non-JSON)
            error_message = f"Malformed JSON log line: {e} - Line: {line.strip()}"
            self.logger.error(error_message)
            self._handle_failed_log(line, "json_decode_error", error_message)
            return None
        except Exception as e:
            # Gérer toute autre exception inattendue
            error_message = f"Unexpected error parsing log line: {e} - Line: {line.strip()}"
            self.logger.error(error_message)
            self._handle_failed_log(line, "unexpected_error", error_message)
            return None

    def validate_log_entry(self, entry: Dict) -> bool:
        """
        Valide la structure et les types de données d'une entrée de log.
        Retourne True si l'entrée est valide, False sinon.
        """
        required_fields = {
            'timestamp': str,
            'event_type': str,
            'session_id': str,
            'user_id': str,
            'ip_address': str,
            'user_agent': str,
            'location': str,
            'data': dict
        }

        for field, expected_type in required_fields.items():
            if field not in entry:
                self.logger.debug(f"Validation Error: Missing required field '{field}' in log entry: {entry}")
                return False
            if not isinstance(entry[field], expected_type):
                self.logger.debug(f"Validation Error: Field '{field}' has incorrect type. Expected {expected_type}, got {type(entry[field])} in entry: {entry}")
                return False

        # Validation spécifique pour le format de timestamp (ISO format)
        try:
            datetime.fromisoformat(entry['timestamp'])
        except ValueError:
            self.logger.debug(f"Validation Error: Invalid timestamp format for '{entry['timestamp']}' in entry: {entry}")
            return False

        # Validation spécifique pour 'data' (doit contenir 'user_id' pour la plupart des événements)
        if 'user_id' not in entry['data']:
             # Certains événements comme 'error' ou 'user_session_end' pourraient ne pas avoir user_id dans data,
             # mais le champ user_id à la racine est toujours là.
             # Pour la robustesse, on peut décider de rendre user_id dans data optionnel ou de le valider conditionnellement.
             # Ici, on le rend optionnel dans 'data' car il est déjà à la racine.
            pass

        return True

    def _handle_failed_log(self, original_line: str, reason: str, message: str):
        """
        Enregistre un log échoué pour une analyse ultérieure.
        Implémente un système de "retry" simple en stockant les logs.
        """
        self.stats['failed'] += 1
        self.failed_logs.append({
            'timestamp': datetime.now().isoformat(),
            'original_line': original_line.strip(),
            'reason': reason,
            'message': message
        })
        # Pour un système de retry plus avancé, on pourrait ici ajouter une logique
        # pour re-tenter le parsing après un certain délai ou un certain nombre de fois.
        # Pour cet exercice, le stockage dans failed_logs sert de mécanisme de "retry" passif.

    def get_stats(self) -> Dict[str, int]:
        """Retourne les statistiques de parsing."""
        return self.stats

    def get_failed_logs(self) -> List[Dict[str, Any]]:
        """Retourne la liste des logs qui n'ont pas pu être parsés ou validés."""
        return self.failed_logs

# --- Exemple d'utilisation du LogParser ---
if __name__ == "__main__":
    parser = LogParser()

    # Exemples de logs valides
    valid_logs = [
        '{"timestamp": "2025-07-03T16:29:10.565498", "event_type": "page_view", "session_id": "2cf322fb-9f7d-4c7f-aaed-4e5eace5d975", "user_id": "94d17b5d-1926-4f26-a927-07d320e9ca9b", "ip_address": "234.234.88.136", "user_agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0", "location": "France", "data": {"user_id": "94d17b5d-1926-4f26-a927-07d320e9ca9b", "page_url": "/"}}',
        '{"timestamp": "2025-07-03T16:29:10.839976", "event_type": "login", "session_id": "9a96ff19-d3ab-4909-ad7b-4fb3b2aee06a", "user_id": "94d17b5d-1926-4f26-a927-07d320e9ca9b", "ip_address": "5.230.32.189", "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36", "location": "USA", "data": {"user_id": "94d17b5d-1926-4f26-a927-07d320e9ca9b"}}',
        '{"timestamp": "2025-07-03T16:29:11.251208", "event_type": "product_view", "session_id": "03bd2f70-d392-4faf-b4fb-9050f4e040a9", "user_id": "94d17b5d-1926-4f26-a927-07d320e9ca9b", "ip_address": "185.52.72.81", "user_agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0", "location": "Germany", "data": {"user_id": "94d17b5d-1926-4f26-a927-07d320e9ca9b", "product_id": "85289387-2f47-4d21-9d4f-dcc225d20bb5", "product_name": "Product 40", "price": 463.48}}'
    ]

    # Exemples de logs malformés ou invalides
    invalid_logs = [
        'This is not a JSON log line', # Malformed JSON
        '{"timestamp": "invalid-date", "event_type": "page_view", "session_id": "abc", "user_id": "123", "ip_address": "0.0.0.0", "user_agent": "test", "location": "test", "data": {}}', # Invalid timestamp
        '{"event_type": "page_view", "session_id": "abc", "user_id": "123", "ip_address": "0.0.0.0", "user_agent": "test", "location": "test", "data": {}}', # Missing timestamp
        '{"timestamp": "2025-07-03T16:29:10.565498", "event_type": "page_view", "session_id": "abc", "user_id": "123", "ip_address": "0.0.0.0", "user_agent": "test", "location": "test", "data": "not_a_dict"}', # Data not a dict
        '{"timestamp": "2025-07-03T16:29:10.565498", "event_type": "page_view", "session_id": "abc", "user_id": 123, "ip_address": "0.0.0.0", "user_agent": "test", "location": "test", "data": {}}' # user_id not string
    ]

    print("--- Parsing des logs valides ---")
    for log_line in valid_logs:
        parsed_data = parser.parse_log_line(log_line)
        if parsed_data:
            print(f"Parsed: {parsed_data['event_type']} by {parsed_data['user_id']}")

    print("\n--- Parsing des logs invalides ---")
    for log_line in invalid_logs:
        parsed_data = parser.parse_log_line(log_line)
        if not parsed_data:
            print(f"Failed to parse or validate: {log_line.strip()[:50]}...") # Print a snippet

    print("\n--- Statistiques de parsing ---")
    print(f"Stats: {parser.get_stats()}")

    print("\n--- Logs échoués (pour re-tentative ou analyse) ---")
    for failed_log in parser.get_failed_logs():
        print(f"Reason: {failed_log['reason']}, Message: {failed_log['message']}, Original Line: {failed_log['original_line'][:100]}...")
